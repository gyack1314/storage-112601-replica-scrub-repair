"""Repair claiming, fenced target confirmations and independent verification."""

from __future__ import annotations

from typing import Any

from .errors import RepairRejected, ReplicaStoreError
from .util import as_int, sha256_bytes, text_bytes, write_atomic

PRIORITY_ORDER = {"critical": 0, "high": 1, "normal": 2, "low": 3}
ACTIVE_ITEM_STATUSES = {"claimed", "written_unverified", "verifying", "retry_ready"}


class RepairMixin:
    # BaseStore supplies transaction/journal/next_seq in ReplicaStore.

    # ---------- priority and planning ----------

    def set_priority(self, key: str, priority: str = "high") -> dict[str, Any]:
        if priority not in PRIORITY_ORDER:
            raise ValueError("priority 必须是 critical/high/normal/low")
        with self.transaction() as state:
            obj = self.object_state(state, key, create=True)
            assert obj is not None
            obj["priority"] = priority
            result = {"key": key, "priority": priority}
        return result

    def plan_repairs(self, key: str | None = None) -> list[dict[str, Any]]:
        with self.transaction() as state:
            return self._plan_repairs(state, key)

    def _plan_repairs(self, state: dict[str, Any], key: str | None = None) -> list[dict[str, Any]]:
        planned: list[dict[str, Any]] = []
        keys = [key] if key else sorted(state["objects"].keys())
        for object_key in keys:
            obj = self.object_state(state, object_key)
            if obj is None or obj.get("current_rev") is None:
                continue
            rev = int(obj["current_rev"])
            version = self.version_state(state, object_key, rev)
            if version is None or not version.get("committed"):
                continue
            priority = obj.get("priority", "normal")
            for block_str, digest in sorted(version["blocks"].items(), key=lambda item: int(item[0])):
                block = int(block_str)
                statuses = []
                trusted = []
                candidates = list(version.get("placement", []))
                for node in state["nodes"]:
                    if node not in candidates:
                        candidates.append(node)
                for node in candidates:
                    status = self.replica_classification(state, node, object_key, rev, block)
                    statuses.append(status)
                    if status["trusted"]:
                        trusted.append(node)
                intended = set(version.get("targets") or version.get("placement") or [])
                untrusted = []
                for status in statuses:
                    if status["trusted"]:
                        continue
                    if status["node"] in intended or status["status"] in {
                        "corrupt",
                        "missing",
                        "written_unverified",
                    }:
                        untrusted.append(status["node"])
                risk = 0 if not trusted else 1
                if not trusted:
                    planned.append(
                        {
                            "key": object_key,
                            "rev": rev,
                            "block": block,
                            "digest": digest,
                            "priority": priority,
                            "risk_rank": risk,
                            "source": None,
                            "target": None,
                            "unreadable": True,
                            "reason_code": "no_verified_source",
                            "statuses": statuses,
                        }
                    )
                else:
                    source = trusted[0]
                    for target in untrusted:
                        planned.append(
                            {
                                "key": object_key,
                                "rev": rev,
                                "block": block,
                                "digest": digest,
                                "priority": priority,
                                "risk_rank": risk,
                                "source": source,
                                "target": target,
                                "unreadable": False,
                                "reason_code": "under_replicated",
                                "statuses": statuses,
                            }
                        )
        planned.sort(
            key=lambda item: (
                PRIORITY_ORDER.get(item["priority"], 2),
                item["risk_rank"],
                item["key"],
                item["block"],
                item["target"] or "",
            )
        )
        return planned

    def claim_next(self, job_id: str | None = None, batch_size: int = 3, at: int | None = None) -> dict[str, Any]:
        planned = self.plan_repairs()
        items = [
            {k: item[k] for k in ("key", "rev", "block", "source", "target")}
            for item in planned
            if item.get("source") and item.get("target")
        ][:batch_size]
        if not items:
            return {"claimed": [], "job": None, "message": "没有可领取的已校验修复来源"}
        return self.claim_repairs(job_id, items, at=at)

    # ---------- claim ----------

    def claim_repair(self, record: dict[str, Any], at: int | None = None) -> dict[str, Any]:
        if "items" in record:
            return self.claim_repairs(str(record.get("id") or record.get("job_id")), record["items"], at=at)
        blocks = record.get("blocks")
        if isinstance(blocks, list) and blocks and all(not isinstance(value, dict) for value in blocks):
            items = [
                {
                    "key": record["key"],
                    "rev": record["rev"],
                    "block": block,
                    "source": record.get("source"),
                    "target": record.get("target"),
                }
                for block in blocks
            ]
            return self.claim_repairs(str(record["id"]), items, at=at)
        if isinstance(blocks, list) and blocks and all(isinstance(value, dict) for value in blocks):
            return self.claim_repairs(str(record["id"]), blocks, at=at)
        item = {
            "key": record["key"],
            "rev": record["rev"],
            "block": record.get("block", 0),
            "source": record.get("source"),
            "target": record.get("target"),
        }
        return self.claim_repairs(str(record["id"]), [item], at=at)

    def claim_repairs(
        self,
        job_id: str | None,
        items: list[dict[str, Any]],
        at: int | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as state:
            if job_id is None:
                job_id = f"repair-{self.next_seq(state)}"
            if job_id in state["jobs"]:
                job = state["jobs"][job_id]
                if not self._same_claim_items(job, items):
                    raise RepairRejected(
                        f"修复作业 ID {job_id} 已用于不同的块集合",
                        {"reason_code": "job_id_conflict"},
                    )
                job["idempotent_replay"] = True
                result = job
                seq = state["sequence"]
            else:
                job = self._create_job(state, job_id, items, at)
                state["job_order"].append(job_id)
                result = job
                seq = state["sequence"]
        self.journal("repair", {"id": job_id, "items": items}, at, seq)
        return result

    @staticmethod
    def _same_claim_items(job: dict[str, Any], raw_items: list[dict[str, Any]]) -> bool:
        existing = {
            (item["key"], int(item["rev"]), int(item["block"]), item["target_node"])
            for item in job.get("items", [])
        }
        requested = {
            (str(item["key"]), int(item["rev"]), int(item.get("block", 0)), str(item["target"]))
            for item in raw_items
        }
        return existing == requested

    def _create_job(self, state: dict[str, Any], job_id: str, items: list[dict[str, Any]], at: int | None) -> dict[str, Any]:
        if not items:
            raise ValueError("修复批次至少需要一个块")
        first_obj = None
        job_items: list[dict[str, Any]] = []
        locks = state.setdefault("locks", {})
        claim_seq = self.next_seq(state)
        for raw_item in items:
            key = str(raw_item["key"])
            rev = as_int(raw_item["rev"], "rev")
            block = as_int(raw_item.get("block", 0), "block")
            source = str(raw_item["source"]) if raw_item.get("source") else None
            target = str(raw_item["target"]) if raw_item.get("target") else None
            obj = self.object_state(state, key)
            version = self.version_state(state, key, rev)
            if obj is None or version is None:
                raise RepairRejected(f"未知对象版本: {key} rev={rev}")
            if not version.get("committed"):
                raise RepairRejected(f"只能修复已提交版本: {key} rev={rev}")
            expected = version.get("blocks", {}).get(str(block))
            if not expected:
                raise RepairRejected(f"块不在版本清单中: {key} rev={rev} block={block}")
            if source is None:
                source = self._find_verified_source(state, key, rev, block, expected)
            source_boot, source_digest = self._verify_node_bytes(state, source, key, rev, block, expected, at, "repair_claim")
            if source_boot is None or source_digest != expected:
                decision = {
                    "job_id": job_id,
                    "key": key,
                    "rev": rev,
                    "block": block,
                    "source": source,
                    "reason_code": "source_not_verified",
                }
                raise RepairRejected("修复来源没有相同对象版本和块身份的已校验副本", decision)
            if target is None:
                raise RepairRejected("必须指定 target 节点")
            target_node = self.node_state(state, target)
            if target_node is None:
                raise RepairRejected(f"目标节点未登记启动身份: {target}")
            target_boot = target_node.get("current_boot")
            current_rev = obj.get("current_rev")
            historical = current_rev is not None and rev < int(current_rev)
            lock_key = f"{target}|{key}|{rev}|{block}"
            existing_lock = locks.get(lock_key)
            if existing_lock and existing_lock.get("status") == "active" and existing_lock.get("job_id") != job_id:
                raise RepairRejected(
                    f"{lock_key} 已有活动修复 {existing_lock.get('job_id')}",
                    {"reason_code": "repair_already_claimed"},
                )
            fence = f"{job_id}:{claim_seq}:{block}"
            item_id = str(raw_item.get("item_id") or f"{job_id}:{key}:r{rev}:b{block}:{target}")
            item = {
                "item_id": item_id,
                "key": key,
                "rev": rev,
                "block": block,
                "digest": expected,
                "source_node": source,
                "source_boot": source_boot,
                "source_verified": True,
                "source_digest": source_digest,
                "target_node": target,
                "expected_target_boot": target_boot,
                "claimed_current_rev": current_rev,
                "historical": historical,
                "fence_current_rev": None if historical else rev,
                "fence": fence,
                "status": "claimed",
                "is_valid": False,
                "is_stale": False,
                "reason_code": None,
                "claimed_seq": claim_seq,
                "claimed_at": at,
                "attempts": [],
            }
            job_items.append(item)
            locks[lock_key] = {"job_id": job_id, "item_id": item_id, "status": "active", "fence": fence}
            if first_obj is None:
                first_obj = obj
        priority = first_obj.get("priority", "normal") if first_obj else "normal"
        job = {
            "id": job_id,
            "kind": "repair_batch",
            "status": "claimed",
            "priority": priority,
            "items": job_items,
            "claimed_seq": claim_seq,
            "claimed_at": at,
            "latest_seq": claim_seq,
        }
        state["jobs"][job_id] = job
        return job

    def _find_verified_source(
        self, state: dict[str, Any], key: str, rev: int, block: int, expected: str
    ) -> str | None:
        for node in state["nodes"]:
            boot, digest = self._verify_node_bytes(state, node, key, rev, block, expected, None, "source_lookup")
            if digest == expected:
                return node
        return None

    def _verify_node_bytes(
        self,
        state: dict[str, Any],
        node: str,
        key: str,
        rev: int,
        block: int,
        expected: str,
        at: int | None,
        source: str,
    ) -> tuple[int | None, str | None]:
        boot = self.current_boot(state, node)
        data, digest, _ = self.hash_path(self.block_path(node, key, rev, block))
        if data is None or digest != expected:
            if data is not None:
                self.add_observation(
                    state,
                    node=node,
                    key=key,
                    rev=rev,
                    block=block,
                    source=source,
                    boot=boot,
                    observed_digest=digest,
                    verified=True,
                    matches=False,
                    at=at,
                )
            return boot, digest
        self.add_observation(
            state,
            node=node,
            key=key,
            rev=rev,
            block=block,
            source=source,
            boot=boot,
            observed_digest=digest,
            verified=True,
            matches=True,
            at=at,
        )
        return boot, digest

    # ---------- actual local execution from a verified source ----------

    def execute_repair(
        self,
        job_id: str,
        block: int | None = None,
        item_id: str | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as state:
            job = state["jobs"].get(job_id)
            if job is None:
                raise ReplicaStoreError(f"未知修复作业: {job_id}")
            if block is None and item_id is None and len(job["items"]) > 1:
                outputs = [self._execute_item(state, job, item, at) for item in job["items"]]
                self._aggregate_job(job)
                result = {"id": job_id, "executed": outputs}
            else:
                item = self._select_item(job, {"block": block, "item_id": item_id})
                if item is None:
                    raise ReplicaStoreError(f"修复批次 {job_id} 找不到块/子项")
                output = self._execute_item(state, job, item, at)
                self._aggregate_job(job)
                result = {"id": job_id, **output}
            seq = state["sequence"]
        self.journal("repair_execute", {"id": job_id, "block": block, "item_id": item_id}, at, seq)
        return result

    def _execute_item(
        self, state: dict[str, Any], job: dict[str, Any], item: dict[str, Any], at: int | None
    ) -> dict[str, Any]:
        if item.get("status") == "verified":
            return self._public_item(item)
        seq = self.next_seq(state)
        obj = self.object_state(state, item["key"])
        current_rev = None if obj is None else obj.get("current_rev")
        current_boot = self.current_boot(state, item["target_node"])
        if not item.get("historical") and current_rev != item.get("fence_current_rev"):
            item.update(
                {
                    "status": "stale",
                    "is_stale": True,
                    "is_valid": False,
                    "reason_code": "new_version_committed",
                    "reason": "执行前对象代次已经前进；条件写失败，旧字节没有覆盖新版本",
                    "completed_seq": seq,
                }
            )
            self._set_lock_status(state, item, "stale")
            return self._public_item(item)
        if current_boot != item.get("expected_target_boot"):
            item.update(
                {
                    "status": "stale",
                    "is_stale": True,
                    "is_valid": False,
                    "reason_code": "target_boot_mismatch",
                    "reason": "目标节点启动代次已变化；修复没有写入",
                    "completed_seq": seq,
                }
            )
            self._set_lock_status(state, item, "stale")
            return self._public_item(item)
        source_boot, source_digest = self._verify_node_bytes(
            state,
            item["source_node"],
            item["key"],
            item["rev"],
            item["block"],
            item["digest"],
            at,
            "repair_execute_source",
        )
        if source_digest != item["digest"]:
            item.update(
                {
                    "status": "blocked",
                    "is_valid": False,
                    "reason_code": "source_not_verified_at_execution",
                    "reason": "执行时来源不再具有相同版本和块身份的已校验字节",
                    "completed_seq": seq,
                }
            )
            self._set_lock_status(state, item, "failed")
            return self._public_item(item)
        source_data, _, _ = self.hash_path(
            self.block_path(item["source_node"], item["key"], item["rev"], item["block"])
        )
        if source_data is None:
            item["status"] = "blocked"
            item["reason_code"] = "source_missing_at_execution"
            self._set_lock_status(state, item, "failed")
            return self._public_item(item)
        write_atomic(
            self.block_path(item["target_node"], item["key"], item["rev"], item["block"]),
            source_data,
        )
        item["status"] = "written_unverified"
        item["is_valid"] = False
        item["is_stale"] = False
        item["reason_code"] = "awaiting_independent_verification"
        item["reason"] = "已从校验来源条件写入目标；在独立磁盘校验前不恢复可信资格"
        item["latest_seq"] = seq
        item["result"] = {
            "boot": current_boot,
            "rev": item["rev"],
            "digest": item["digest"],
            "executed": True,
            "seq": seq,
            "at": at,
        }
        self._set_lock_status(state, item, "verifying")
        self.add_observation(
            state,
            node=item["target_node"],
            key=item["key"],
            rev=item["rev"],
            block=item["block"],
            source="repair_execute",
            boot=current_boot,
            observed_digest=item["digest"],
            verified=False,
            matches=True,
            at=at,
            extra={"write_report_only": True},
        )
        return self._public_item(item)

    # ---------- target result ----------

    def record_result(self, record: dict[str, Any], at: int | None = None) -> dict[str, Any]:
        if "results" in record:
            outputs = []
            for sub in record["results"]:
                merged = {"id": record["id"], **sub}
                outputs.append(self.record_result(merged, at=at))
            return {"id": record["id"], "results": outputs}
        job_id = str(record["id"])
        with self.transaction() as state:
            job = state["jobs"].get(job_id)
            if job is None:
                raise ReplicaStoreError(f"未知修复作业: {job_id}")
            item = self._select_item(job, record)
            if item is None:
                raise ReplicaStoreError(f"修复批次 {job_id} 找不到块/子项")
            if item["status"] in {"verified", "stale", "failed"} and not record.get("force"):
                if self._result_is_duplicate(item, record):
                    result = {"id": job_id, "item_id": item["item_id"], "duplicate": True, **self._public_item(item)}
                    seq = state["sequence"]
                    return result
            result = self._apply_result(state, job, item, record, at)
            self._aggregate_job(job)
            seq = state["sequence"]
        self.journal("result", record, at, seq)
        return result

    @staticmethod
    def _select_item(job: dict[str, Any], record: dict[str, Any]) -> dict[str, Any] | None:
        items = job["items"]
        if record.get("item_id"):
            return next((i for i in items if i["item_id"] == record["item_id"]), None)
        if record.get("block") is not None:
            block = as_int(record["block"], "block")
            return next((i for i in items if i["block"] == block), None)
        return items[0] if len(items) == 1 else None

    @staticmethod
    def _result_is_duplicate(item: dict[str, Any], record: dict[str, Any]) -> bool:
        previous = item.get("result") or {}
        return (
            previous.get("boot") == record.get("boot")
            and previous.get("rev") == record.get("rev")
            and (record.get("text") is None or previous.get("digest") == sha256_bytes(text_bytes(record["text"])))
        )

    def _apply_result(
        self,
        state: dict[str, Any],
        job: dict[str, Any],
        item: dict[str, Any],
        record: dict[str, Any],
        at: int | None,
    ) -> dict[str, Any]:
        seq = self.next_seq(state)
        result_boot = as_int(record["boot"], "boot") if record.get("boot") is not None else None
        result_rev = as_int(record["rev"], "rev") if record.get("rev") is not None else None
        text = record.get("text")
        result_digest = record.get("digest")
        if text is not None:
            result_digest = sha256_bytes(text_bytes(text))
        obj = self.object_state(state, item["key"])
        current_rev = None if obj is None else obj.get("current_rev")
        current_target_boot = self.current_boot(state, item["target_node"])
        checks = {
            "source_verified": bool(item.get("source_verified")),
            "same_object_version": result_rev == item["rev"],
            "same_block_identity": result_digest in {None, item["digest"]},
            "planned_boot_match": result_boot == item["expected_target_boot"],
            "current_target_boot_match": current_target_boot == item["expected_target_boot"],
            "current_version_fence": bool(item.get("historical")) or current_rev == item["fence_current_rev"],
        }
        attempt = {"seq": seq, "at": at, "recorded": record, "checks": checks}
        item.setdefault("attempts", []).append(attempt)
        item["latest_seq"] = seq
        stale_reason = None
        if not checks["same_object_version"]:
            stale_reason = ("result_rev_mismatch", "回执的对象版本不是领取时预定版本")
        elif not checks["planned_boot_match"] or not checks["current_target_boot_match"]:
            stale_reason = ("target_boot_mismatch", "目标节点启动代次已变化，旧检修回执已过时")
        elif not checks["current_version_fence"]:
            stale_reason = ("new_version_committed", "检修期间已有更新写入代次发布；旧字节不得覆盖新版本")
        if stale_reason is not None:
            code, message = stale_reason
            item["status"] = "stale"
            item["is_valid"] = False
            item["is_stale"] = True
            item["reason_code"] = code
            item["reason"] = message
            item["completed_seq"] = seq
            item["checks"] = checks
            self._set_lock_status(state, item, "stale")
            self.add_observation(
                state,
                node=item["target_node"],
                key=item["key"],
                rev=item["rev"],
                block=item["block"],
                source="repair_result_rejected",
                boot=result_boot,
                observed_digest=result_digest,
                verified=False,
                matches=checks["same_block_identity"],
                at=at,
                extra={"rejected_reason": code},
            )
            return {"id": job["id"], **self._public_item(item)}
        if result_digest is not None and result_digest != item["digest"]:
            item["status"] = "failed"
            item["is_valid"] = False
            item["is_stale"] = False
            item["reason_code"] = "result_digest_mismatch"
            item["reason"] = "目标回执文本的摘要与预定块身份不符"
            item["completed_seq"] = seq
            item["checks"] = checks
            self._set_lock_status(state, item, "failed")
            self.add_observation(
                state,
                node=item["target_node"],
                key=item["key"],
                rev=item["rev"],
                block=item["block"],
                source="repair_result",
                boot=result_boot,
                observed_digest=result_digest,
                verified=True,
                matches=False,
                at=at,
            )
            return {"id": job["id"], **self._public_item(item)}
        item["status"] = "written_unverified"
        item["is_valid"] = False
        item["is_stale"] = False
        item["reason_code"] = "awaiting_independent_verification"
        item["reason"] = "目标端只确认已写入；在独立校验前不恢复为可信副本"
        item["result"] = {"boot": result_boot, "rev": result_rev, "digest": result_digest, "seq": seq, "at": at}
        item["checks"] = checks
        self._set_lock_status(state, item, "verifying")
        if text is not None and result_digest == item["digest"]:
            # Fenced checks passed; this writes only the exact immutable revision path.
            write_atomic(self.block_path(item["target_node"], item["key"], item["rev"], item["block"]), text_bytes(text))
        self.add_observation(
            state,
            node=item["target_node"],
            key=item["key"],
            rev=item["rev"],
            block=item["block"],
            source="repair_result",
            boot=result_boot,
            observed_digest=result_digest,
            verified=False,
            matches=True if result_digest else None,
            at=at,
            extra={"write_report_only": True},
        )
        return {"id": job["id"], **self._public_item(item)}

    # ---------- verify ----------

    def verify_repair(self, record: dict[str, Any], at: int | None = None) -> dict[str, Any]:
        job_id = str(record["id"])
        with self.transaction() as state:
            job = state["jobs"].get(job_id)
            if job is None:
                raise ReplicaStoreError(f"未知修复作业: {job_id}")
            item = self._select_item(job, record)
            if item is None:
                raise ReplicaStoreError(f"修复批次 {job_id} 找不到块/子项")
            if item["status"] == "verified":
                result = {"id": job_id, "duplicate": True, **self._public_item(item)}
                seq = state["sequence"]
                return result
            result = self._apply_verify(state, job, item, record, at)
            self._aggregate_job(job)
            seq = state["sequence"]
        self.journal("verify", record, at, seq)
        return result

    def _apply_verify(
        self,
        state: dict[str, Any],
        job: dict[str, Any],
        item: dict[str, Any],
        record: dict[str, Any],
        at: int | None,
    ) -> dict[str, Any]:
        seq = self.next_seq(state)
        if item["status"] == "stale":
            item.setdefault("verification_attempts", []).append({"seq": seq, "accepted": False, "at": at})
            return {"id": job["id"], "verified": False, **self._public_item(item)}
        obj = self.object_state(state, item["key"])
        current_rev = None if obj is None else obj.get("current_rev")
        if not item.get("historical") and current_rev != item.get("fence_current_rev"):
            item["status"] = "stale"
            item["is_stale"] = True
            item["is_valid"] = False
            item["reason_code"] = "new_version_committed"
            item["reason"] = "校验前对象写入代次已变化，旧修复已过时"
            item["completed_seq"] = seq
            self._set_lock_status(state, item, "stale")
            return {"id": job["id"], "verified": False, **self._public_item(item)}
        text = record.get("text")
        digest = record.get("digest")
        if text is not None:
            digest = sha256_bytes(text_bytes(text))
        actual_boot = self.current_boot(state, item["target_node"])
        if digest is None:
            data, digest, _ = self.hash_path(
                self.block_path(item["target_node"], item["key"], item["rev"], item["block"])
            )
            if data is None:
                item["status"] = "verify_failed"
                item["reason_code"] = "target_missing_after_write"
                item["reason"] = "独立校验时目标块文件不存在"
                self._set_lock_status(state, item, "failed")
                return {"id": job["id"], "verified": False, **self._public_item(item)}
        if digest != item["digest"]:
            item["status"] = "verify_failed"
            item["is_valid"] = False
            item["reason_code"] = "verification_digest_mismatch"
            item["reason"] = "独立校验摘要与版本清单中的块身份不同"
            item["verified_seq"] = seq
            self._set_lock_status(state, item, "failed")
            self.add_observation(
                state,
                node=item["target_node"],
                key=item["key"],
                rev=item["rev"],
                block=item["block"],
                source="repair_verify",
                boot=actual_boot,
                observed_digest=digest,
                verified=True,
                matches=False,
                at=at,
            )
            return {"id": job["id"], "verified": False, **self._public_item(item)}
        item["status"] = "verified"
        item["is_valid"] = True
        item["is_stale"] = False
        item["reason_code"] = "repair_verified"
        item["reason"] = "来源、对象版本、块身份、目标代次和独立摘要校验全部一致"
        item["verified_seq"] = seq
        item["verified_at"] = at
        item["verification"] = {"seq": seq, "at": at, "digest": digest}
        self._set_lock_status(state, item, "completed")
        verify_boot = (item.get("result") or {}).get("boot", item.get("expected_target_boot"))
        self.add_observation(
            state,
            node=item["target_node"],
            key=item["key"],
            rev=item["rev"],
            block=item["block"],
            source="repair_verify",
            boot=verify_boot,
            observed_digest=digest,
            verified=True,
            matches=True,
            at=at,
        )
        return {"id": job["id"], "verified": True, **self._public_item(item)}

    @staticmethod
    def _set_lock_status(state: dict[str, Any], item: dict[str, Any], status: str) -> None:
        lock_key = f"{item['target_node']}|{item['key']}|{item['rev']}|{item['block']}"
        lock = state.setdefault("locks", {}).get(lock_key)
        if lock and lock.get("job_id") == item.get("job_id"):
            lock["status"] = status
        # item does not store job_id currently; caller's lock may still be found by item_id.
        for candidate in state.get("locks", {}).values():
            if candidate.get("item_id") == item.get("item_id"):
                candidate["status"] = status

    @staticmethod
    def _aggregate_job(job: dict[str, Any]) -> None:
        statuses = [item["status"] for item in job["items"]]
        if all(s == "verified" for s in statuses):
            job["status"] = "verified"
        elif any(s == "stale" for s in statuses) and all(s in {"stale", "verified"} for s in statuses):
            job["status"] = "partially_stale" if "verified" in statuses else "stale"
        elif any(s in {"failed", "verify_failed"} for s in statuses):
            job["status"] = "failed"
        elif all(s == "written_unverified" for s in statuses):
            job["status"] = "written_unverified"
        else:
            job["status"] = "in_progress"
        job["latest_seq"] = max((item.get("latest_seq") or item.get("claimed_seq") or 0) for item in job["items"])

    @staticmethod
    def _public_item(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "item_id": item.get("item_id"),
            "key": item.get("key"),
            "rev": item.get("rev"),
            "block": item.get("block"),
            "digest": item.get("digest"),
            "source_node": item.get("source_node"),
            "source_boot": item.get("source_boot"),
            "target_node": item.get("target_node"),
            "expected_target_boot": item.get("expected_target_boot"),
            "status": item.get("status"),
            "is_valid": item.get("is_valid", item.get("status") == "verified"),
            "is_stale": item.get("is_stale", item.get("status") == "stale"),
            "reason_code": item.get("reason_code"),
            "reason": item.get("reason"),
            "checks": item.get("checks"),
            "claimed_seq": item.get("claimed_seq"),
            "completed_seq": item.get("completed_seq") or item.get("verified_seq"),
        }
