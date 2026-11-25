"""Write staging, committed reads, corruption injection and scrub reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import BlockUnderRepair, BlockUnavailable, GenerationFenceError, ReplicaStoreError
from .util import as_int, sha256_bytes, text_bytes, write_atomic


class ReadWriteMixin:
    # Methods such as transaction(), journal() and next_seq() are provided by
    # BaseStore and appear later in the concrete ReplicaStore MRO.

    # ---------- synthetic writes ----------

    def write_object(
        self,
        key: str,
        blocks: list[Any] | dict[int | str, Any],
        rev: int | None = None,
        nodes: list[str] | tuple[str, ...] | None = None,
        generation: str | None = None,
        commit: bool = True,
        at: int | None = None,
    ) -> dict[str, Any]:
        if isinstance(blocks, list):
            raw_blocks = {i: text_bytes(value) for i, value in enumerate(blocks)}
        elif isinstance(blocks, dict):
            raw_blocks = {int(i): text_bytes(value) for i, value in blocks.items()}
        elif isinstance(blocks, (str, bytes)):
            raw_blocks = {0: text_bytes(blocks)}
        else:
            raise ValueError("blocks 必须是字符串、字节、列表或字典")
        digests = {str(i): sha256_bytes(data) for i, data in sorted(raw_blocks.items())}
        selected_nodes = list(nodes or self.default_nodes)
        manifest = self.propose_manifest(
            key,
            digests,
            rev=rev,
            generation=generation,
            at=at,
            placement=selected_nodes,
        )
        rev = int(manifest["rev"])
        staged = []
        for node in selected_nodes:
            for block, data in sorted(raw_blocks.items()):
                staged.append(self.stage_block(node, key, rev, block, data, at=at))
        if commit:
            self.commit(key, rev, generation=generation, at=at)
        return {
            "key": key,
            "rev": rev,
            "committed": commit,
            "nodes": selected_nodes,
            "blocks": digests,
            "staged": staged,
        }

    def stage_block(
        self,
        node: str,
        key: str,
        rev: int,
        block: int,
        data: str | bytes,
        boot: int | None = None,
        at: int | None = None,
        *,
        fence: str | None = None,
    ) -> dict[str, Any]:
        raw = text_bytes(data)
        digest = sha256_bytes(raw)
        rev, block = as_int(rev, "rev"), as_int(block, "block")
        with self.transaction() as state:
            nd = self.node_state(state, node)
            if nd is None:
                self._register_node(state, node, boot or 1, at, scan=False)
            elif boot is not None and int(boot) > int(nd.get("current_boot") or 0):
                self._register_node(state, node, int(boot), at, scan=False)
            active_boot = self.current_boot(state, node)
            lock_key = f"{node}|{key}|{rev}|{block}"
            lock = state.setdefault("locks", {}).get(lock_key)
            if lock and lock.get("fence") != fence and lock.get("status") == "active":
                raise BlockUnderRepair(f"{lock_key} 正由修复作业 {lock.get('job_id')} 检修")
            write_atomic(self.block_path(node, key, rev, block), raw)
            entry = self.add_observation(
                state,
                node=node,
                key=key,
                rev=rev,
                block=block,
                source="stored",
                boot=active_boot,
                observed_digest=digest,
                verified=True,
                at=at,
                extra={"size": len(raw)},
            )
            version = self.version_state(state, key, rev)
            if version is not None and node not in version.setdefault("placement", []):
                version["placement"].append(node)
            result = {"node": node, "boot": active_boot, "digest": digest, "seq": entry["seq"]}
            seq = state["sequence"]
        self.journal(
            "stored",
            {"node": node, "boot": boot, "key": key, "rev": rev, "block": block, "text": data if isinstance(data, str) else None},
            at,
            seq,
        )
        return result

    def record_stored(self, record: dict[str, Any], at: int | None = None) -> dict[str, Any]:
        node = str(record["node"])
        boot = as_int(record["boot"], "boot")
        key = str(record["key"])
        rev = as_int(record["rev"], "rev")
        block = as_int(record.get("block", 0), "block")
        text = record.get("text")
        digest = record.get("digest")
        with self.transaction() as state:
            nd = self.node_state(state, node)
            if nd is None:
                self._register_node(state, node, boot, at, scan=False)
            active_boot = self.current_boot(state, node)
            historical = active_boot != boot
            raw = text_bytes(text) if text is not None else None
            observed = sha256_bytes(raw) if raw is not None else digest
            if not historical and raw is not None:
                write_atomic(self.block_path(node, key, rev, block), raw)
            elif not historical and raw is None:
                disk_data, disk_digest, _ = self.hash_path(self.block_path(node, key, rev, block))
                if disk_data is not None:
                    observed = disk_digest
            entry = self.add_observation(
                state,
                node=node,
                key=key,
                rev=rev,
                block=block,
                source="stored",
                boot=boot,
                observed_digest=observed,
                verified=raw is not None or (not historical and observed == digest and digest is not None and text is None and False),
                at=at,
                extra={
                    "reported_boot": boot,
                    "historical_generation": historical,
                    "write_report_only": raw is None,
                },
            )
            # A supplied synthetic text is direct evidence and can be hash checked.
            if raw is not None:
                entry["verified"] = True
            version = self.version_state(state, key, rev)
            if not historical and version is not None and entry.get("matches") and node not in version.setdefault("placement", []):
                version["placement"].append(node)
            result = {
                "classification": "historical_generation" if historical else "current",
                "node": node,
                "reported_boot": boot,
                "active_boot": active_boot,
                "digest": observed,
                "seq": entry["seq"],
            }
            seq = state["sequence"]
        self.journal("stored", record, at, seq)
        return result

    def corrupt_replica(
        self,
        node: str,
        key: str,
        rev: int,
        block: int,
        text: str | bytes = "silent-corruption",
        at: int | None = None,
    ) -> dict[str, Any]:
        rev, block = as_int(rev, "rev"), as_int(block, "block")
        raw = text_bytes(text)
        with self.transaction() as state:
            path = self.block_path(node, key, rev, block)
            write_atomic(path, raw)
            state.setdefault("fault_injections", []).append(
                {"seq": self.next_seq(state), "at": at, "node": node, "key": key, "rev": rev, "block": block}
            )
            seq = state["sequence"]
        self.journal("fault", {"node": node, "key": key, "rev": rev, "block": block}, at, seq)
        return {"corrupted": True, "path": str(path), "digest": sha256_bytes(raw)}

    # ---------- online reads ----------

    def current_revision(self, key: str) -> int | None:
        with self.transaction() as state:
            obj = self.object_state(state, key)
            return None if obj is None else obj.get("current_rev")

    def read_block(
        self, key: str, block: int, rev: int | None = None, at: int | None = None
    ) -> dict[str, Any]:
        block = as_int(block, "block")
        with self.transaction() as state:
            outcome = self._read_block(state, key, block, rev, at)
            seq = state["sequence"]
        self.journal("read", {"key": key, "rev": rev, "block": block, "ok": outcome["ok"]}, at, seq)
        if not outcome["ok"]:
            raise BlockUnavailable(outcome.get("message", "块不可读"), outcome)
        return outcome

    def _read_block(
        self,
        state: dict[str, Any],
        key: str,
        block: int,
        rev: int | None,
        at: int | None,
    ) -> dict[str, Any]:
        obj = self.object_state(state, key)
        if obj is None:
            return {"ok": False, "key": key, "block": block, "reason_code": "no_committed_version"}
        if rev is None:
            if obj.get("current_rev") is None:
                return {"ok": False, "key": key, "block": block, "reason_code": "no_committed_version"}
            rev = int(obj["current_rev"])
        rev = as_int(rev, "rev")
        version = self.version_state(state, key, rev)
        if version is None or not version.get("committed"):
            return {
                "ok": False,
                "key": key,
                "rev": rev,
                "block": block,
                "reason_code": "version_not_committed",
            }
        expected = version.get("blocks", {}).get(str(block))
        if not expected:
            return {
                "ok": False,
                "key": key,
                "rev": rev,
                "block": block,
                "reason_code": "block_not_in_manifest",
            }
        candidates: list[str] = []
        for node in list(version.get("placement", [])) + list(state.get("nodes", {}).keys()):
            if node not in candidates:
                candidates.append(node)
        attempts = []
        corrupt_count = missing_count = 0
        for node in candidates:
            path = self.block_path(node, key, rev, block)
            data, digest, size = self.hash_path(path)
            boot = self.current_boot(state, node)
            if data is None:
                missing_count += 1
                self.add_observation(
                    state,
                    node=node,
                    key=key,
                    rev=rev,
                    block=block,
                    source="read",
                    boot=boot,
                    observed_digest=None,
                    verified=True,
                    matches=False,
                    present=False,
                    at=at,
                )
                attempts.append({"node": node, "status": "missing"})
                continue
            ok = digest == expected
            self.add_observation(
                state,
                node=node,
                key=key,
                rev=rev,
                block=block,
                source="read",
                boot=boot,
                observed_digest=digest,
                verified=True,
                matches=ok,
                at=at,
                extra={"size": size},
            )
            if ok:
                try:
                    text = data.decode("utf-8")
                except UnicodeDecodeError:
                    text = None
                return {
                    "ok": True,
                    "key": key,
                    "rev": rev,
                    "block": block,
                    "node": node,
                    "boot": boot,
                    "digest": digest,
                    "size": size,
                    "data": data,
                    "text": text,
                    "attempts": attempts + [{"node": node, "status": "valid"}],
                }
            corrupt_count += 1
            attempts.append({"node": node, "status": "corrupt", "observed_digest": digest})
        if corrupt_count and not missing_count:
            code = "all_replicas_corrupt"
        elif corrupt_count:
            code = "no_trusted_replica_some_corrupt"
        elif missing_count:
            code = "all_replicas_missing"
        else:
            code = "no_replicas_present"
        outcome = {
            "ok": False,
            "key": key,
            "rev": rev,
            "block": block,
            "expected_digest": expected,
            "reason_code": code,
            "attempts": attempts,
            "message": f"{key} rev={rev} block={block} 没有清单可证明的副本",
        }
        state.setdefault("read_results", []).append({k: v for k, v in outcome.items() if k != "data"})
        state["read_results"] = state["read_results"][-100:]
        return outcome

    def read_object(
        self, key: str, rev: int | None = None, at: int | None = None
    ) -> dict[str, Any]:
        with self.transaction() as state:
            obj = self.object_state(state, key)
            if obj is None or obj.get("current_rev") is None:
                report = {"ok": False, "key": key, "reason_code": "no_committed_version", "blocks": {}}
                raise BlockUnavailable(f"{key} 没有已提交版本", report)
            if rev is None:
                rev = int(obj["current_rev"])
            version = self.version_state(state, key, rev)
            if version is None or not version.get("committed"):
                report = {"ok": False, "key": key, "rev": rev, "reason_code": "version_not_committed", "blocks": {}}
                raise BlockUnavailable(f"{key} rev={rev} 未提交", report)
            block_ids = sorted(int(i) for i in version["blocks"].keys())
        results = {}
        missing = []
        chunks: list[bytes] = []
        attempts = []
        for block_id in block_ids:
            try:
                result = self.read_block(key, block_id, rev=rev, at=at)
            except BlockUnavailable as error:
                results[str(block_id)] = error.report
                missing.append(block_id)
                attempts.extend(error.report.get("attempts", []))
            else:
                results[str(block_id)] = {k: v for k, v in result.items() if k not in {"data"}}
                chunks.append(result["data"])
                attempts.extend(result.get("attempts", []))
        report = {
            "ok": not missing,
            "key": key,
            "rev": rev,
            "blocks": results,
            "missing_blocks": missing,
            "substituted_previous_revision": False,
        }
        if missing:
            report["reason_code"] = "incomplete_object_refused"
            report["message"] = "只有部分块存在可信副本；不会用上一版同块填充"
            raise BlockUnavailable(report["message"], report)
        data = b"".join(chunks)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        report.update({"data": data, "text": text, "size": len(data)})
        return report

    # ---------- scrub reports ----------

    def report_scrub(
        self,
        record: dict[str, Any],
        at: int | None = None,
        *,
        adopt_observed_bytes: bool = False,
    ) -> dict[str, Any]:
        node = str(record["node"])
        boot = as_int(record["boot"], "boot")
        key = str(record["key"])
        rev = as_int(record["rev"], "rev")
        observations = self._scrub_observations(record)
        start = min(observations)
        end = max(observations) + 1
        with self.transaction() as state:
            nd = self.node_state(state, node)
            if nd is None:
                self._register_node(state, node, boot, at, scan=False)
            active_boot = self.current_boot(state, node)
            historical = active_boot != boot
            version = self.version_state(state, key, rev)
            manifest_digest = version.get("manifest_digest") if version else None
            block_results = []
            for block, obs in sorted(observations.items()):
                text = obs.get("text")
                raw = text_bytes(text) if text is not None else None
                observed = sha256_bytes(raw) if raw is not None else obs.get("digest")
                if adopt_observed_bytes and not historical and raw is not None:
                    write_atomic(self.block_path(node, key, rev, block), raw)
                expected = None if version is None else version.get("blocks", {}).get(str(block))
                matches = None if expected is None or observed is None else observed == expected
                self.add_observation(
                    state,
                    node=node,
                    key=key,
                    rev=rev,
                    block=block,
                    source="scrub",
                    boot=boot,
                    observed_digest=observed,
                    verified=True,
                    matches=matches,
                    at=at,
                    extra={
                        "scrub_range": [start, end],
                        "historical_generation": historical,
                        "reported_digest": obs.get("digest"),
                    },
                )
                block_results.append(
                    {
                        "block": block,
                        "range": [block, block + 1],
                        "observed_digest": observed,
                        "expected_digest": expected,
                        "matches": matches,
                        "classification": "historical_generation" if historical else "current_generation",
                    }
                )
            report = {
                "seq": self.next_seq(state),
                "at": at,
                "key": key,
                "rev": rev,
                "node": node,
                "boot": boot,
                "active_boot": active_boot,
                "range": [start, end],
                "requested_interval": record.get("interval") or record.get("range") or [start, end],
                "byte_range": record.get("byte_range"),
                "object_binding": {"key": key, "rev": rev, "manifest_digest": manifest_digest},
                "historical_generation": historical,
                "blocks": block_results,
            }
            if historical:
                report["decision"] = "archived_as_old_node_generation"
                report["reason_code"] = "stale_boot_report"
            elif any(item["matches"] is False for item in block_results):
                report["decision"] = "isolated_mismatching_replicas"
                report["reason_code"] = "digest_mismatch"
            else:
                report["decision"] = "verified"
                report["reason_code"] = "digest_match"
            state.setdefault("scrub_reports", []).append(report)
            state["scrub_reports"] = state["scrub_reports"][-100:]
            seq = state["sequence"]
        self.journal("scrub", record, at, seq)
        return report

    def scrub(
        self,
        record: dict[str, Any],
        at: int | None = None,
        *,
        adopt_observed_bytes: bool = False,
    ) -> dict[str, Any]:
        """Convenience alias for report_scrub."""
        return self.report_scrub(record, at, adopt_observed_bytes=adopt_observed_bytes)

    @staticmethod
    def _scrub_observations(record: dict[str, Any]) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        if "items" in record:
            for item in record["items"]:
                result[int(item["block"])] = {"text": item.get("text"), "digest": item.get("digest")}
        if "blocks" in record and isinstance(record["blocks"], list):
            for value in record["blocks"]:
                if isinstance(value, dict):
                    result[int(value["block"])] = {"text": value.get("text"), "digest": value.get("digest")}
                else:
                    result[int(value)] = {"text": record.get("text"), "digest": record.get("digest")}
        if "blocks" in record and isinstance(record["blocks"], dict):
            for block, value in record["blocks"].items():
                if isinstance(value, dict):
                    result[int(block)] = {"text": value.get("text"), "digest": value.get("digest")}
                else:
                    result[int(block)] = {"digest": value}
        if "texts" in record and isinstance(record["texts"], dict):
            for block, text in record["texts"].items():
                result[int(block)] = {"text": text}
        if "block" in record:
            result[int(record["block"])] = {"text": record.get("text"), "digest": record.get("digest")}
        if not result:
            interval = record.get("range") or record.get("interval")
            if isinstance(interval, (list, tuple)) and len(interval) == 2:
                start, end = int(interval[0]), int(interval[1])
            else:
                start = int(record.get("start", 0))
                end = int(record["end"])
            for block in range(start, end):
                result[block] = {"text": None, "digest": None}
        return result

    def scrub_disk(
        self,
        node: str | None = None,
        key: str | None = None,
        rev: int | None = None,
        at: int | None = None,
    ) -> dict[str, Any]:
        reports = []
        with self.transaction() as state:
            nodes = [node] if node else list(state["nodes"].keys())
            for selected_node in nodes:
                boot = self.current_boot(state, selected_node)
                if key is None or rev is None:
                    reports.extend(self._scan_node(state, selected_node, boot, at=at))
                    continue
                version = self.version_state(state, key, rev)
                if version is None:
                    raise ReplicaStoreError(f"未知版本: {key} rev={rev}")
                for block_str, expected in sorted(version["blocks"].items(), key=lambda x: int(x[0])):
                    block = int(block_str)
                    data, digest, _ = self.hash_path(self.block_path(selected_node, key, rev, block))
                    matches = digest == expected if data is not None else False
                    self.add_observation(
                        state,
                        node=selected_node,
                        key=key,
                        rev=rev,
                        block=block,
                        source="scrub",
                        boot=boot,
                        observed_digest=digest,
                        verified=True,
                        matches=matches,
                        present=data is not None,
                        at=at,
                    )
                    reports.append(
                        {"node": selected_node, "block": block, "present": data is not None, "matches": matches, "digest": digest}
                    )
            seq = state["sequence"]
        self.journal("scrub", {"node": node, "key": key, "rev": rev, "disk": True}, at, seq)
        return {"reports": reports}
