"""Restart reconciliation from durable state plus actual local block files."""

from __future__ import annotations

from typing import Any


class RecoveryMixin:
    # BaseStore supplies transaction/journal/next_seq in ReplicaStore.

    def restart(self, at: int | None = None) -> dict[str, Any]:
        with self.transaction() as state:
            restart_seq = self.next_seq(state)
            state["restarts"] = int(state.get("restarts", 0)) + 1
            scanned = []
            for node in list(state.get("nodes", {}).keys()):
                boot = self.current_boot(state, node)
                scanned.extend(self._scan_node(state, node, boot, at=at))
            recovered_jobs = self._reconcile_jobs_after_restart(state, at)
            missing = self._persist_missing_reasons(state, restart_seq, at)
            result = {
                "restart_seq": restart_seq,
                "scanned_replicas": len(scanned),
                "jobs": recovered_jobs,
                "missing_blocks": missing,
            }
            seq = state["sequence"]
        self.journal("restart", {}, at, seq)
        return result

    def _reconcile_jobs_after_restart(self, state: dict[str, Any], at: int | None) -> list[dict[str, Any]]:
        summaries = []
        for job in state.get("jobs", {}).values():
            for item in job.get("items", []):
                before = item.get("status")
                if before in {"verified", "stale", "failed"}:
                    continue
                obj = self.object_state(state, item["key"])
                current_rev = None if obj is None else obj.get("current_rev")
                if not item.get("historical") and current_rev != item.get("fence_current_rev"):
                    item["status"] = "stale"
                    item["is_stale"] = True
                    item["is_valid"] = False
                    item["reason_code"] = "new_version_committed_during_restart"
                    item["reason"] = "重启恢复时发现对象代次已经前进；半途旧修复作废，不能覆盖新版本"
                    item["recovery_seq"] = self.next_seq(state)
                    self._set_lock_status(state, item, "stale")
                else:
                    data, digest, _ = self.hash_path(
                        self.block_path(item["target_node"], item["key"], item["rev"], item["block"])
                    )
                    if data is not None and digest == item["digest"]:
                        item["status"] = "verified"
                        item["is_valid"] = True
                        item["is_stale"] = False
                        item["reason_code"] = "restart_verified_from_actual_disk"
                        item["reason"] = "重启后直接哈希实际落盘块，摘要与预定块身份一致"
                        item["verified_seq"] = self.next_seq(state)
                        item["verified_at"] = at
                        item["recovery"] = {"from_status": before, "actual_digest": digest}
                        self._set_lock_status(state, item, "completed")
                    else:
                        source_boot, source_digest = self._verify_node_bytes(
                            state,
                            item["source_node"],
                            item["key"],
                            item["rev"],
                            item["block"],
                            item["digest"],
                            at,
                            "restart_source_check",
                        )
                        if source_digest != item["digest"]:
                            alternate = self._find_verified_source(
                                state, item["key"], item["rev"], item["block"], item["digest"]
                            )
                            if alternate:
                                item["source_node"] = alternate
                                source_boot = self.current_boot(state, alternate)
                                source_digest = item["digest"]
                                item["source_boot"] = source_boot
                                item["source_failover"] = True
                        item["recovery_seq"] = self.next_seq(state)
                        if source_digest == item["digest"]:
                            item["status"] = "retry_ready"
                            item["is_valid"] = False
                            item["reason_code"] = "interrupted_before_complete_write"
                            item["reason"] = "目标块未按预期落盘，但仍有已校验的相同版本/块身份来源，可重新领取修复"
                        else:
                            item["status"] = "blocked"
                            item["is_valid"] = False
                            item["reason_code"] = "unrecoverable_no_verified_source"
                            item["reason"] = "重启后证明目标不完整，且已知来源也没有相同版本和块身份的可信副本"
                            item["missing_since_seq"] = item["recovery_seq"]
                            self._set_lock_status(state, item, "failed")
                self._aggregate_job(job)
                summaries.append(
                    {
                        "job_id": job["id"],
                        "item_id": item.get("item_id"),
                        "from_status": before,
                        "status": item.get("status"),
                        "reason_code": item.get("reason_code"),
                    }
                )
        return summaries

    def _persist_missing_reasons(self, state: dict[str, Any], seq: int, at: int | None) -> list[dict[str, Any]]:
        report = self._build_report(state, None)
        current_missing = {}
        for item in report.get("missing_blocks", []):
            key = item["key"]
            obj = self.object_state(state, key, create=True)
            assert obj is not None
            reasons = obj.setdefault("missing_reasons", {})
            block_token = str(item["block"])
            reason = {
                "rev": item.get("rev"),
                "block": item.get("block"),
                "code": item["reason_code"],
                "message": item.get("reason"),
                "since_seq": seq,
                "at": at,
            }
            reasons[block_token] = reason
            obj.setdefault("missing_history", []).append(dict(reason))
            current_missing[(key, block_token)] = True
        for obj in state.get("objects", {}).values():
            reasons = obj.get("missing_reasons", {})
            for block_token in list(reasons.keys()):
                key = None
                # Resolve key by object identity is unnecessary for cleanup scan below.
            # Clean only via object report block state where trusted now exists.
        for object_report in report.get("objects", []):
            obj = self.object_state(state, object_report["key"])
            if obj is None:
                continue
            for block_token, block in object_report.get("blocks", {}).items():
                if block.get("trusted_nodes"):
                    if block_token in obj.get("missing_reasons", {}):
                        resolved = obj["missing_reasons"].pop(block_token)
                        obj.setdefault("missing_history", []).append(
                            {"resolved_seq": seq, "at": at, "resolved_reason": resolved}
                        )
        return report.get("missing_blocks", [])
