"""On-call inspection records and Markdown rendering."""

from __future__ import annotations

from typing import Any


class InspectionMixin:
    pass

    def inspect(self, key: str | None = None, scan: bool = False, at: int | None = None) -> dict[str, Any]:
        with self.transaction() as state:
            if scan:
                for node in list(state["nodes"].keys()):
                    boot = self.current_boot(state, node)
                    self._scan_node(state, node, boot, at=at)
            report = self._build_report(state, key)
            state["last_report"] = report
            state.setdefault("inspections", []).append({"seq": self.next_seq(state), "at": at, "key": key})
            state["inspections"] = state["inspections"][-50:]
            seq = state["sequence"]
        self.journal("inspect", {"key": key, "scan": scan}, at, seq)
        report["markdown"] = render_markdown(report)
        return report

    def _build_report(self, state: dict[str, Any], key: str | None) -> dict[str, Any]:
        object_keys = [key] if key else sorted(state["objects"].keys())
        objects = []
        missing = []
        for object_key in object_keys:
            obj = self.object_state(state, object_key)
            if obj is None:
                continue
            rev = obj.get("current_rev")
            object_report: dict[str, Any] = {
                "key": object_key,
                "priority": obj.get("priority", "normal"),
                "current_rev": rev,
                "committed": rev is not None,
                "blocks": {},
                "readable": False,
            }
            if rev is None:
                object_report["reason_code"] = "no_committed_version"
                objects.append(object_report)
                missing.append({"key": object_key, "rev": None, "block": None, "reason_code": "no_committed_version"})
                continue
            version = self.version_state(state, object_key, int(rev))
            assert version is not None
            object_report["manifest_digest"] = version.get("manifest_digest")
            all_blocks_readable = True
            for block_str, digest in sorted(version["blocks"].items(), key=lambda item: int(item[0])):
                block = int(block_str)
                nodes = list(version.get("placement", []))
                for node in state["nodes"]:
                    if node not in nodes:
                        nodes.append(node)
                replicas = [
                    self.replica_classification(state, node, object_key, int(rev), block) for node in nodes
                ]
                trusted = [r for r in replicas if r["trusted"]]
                invalid = [r for r in replicas if not r["trusted"]]
                placement_nodes = list(version.get("targets") or version.get("placement") or nodes)
                target_statuses = [r for r in replicas if r["node"] in placement_nodes]
                target_trusted = [r for r in target_statuses if r["trusted"]]
                target_invalid = [r for r in target_statuses if not r["trusted"]]
                reason = self._missing_reason(replicas, trusted, placement_nodes)
                block_report = {
                    "block": block,
                    "digest": digest,
                    "readable": bool(trusted),
                    "valid_count": len(trusted),
                    "target_valid_count": len(target_trusted),
                    "placement_count": len(placement_nodes),
                    "desired_count": len(placement_nodes),
                    "placement_nodes": placement_nodes,
                    "trusted_nodes": [r["node"] for r in trusted],
                    "missing_copy_nodes": [r["node"] for r in target_invalid],
                    "untrusted_replica_nodes": [r["node"] for r in invalid],
                    "under_replicated": len(target_trusted) < len(placement_nodes),
                    "lacks_any_valid_copy": not trusted,
                    "missing_reason": reason if not trusted else None,
                    "replicas": replicas,
                }
                object_report["blocks"][block_str] = block_report
                if not trusted:
                    all_blocks_readable = False
                    missing.append(
                        {
                            "key": object_key,
                            "rev": int(rev),
                            "block": block,
                            "digest": digest,
                            "reason_code": reason["code"],
                            "reason": reason["message"],
                            "details": invalid,
                        }
                    )
            object_report["readable"] = all_blocks_readable
            object_report["blocks_without_any_valid_copy"] = [
                int(block) for block, item in object_report["blocks"].items() if item["lacks_any_valid_copy"]
            ]
            object_report["blocks_with_missing_replica"] = [
                int(block)
                for block, item in object_report["blocks"].items()
                if item["under_replicated"] or item["lacks_any_valid_copy"]
            ]
            object_report["latest_repair"] = self._latest_repair(state, object_key)
            objects.append(object_report)
        latest_repair = self._latest_repair(state, key)
        queue = []
        for item in self._plan_repairs(state, key)[:10]:
            queue.append(
                {
                    "key": item["key"],
                    "rev": item["rev"],
                    "block": item["block"],
                    "priority": item["priority"],
                    "source": item["source"],
                    "target": item["target"],
                    "unreadable": item["unreadable"],
                    "reason_code": item["reason_code"],
                }
            )
        return {
            "root": str(getattr(self, "root", "")),
            "object_filter": key,
            "current": {item["key"]: {"rev": item["current_rev"], "readable": item["readable"]} for item in objects},
            "objects": objects,
            "missing_blocks": missing,
            "latest_repair": latest_repair,
            "repair_queue": queue,
            "node_generations": {
                node: {"current_boot": data.get("current_boot"), "boots": [b.get("boot") for b in data.get("boots", [])]}
                for node, data in state.get("nodes", {}).items()
            },
            "scrub_report_count": len(state.get("scrub_reports", [])),
        }

    @staticmethod
    def _missing_reason(
        replicas: list[dict[str, Any]],
        trusted: list[dict[str, Any]],
        placement_nodes: list[str] | None = None,
    ) -> dict[str, str]:
        if trusted:
            return {"code": "none", "message": "存在可信副本"}
        statuses = [r["status"] for r in replicas]
        status_set = set(statuses)
        placement_nodes = placement_nodes or []
        placement_replicas = [r for r in replicas if r["node"] in placement_nodes]
        placement_statuses = {r["status"] for r in placement_replicas}
        corrupt = [r for r in replicas if r["status"] == "corrupt"]
        missing_like = status_set & {"missing", "absent", "unknown_generation"}
        unverified = status_set & {"written_unverified", "unverified"}
        if placement_replicas and placement_statuses == {"corrupt"}:
            return {"code": "all_placement_replicas_corrupt", "message": "预定副本节点上的所有已知字节摘要均与清单不同"}
        if not replicas or status_set == {"absent"}:
            return {"code": "no_replicas_present", "message": "没有任何节点实际拥有该块"}
        if status_set <= {"corrupt"}:
            return {"code": "all_known_replicas_corrupt", "message": "所有已知副本摘要均与已提交清单不同"}
        if corrupt and missing_like and not unverified:
            return {"code": "corrupt_and_missing_replicas", "message": "部分已知副本摘要错误，其余副本缺失或处于旧代次"}
        if status_set <= {"missing", "absent", "unknown_generation"}:
            return {"code": "all_replicas_missing_or_unknown", "message": "没有可证明为当前节点代次且实际存在的副本"}
        if "written_unverified" in status_set and not (status_set & {"corrupt", "missing"}):
            return {"code": "repair_written_but_unverified", "message": "只有写入回执，尚无独立校验或实际哈希验证"}
        return {
            "code": "no_verified_replica",
            "message": "已知副本缺失、摘要不符或处于旧节点代次，不能用于回复",
        }

    @staticmethod
    def _latest_repair(state: dict[str, Any], key: str | None = None) -> dict[str, Any] | None:
        candidates = []
        for job in state.get("jobs", {}).values():
            for item in job.get("items", []):
                if key is not None and item.get("key") != key:
                    continue
                candidates.append((item.get("claimed_seq", 0), job, item))
        if not candidates:
            return None
        _, job, item = max(candidates, key=lambda pair: pair[0])
        return {
            "job_id": job.get("id"),
            "batch_status": job.get("status"),
            **{
                k: item.get(k)
                for k in (
                    "item_id",
                    "key",
                    "rev",
                    "block",
                    "digest",
                    "source_node",
                    "source_boot",
                    "target_node",
                    "expected_target_boot",
                    "status",
                    "is_valid",
                    "is_stale",
                    "reason_code",
                    "reason",
                    "checks",
                    "claimed_seq",
                    "completed_seq",
                    "verified_seq",
                )
            },
        }


def render_markdown(report: dict[str, Any]) -> str:
    lines = ["# 副本存储值班记录", ""]
    if report.get("object_filter"):
        lines.append(f"查询对象：`{report['object_filter']}`")
    lines.append("")
    lines.append("## 当前已提交版本")
    if not report.get("objects"):
        lines.append("- 暂无对象")
    for obj in report.get("objects", []):
        state_text = "完整可读" if obj.get("readable") else "暂不能完整读取"
        lines.append(f"- `{obj['key']}`：当前版本 rev={obj.get('current_rev')}，{state_text}，优先级={obj.get('priority')}")
        for block_id, block in obj.get("blocks", {}).items():
            marks = []
            for replica in block.get("replicas", []):
                marks.append(f"{replica['node']}/boot{replica.get('boot')}:{replica['status']}")
            lines.append(
                f"  - 块 {block_id}：有效副本 {block['valid_count']}/{block['desired_count']}；"
                f"可信节点 {','.join(block['trusted_nodes']) or '无'}；状态 [{'；'.join(marks)}]"
            )
            if block.get("lacks_any_valid_copy"):
                reason = block["missing_reason"]
                lines.append(f"    - 缺失原因：`{reason['code']}` {reason['message']}")
    lines.append("")
    lines.append("## 缺少有效副本的块")
    if not report.get("missing_blocks"):
        lines.append("- 无：当前版本每块都有已校验可信副本")
    for item in report.get("missing_blocks", []):
        lines.append(
            f"- `{item['key']}` rev={item.get('rev')} block={item.get('block')}："
            f"`{item['reason_code']}` {item.get('reason', '')}"
        )
    lines.append("")
    repair = report.get("latest_repair")
    lines.append("## 最新一次修复")
    if not repair:
        lines.append("- 暂无修复记录")
    else:
        verdict = "有效" if repair.get("is_valid") else ("过时" if repair.get("is_stale") else "尚未生效/未校验")
        lines.append(f"- 作业 `{repair.get('job_id')}` / {repair.get('key')} rev={repair.get('rev')} block={repair.get('block')}")
        lines.append(f"- 结论：**{verdict}**；状态 `{repair.get('status')}`；原因代码 `{repair.get('reason_code')}`")
        if repair.get("reason"):
            lines.append(f"- 说明：{repair['reason']}")
        lines.append(
            f"- 来源：{repair.get('source_node')} boot={repair.get('source_boot')}；"
            f"目标：{repair.get('target_node')} 预定 boot={repair.get('expected_target_boot')}"
        )
        if repair.get("checks"):
            checks = "，".join(f"{k}={'是' if v else '否' if v is False else v}" for k, v in repair["checks"].items())
            lines.append(f"- 代次检查：{checks}")
    lines.append("")
    lines.append("## 修复领取顺序")
    if not report.get("repair_queue"):
        lines.append("- 无待领取项")
    for item in report.get("repair_queue", []):
        target = item.get("target") or "无目标"
        source = item.get("source") or "无已校验来源"
        lines.append(
            f"- [{item['priority']}] `{item['key']}` rev={item['rev']} block={item['block']} "
            f"{source} -> {target}（{item['reason_code']}）"
        )
    lines.append("")
    return "\n".join(lines)
