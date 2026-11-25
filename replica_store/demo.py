"""Repeatable synthetic drill used by ``python -m replica_store demo``."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .replay import replay_file
from .store import ReplicaStore
from .util import write_atomic, write_json_atomic

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def run_demo(report_dir: str | Path, *, reset: bool = True) -> dict[str, Any]:
    report_dir = Path(report_dir)
    store_root = report_dir / "cluster"
    if reset:
        for path in (store_root, report_dir / "race", report_dir / "rejoin"):
            shutil.rmtree(path, ignore_errors=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    store = ReplicaStore(store_root)
    for node in ("a", "b", "c"):
        store.register_node(node, 1)

    # Three copies; one silent corruption appears on c.
    store.write_object(
        "process-archive",
        ["synthetic-block-0\n", "synthetic-block-1\n"],
        nodes=["a", "b", "c"],
    )
    healthy_read = store.read_object("process-archive")
    store.corrupt_replica("c", "process-archive", 1, 0, "silent disk corruption")
    failover_read = store.read_object("process-archive")
    scrub = store.scrub_disk("c", "process-archive", 1)
    store.set_priority("process-archive", "high")
    claimed = store.claim_next("fix-process-archive", batch_size=10)
    other_block_available = store.read_block("process-archive", 1)
    item = claimed["items"][0]
    store.record_result(
        {
            "id": claimed["id"],
            "block": item["block"],
            "boot": 1,
            "rev": 1,
            "text": "synthetic-block-0\n",
        }
    )
    unverified_report = store.inspect("process-archive")
    store.verify_repair({"id": claimed["id"], "block": item["block"], "text": "synthetic-block-0\n"})

    # A second job loses its coordinator mid-repair; reopening + restart scans disk.
    store.write_object("half-finished", ["half-data"], nodes=["a", "b", "c"])
    store.corrupt_replica("b", "half-finished", 1, 0, "bad-bytes")
    store.scrub_disk("b", "half-finished", 1)
    store.claim_repairs(
        "fix-half",
        [{"key": "half-finished", "rev": 1, "block": 0, "source": "a", "target": "b"}],
    )
    store.record_result({"id": "fix-half", "boot": 1, "rev": 1, "text": "half-data"})
    reopened = ReplicaStore(store_root)
    restart_summary = reopened.restart()

    # An object whose only two placement copies are both corrupt keeps a reason.
    reopened.write_object("cannot-yet-recover", ["important"], nodes=["a", "b"])
    for node, bad in (("a", "bad-a"), ("b", "bad-b")):
        reopened.corrupt_replica(node, "cannot-yet-recover", 1, 0, bad)
        reopened.scrub_disk(node, "cannot-yet-recover", 1)
    reopened.restart()

    race = replay_file(
        EXAMPLES / "repair-races-new-version.jsonl", report_dir / "race", reset=True
    )
    rejoin = replay_file(
        EXAMPLES / "late-scrub-after-rejoin.jsonl", report_dir / "rejoin", reset=True
    )

    report = reopened.inspect(scan=True)
    report["demo"] = {
        "initial_read_ok": healthy_read["ok"],
        "failover_served_by_node": failover_read["blocks"]["0"]["node"],
        "scrub_findings": scrub["reports"],
        "claimed_blocks": len(claimed["items"]),
        "other_block_during_repair": other_block_available["text"].strip(),
        "unverified_c_status": next(
            r["status"]
            for r in unverified_report["objects"][0]["blocks"]["0"]["replicas"]
            if r["node"] == "c"
        ),
        "restart_summary": restart_summary,
        "race_repair": race["report"]["latest_repair"]["reason_code"],
        "rejoin_repair": rejoin["report"]["latest_repair"]["reason_code"],
    }
    json_path = report_dir / "oncall-report.json"
    md_path = report_dir / "oncall-report.md"
    write_json_atomic(json_path, report)
    markdown = "\n".join(
        [
            "# 反复演练总记录",
            "",
            report["markdown"],
            "",
            "## 内置演练结论",
            "",
            f"- 三副本对象损坏 c/block0 后，在线读由 `{report['demo']['failover_served_by_node']}` 接管。",
            f"- 修复回报后、校验前，c 的状态是 `{report['demo']['unverified_c_status']}`，不获得可信资格。",
            f"- 检修 block0 期间，block1 仍可读取：`{report['demo']['other_block_during_repair']}`。",
            f"- 重启恢复结论：{restart_summary['jobs'][0]['status']} / {restart_summary['jobs'][0]['reason_code']}。",
            f"- 新版本竞争样例：{report['demo']['race_repair']}。",
            f"- 节点重加样例：{report['demo']['rejoin_repair']}。",
            "",
            "## 样例记录",
            "",
            f"- 竞争 JSON：`{race['report_json']}`；Markdown：`{race['report_markdown']}`",
            f"- 重加 JSON：`{rejoin['report_json']}`；Markdown：`{rejoin['report_markdown']}`",
            "",
        ]
    )
    write_atomic(md_path, markdown.encode("utf-8"))
    report["markdown_path"] = str(md_path)
    report["json_path"] = str(json_path)
    return {"report": report, "markdown": markdown, "json_path": str(json_path), "md_path": str(md_path)}
