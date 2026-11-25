"""Replay the documented JSONL drills through the file-backed coordinator."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from scenario_reader import iter_messages

from .errors import BlockUnavailable, RepairRejected
from .store import ReplicaStore
from .util import write_atomic, write_json_atomic

DEFAULT_MESSAGE_TYPES = {
    "commit",
    "inspect",
    "node",
    "read",
    "repair",
    "restart",
    "result",
    "scrub",
    "stored",
    "verify",
    "version",
}


def replay_file(
    path: str | Path,
    root: str | Path,
    *,
    reset: bool = False,
    message_types: set[str] | list[str] | None = None,
) -> dict[str, Any]:
    path = Path(path)
    root = Path(root)
    if reset and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    known = set(message_types) if message_types is not None else _load_types(path)
    store = ReplicaStore(root)
    outputs: list[dict[str, Any]] = []
    count = 0
    for row in iter_messages(path, known):
        count += 1
        outputs.append(_handle_message(store, row["kind"], row["record"], row["at"]))
    report = store.inspect(scan=False)
    write_json_atomic(root / "report.json", report)
    write_atomic(root / "report.md", report["markdown"].encode("utf-8"))
    summary = {
        "scenario": path.name,
        "events": count,
        "root": str(root),
        "report_json": str(root / "report.json"),
        "report_markdown": str(root / "report.md"),
        "report": report,
        "outputs": outputs,
    }
    write_json_atomic(root / "replay-summary.json", {k: v for k, v in summary.items() if k != "report"})
    return summary


def _load_types(path: Path) -> set[str]:
    registry = path.parent / "message_types.json"
    if registry.exists():
        return set(json.loads(registry.read_text(encoding="utf-8")))
    return set(DEFAULT_MESSAGE_TYPES)


def _handle_message(store: ReplicaStore, kind: str, record: dict[str, Any], at: int) -> dict[str, Any]:
    if kind == "node":
        return {"kind": kind, "result": store.register_node(record["node"], int(record["boot"]), at)}
    if kind == "version":
        generation = record.get("generation") or record.get("write_id")
        placement = record.get("nodes") or record.get("targets") or record.get("placement")
        return {
            "kind": kind,
            "result": store.propose_manifest(
                record["key"],
                record["blocks"],
                rev=int(record["rev"]),
                generation=generation,
                at=at,
                placement=placement,
            ),
        }
    if kind == "stored":
        return {"kind": kind, "result": store.record_stored(record, at)}
    if kind == "commit":
        generation = record.get("generation") or record.get("write_id")
        placement = record.get("nodes") or record.get("targets") or record.get("placement")
        return {
            "kind": kind,
            "result": store.commit(
                record["key"], int(record["rev"]), generation, at, placement=placement
            ),
        }
    if kind == "read":
        try:
            if "block" in record:
                result = store.read_block(
                    record["key"], int(record["block"]), rev=_optional_int(record.get("rev")), at=at
                )
            else:
                result = store.read_object(record["key"], rev=_optional_int(record.get("rev")), at=at)
            result = {k: v for k, v in result.items() if k != "data"}
            return {"kind": kind, "ok": True, "result": result}
        except BlockUnavailable as error:
            return {"kind": kind, "ok": False, "result": error.report}
    if kind == "scrub":
        return {"kind": kind, "result": store.report_scrub(record, at, adopt_observed_bytes=True)}
    if kind == "repair":
        try:
            return {"kind": kind, "result": store.claim_repair(record, at)}
        except RepairRejected as error:
            return {"kind": kind, "ok": False, "error": str(error), "decision": error.decision}
    if kind == "result":
        return {"kind": kind, "result": store.record_result(record, at)}
    if kind == "verify":
        return {"kind": kind, "result": store.verify_repair(record, at)}
    if kind == "restart":
        return {"kind": kind, "result": store.restart(at)}
    if kind == "inspect":
        report = store.inspect(record.get("key"), scan=bool(record.get("scan", False)), at=at)
        target = root_report_path(store)
        write_json_atomic(target, report)
        write_atomic(target.with_suffix(".md"), report["markdown"].encode("utf-8"))
        return {"kind": kind, "result": {"key": record.get("key"), "report_json": str(target)}}
    raise ValueError(f"未处理的消息类型: {kind}")


def root_report_path(store: ReplicaStore) -> Path:
    path = store.root / "reports"
    path.mkdir(parents=True, exist_ok=True)
    return path / "inspection.json"


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


# Public alias for callers that want to feed one validated message at a time.
handle_message = _handle_message


class ScenarioEngine:
    """Stateful, repeatable JSONL message engine backed by a ReplicaStore."""

    def __init__(self, root: str | Path, reset: bool = False):
        self.root = Path(root)
        if reset and self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = ReplicaStore(self.root)
        self.outputs: list[dict[str, Any]] = []

    def handle(self, kind: str, record: dict[str, Any], at: int = 0) -> dict[str, Any]:
        output = handle_message(self.store, kind, record, at)
        self.outputs.append(output)
        return output

    def replay(self, path: str | Path, message_types: set[str] | list[str] | None = None) -> dict[str, Any]:
        return replay_file(path, self.root, reset=False, message_types=message_types)

    def inspect(self, key: str | None = None, scan: bool = False) -> dict[str, Any]:
        return self.store.inspect(key, scan=scan)
