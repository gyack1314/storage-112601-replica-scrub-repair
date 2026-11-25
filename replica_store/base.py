"""File-backed coordinator primitives and state helpers."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import ReplicaStoreError
from .util import (
    append_jsonl,
    as_int,
    manifest_digest,
    normalize_digest,
    read_json,
    safe_component,
    sha256_bytes,
    unsafe_component,
    write_json_atomic,
)

SCHEMA = 1


def default_state() -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "sequence": 0,
        "restarts": 0,
        "nodes": {},
        "objects": {},
        "jobs": {},
        "job_order": [],
        "scrub_reports": [],
        "read_results": [],
        "inspections": [],
        "last_report": None,
    }


class BaseStore:
    """Owns the local directory, lock, JSON state and actual block files."""

    def __init__(self, root: str | Path, default_nodes: tuple[str, ...] = ("a", "b", "c")):
        self.root = Path(root)
        self.default_nodes = tuple(default_nodes)
        self.state_path = self.root / "coordinator" / "state.json"
        self.journal_path = self.root / "coordinator" / "journal.jsonl"
        self.lock_path = self.root / "coordinator" / "store.lock"

    @contextmanager
    def transaction(self) -> Iterator[dict[str, Any]]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            state = self._load_state()
            try:
                yield state
                write_json_atomic(self.state_path, state)
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _load_state(self) -> dict[str, Any]:
        state = read_json(self.state_path)
        if state is None:
            return default_state()
        if not isinstance(state, dict) or state.get("schema") != SCHEMA:
            raise ReplicaStoreError(f"状态文件模式不受支持: {self.state_path}")
        for key, value in default_state().items():
            state.setdefault(key, value if not isinstance(value, (dict, list)) else type(value)())
        return state

    def journal(self, kind: str, record: dict[str, Any], at: int | None = None, seq: int | None = None) -> None:
        append_jsonl(
            self.journal_path,
            {"seq": seq, "at": at, "kind": kind, "record": record},
        )

    @staticmethod
    def next_seq(state: dict[str, Any]) -> int:
        state["sequence"] = int(state.get("sequence", 0)) + 1
        return state["sequence"]

    # ---------- paths and actual files ----------

    def node_root(self, node: str) -> Path:
        return self.root / "nodes" / safe_component(str(node))

    def block_path(self, node: str, key: str, rev: int, block: int) -> Path:
        return (
            self.node_root(node)
            / "objects"
            / safe_component(key)
            / f"rev-{as_int(rev, 'rev')}"
            / f"block-{as_int(block, 'block')}.bin"
        )

    def hash_path(self, path: Path) -> tuple[bytes | None, str | None, int | None]:
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            return None, None, None
        return data, sha256_bytes(data), len(data)

    def parse_block_path(self, path: Path) -> tuple[str, int, int] | None:
        try:
            block_name = path.name
            if not block_name.startswith("block-") or not block_name.endswith(".bin"):
                return None
            block = int(block_name[len("block-") : -len(".bin")])
            rev_part = path.parent.name
            if not rev_part.startswith("rev-"):
                return None
            rev = int(rev_part[len("rev-") :])
            key = unsafe_component(path.parent.parent.name)
            return key, rev, block
        except (ValueError, IndexError):
            return None

    # ---------- state lookup helpers ----------

    @staticmethod
    def object_state(state: dict[str, Any], key: str, create: bool = False) -> dict[str, Any] | None:
        objects = state["objects"]
        if key not in objects:
            if not create:
                return None
            objects[key] = {
                "priority": "normal",
                "current_rev": None,
                "versions": {},
                "missing_reasons": {},
                "missing_history": [],
            }
        return objects[key]

    @classmethod
    def version_state(
        cls, state: dict[str, Any], key: str, rev: int, create: bool = False
    ) -> dict[str, Any] | None:
        obj = cls.object_state(state, key, create=create)
        if obj is None:
            return None
        rev = as_int(rev, "rev")
        versions = obj["versions"]
        token = str(rev)
        if token not in versions:
            if not create:
                return None
            versions[token] = {
                "key": key,
                "rev": rev,
                "committed": False,
                "blocks": {},
                "placement": [],
                "generation": f"{key}@{rev}",
            }
        return versions[token]

    @classmethod
    def node_state(cls, state: dict[str, Any], node: str, create: bool = False) -> dict[str, Any] | None:
        nodes = state["nodes"]
        if node not in nodes:
            if not create:
                return None
            nodes[node] = {"current_boot": None, "boots": [], "replicas": {}}
        return nodes[node]

    @classmethod
    def replica_state(
        cls,
        state: dict[str, Any],
        node: str,
        key: str,
        rev: int,
        block: int,
        create: bool = False,
    ) -> dict[str, Any] | None:
        nd = cls.node_state(state, node, create=create)
        if nd is None:
            return None
        rev, block = as_int(rev, "rev"), as_int(block, "block")
        key_map = nd["replicas"].setdefault(key, {})
        rev_map = key_map.setdefault(str(rev), {})
        token = str(block)
        if token not in rev_map:
            if not create:
                return None
            rev_map[token] = {
                "node": node,
                "key": key,
                "rev": rev,
                "block": block,
                "history": [],
            }
        return rev_map[token]

    @classmethod
    def add_observation(
        cls,
        state: dict[str, Any],
        *,
        node: str,
        key: str,
        rev: int,
        block: int,
        source: str,
        boot: int | None,
        observed_digest: str | None,
        verified: bool,
        matches: bool | None = None,
        present: bool = True,
        at: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rep = cls.replica_state(state, node, key, rev, block, create=True)
        assert rep is not None
        version = cls.version_state(state, key, rev)
        expected = None
        if version is not None:
            expected = version.get("blocks", {}).get(str(block))
        if matches is None and expected and observed_digest:
            matches = observed_digest == expected
        entry = {
            "seq": cls.next_seq(state),
            "at": at,
            "source": source,
            "boot": boot,
            "present": present,
            "observed_digest": observed_digest,
            "expected_digest": expected,
            "matches": matches,
            "verified": bool(verified),
        }
        if extra:
            entry.update(extra)
        rep["history"].append(entry)
        rep["last_seq"] = entry["seq"]
        return entry

    @classmethod
    def current_boot(cls, state: dict[str, Any], node: str) -> int | None:
        nd = cls.node_state(state, node)
        return None if nd is None else nd.get("current_boot")

    @staticmethod
    def latest_current_boot_entry(rep: dict[str, Any], boot: int | None) -> dict[str, Any] | None:
        for entry in reversed(rep.get("history", [])):
            if boot is not None and entry.get("boot") != boot:
                continue
            return entry
        return None

    @classmethod
    def replica_classification(
        cls, state: dict[str, Any], node: str, key: str, rev: int, block: int
    ) -> dict[str, Any]:
        nd = cls.node_state(state, node)
        rep = cls.replica_state(state, node, key, rev, block)
        boot = cls.current_boot(state, node)
        version = cls.version_state(state, key, rev)
        expected = None if version is None else version.get("blocks", {}).get(str(block))
        if rep is None:
            return {
                "node": node,
                "boot": boot,
                "status": "absent",
                "trusted": False,
                "reason_code": "no_observation",
            }
        entry = cls.latest_current_boot_entry(rep, boot)
        if entry is None:
            return {
                "node": node,
                "boot": boot,
                "status": "unknown_generation",
                "trusted": False,
                "reason_code": "no_observation_for_current_boot",
                "history_count": len(rep.get("history", [])),
            }
        observed = entry.get("observed_digest")
        digest_ok = bool(expected and observed == expected)
        if entry.get("present") is False:
            status, code, trusted = "missing", "replica_file_missing", False
        elif digest_ok and entry.get("verified"):
            status, code, trusted = "valid", "hash_verified", True
        elif digest_ok and not entry.get("verified"):
            status, code, trusted = "written_unverified", "write_report_not_verified", False
        elif entry.get("matches") is False or (expected and observed and not digest_ok):
            status, code, trusted = "corrupt", "digest_mismatch", False
        else:
            status, code, trusted = "unverified", "identity_not_proven", False
        return {
            "node": node,
            "boot": boot,
            "status": status,
            "trusted": trusted,
            "reason_code": code,
            "source": entry.get("source"),
            "seq": entry.get("seq"),
            "at": entry.get("at"),
            "observed_digest": observed,
            "expected_digest": expected,
        }

    # ---------- node and manifest operations ----------

    def register_node(
        self,
        node: str,
        boot: int,
        at: int | None = None,
        *,
        scan: bool = True,
    ) -> dict[str, Any]:
        node = str(node)
        boot = as_int(boot, "boot")
        with self.transaction() as state:
            result = self._register_node(state, node, boot, at, scan=scan)
            seq = state["sequence"]
        self.journal("node", {"node": node, "boot": boot}, at, seq)
        return result

    def _register_node(
        self, state: dict[str, Any], node: str, boot: int, at: int | None, *, scan: bool
    ) -> dict[str, Any]:
        nd = self.node_state(state, node, create=True)
        assert nd is not None
        known_boots = [item["boot"] for item in nd.get("boots", [])]
        if boot not in known_boots:
            nd.setdefault("boots", []).append({"boot": boot, "seq": self.next_seq(state), "at": at})
        old_boot = nd.get("current_boot")
        # A delayed old node fact must not move the active generation backwards.
        if old_boot is None or boot >= old_boot:
            nd["current_boot"] = boot
            active_boot = boot
        else:
            active_boot = old_boot
        scanned = self._scan_node(state, node, active_boot, at=at) if scan else []
        return {"node": node, "boot": active_boot, "registered_boot": boot, "scanned": scanned}

    def propose_manifest(
        self,
        key: str,
        blocks: dict[str, str] | list[str],
        rev: int | None = None,
        generation: str | None = None,
        at: int | None = None,
        placement: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        with self.transaction() as state:
            obj = self.object_state(state, key, create=True)
            assert obj is not None
            if rev is None:
                rev = int(obj.get("current_rev") or max(map(int, obj["versions"].keys()), default=0)) + 1
            rev = as_int(rev, "rev")
            digests = self._digest_map(blocks)
            existing = self.version_state(state, key, rev)
            if existing is not None:
                if existing["blocks"] != digests:
                    from .errors import ManifestConflict

                    raise ManifestConflict(f"{key} rev={rev} 已存在不同清单")
                result = existing
            else:
                version = self.version_state(state, key, rev, create=True)
                assert version is not None
                version["blocks"] = digests
                version["manifest_digest"] = manifest_digest(digests)
                version["generation"] = generation or f"{key}@{rev}:{version['manifest_digest'][:12]}"
                if placement:
                    version["targets"] = [str(node) for node in placement]
                    version["placement"] = list(dict.fromkeys(version.get("placement", []) + [str(n) for n in placement]))
                version["proposed_seq"] = self.next_seq(state)
                version["proposed_at"] = at
                result = version
            seq = state["sequence"]
        self.journal(
            "version",
            {"key": key, "rev": rev, "blocks": digests, "generation": generation},
            at,
            seq,
        )
        return result

    @staticmethod
    def _digest_map(blocks: dict[str, Any] | list[Any]) -> dict[str, str]:
        if isinstance(blocks, list):
            items = enumerate(blocks)
        elif isinstance(blocks, dict):
            items = blocks.items()
        else:
            raise ValueError("blocks 必须是列表或 块号->摘要/字节 的对象")
        return {str(index): normalize_digest(value) for index, value in items}

    def commit(
        self,
        key: str,
        rev: int,
        generation: str | None = None,
        at: int | None = None,
        placement: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        rev = as_int(rev, "rev")
        with self.transaction() as state:
            version = self.version_state(state, key, rev)
            if version is None:
                raise ReplicaStoreError(f"未知版本: {key} rev={rev}")
            if generation is not None and generation != version.get("generation"):
                from .errors import GenerationFenceError

                raise GenerationFenceError("发布确认的写入代次与清单不一致")
            if placement:
                version["targets"] = [str(node) for node in placement]
            version["committed"] = True
            version["commit_seq"] = self.next_seq(state)
            version["committed_at"] = at
            obj = self.object_state(state, key, create=True)
            assert obj is not None
            if not version.get("targets"):
                previous = [
                    candidate
                    for token, candidate in obj["versions"].items()
                    if candidate is not version and candidate.get("committed") and int(token) < rev
                ]
                if previous:
                    newest_previous = max(previous, key=lambda item: int(item["rev"]))
                    inherited = newest_previous.get("targets") or newest_previous.get("placement") or []
                    if inherited:
                        version["targets"] = list(inherited)
            if version.get("targets"):
                merged = list(version.get("targets", []))
                for node in version.get("placement", []):
                    if node not in merged:
                        merged.append(node)
                version["placement"] = merged
            if obj.get("current_rev") is None or rev >= int(obj["current_rev"]):
                obj["current_rev"] = rev
                current = rev
            else:
                current = obj["current_rev"]
            result = {"key": key, "rev": rev, "current_rev": current, "committed": True}
            seq = state["sequence"]
        self.journal("commit", {"key": key, "rev": rev, "generation": generation}, at, seq)
        return result

    # ---------- disk scan ----------

    def scan_all(self, at: int | None = None) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        with self.transaction() as state:
            for node in list(state["nodes"].keys()):
                boot = self.current_boot(state, node)
                found.extend(self._scan_node(state, node, boot, at=at))
        return found

    def _scan_node(
        self,
        state: dict[str, Any],
        node: str,
        boot: int | None,
        at: int | None = None,
    ) -> list[dict[str, Any]]:
        root = self.node_root(node) / "objects"
        if not root.exists():
            return []
        found = []
        for path in sorted(root.rglob("block-*.bin")):
            parsed = self.parse_block_path(path)
            if parsed is None:
                continue
            key, rev, block = parsed
            data, digest, size = self.hash_path(path)
            version = self.version_state(state, key, rev)
            expected = None if version is None else version.get("blocks", {}).get(str(block))
            matches = None if expected is None else digest == expected
            self.add_observation(
                state,
                node=node,
                key=key,
                rev=rev,
                block=block,
                source="restart_scan",
                boot=boot,
                observed_digest=digest,
                verified=True,
                matches=matches,
                present=data is not None,
                at=at,
                extra={"size": size, "actual_scan": True},
            )
            if matches and version is not None and node not in version.setdefault("placement", []):
                version["placement"].append(node)
            found.append({"node": node, "key": key, "rev": rev, "block": block, "digest": digest, "matches": matches})
        return found
