"""Small encoding, hashing and filesystem helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, unquote

_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def text_bytes(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError("synthetic block must be str or bytes")


def is_digest(value: Any) -> bool:
    return isinstance(value, str) and bool(_HEX_RE.fullmatch(value))


def block_digest(value: Any) -> str:
    """Return a digest from either a hex digest or synthetic text/bytes."""
    if is_digest(value):
        return value
    return sha256_bytes(text_bytes(value))


def manifest_digest(blocks: Mapping[Any, Any]) -> str:
    normalized = {str(index): normalize_digest(value) for index, value in blocks.items()}
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(raw)


def normalize_digest(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("digest") or value.get("sha256") or value.get("id")
    if not is_digest(value):
        raise ValueError(f"不是合法的 SHA-256 摘要: {value!r}")
    return value


def safe_component(value: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."} or "/" in value or "\\" in value:
        raise ValueError(f"不安全的名称: {value!r}")
    return quote(value, safe="._-")


def unsafe_component(value: str) -> str:
    return unquote(value)


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with tmp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def write_json_atomic(path: Path, value: Any) -> None:
    write_atomic(path, (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def as_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
            value = int(value)
        else:
            raise ValueError(f"{name} 必须是整数")
    if value < 0 and name in {"rev", "boot", "block"}:
        raise ValueError(f"{name} 不能为负数")
    return value
