"""Public local replica store composed from focused mixins."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import BaseStore
from .inspection import InspectionMixin
from .io_ops import ReadWriteMixin
from .recovery import RecoveryMixin
from .repair import RepairMixin


class ReplicaStore(ReadWriteMixin, RepairMixin, InspectionMixin, RecoveryMixin, BaseStore):
    """Three-copy oriented, file-backed coordinator for synthetic drills.

    All persistent state is under ``root``:

    * ``nodes/<node>/objects/...`` contains synthetic block bytes;
    * ``coordinator/state.json`` is the transactionally replaced state;
    * ``coordinator/journal.jsonl`` is an append-only operation archive.
    """

    def __init__(self, root: str | Path, default_nodes: tuple[str, ...] = ("a", "b", "c")):
        super().__init__(root, default_nodes=default_nodes)

    # Short, discovery-friendly aliases.
    read = ReadWriteMixin.read_object
    write_block = ReadWriteMixin.stage_block
    claim = RepairMixin.claim_repair
    verify = RepairMixin.verify_repair

    def write(self, key: str, data: Any, block_size: int | None = None, **kwargs: Any):
        """Write one object; str/bytes is one block unless block_size is set."""
        if isinstance(data, (str, bytes)):
            raw = data.encode("utf-8") if isinstance(data, str) else data
            if block_size:
                blocks = [raw[offset : offset + block_size] for offset in range(0, len(raw), block_size)]
            else:
                blocks = [raw]
        else:
            blocks = data
        return self.write_object(key, blocks, **kwargs)
