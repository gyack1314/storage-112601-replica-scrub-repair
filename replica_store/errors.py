"""Errors raised by the local replica coordinator."""


class ReplicaStoreError(Exception):
    """Base error for invalid commands or an inconsistent local store."""


class ManifestConflict(ReplicaStoreError):
    """A revision already exists with a different write generation/manifest."""


class GenerationFenceError(ReplicaStoreError):
    """A commit or repair confirmation used the wrong generation."""


class BlockUnavailable(ReplicaStoreError):
    """One or more blocks cannot currently be proved against their manifest."""

    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report or {}


class RepairRejected(ReplicaStoreError):
    """A repair claim/confirmation cannot be accepted."""

    def __init__(self, message, decision=None):
        super().__init__(message)
        self.decision = decision or {}


class BlockUnderRepair(ReplicaStoreError):
    """The exact local replica slot is fenced by an active repair."""
