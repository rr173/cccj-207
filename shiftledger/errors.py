"""Error types for the shift handover ledger.

Each error carries a stable machine-readable ``code`` so callers can branch
without parsing message text.  The status/reason token vocabulary lives in
:mod:`shiftledger.vocab`.
"""

from __future__ import annotations


class LedgerError(Exception):
    """Base class; carries a stable machine-readable ``code``."""

    code = "ledger_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class ManifestError(LedgerError):
    """The handover manifest is invalid; no ledger may be created."""

    code = "invalid_manifest"


class StaleVersionError(LedgerError):
    """The request was based on a version that is no longer the head."""

    code = "stale_version"


class DecisionRejectedError(LedgerError):
    """A decision request contains an illegal edge / conflict.

    The whole request is rejected and no partial version is left behind.
    """

    code = "decision_rejected"
