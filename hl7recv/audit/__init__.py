"""Local data-quality audit for confirmed warehoused result facts.

The audit compares two caller-supplied half-open time windows of the
*committed* fact stream (``messages`` / ``results`` only -- rejected or
unconfirmed inbox records are never facts) and emits a deterministic JSON
report describing data shape and pipeline quality.  It never interprets the
clinical meaning of result values.

Public surface:

* :class:`Policy` / :func:`load_policy` -- versioned JSON strategy files
* :class:`AuditError`                 -- invalid policy / unrunnable audit
* :func:`run_audit`                   -- snapshot + metrics + report
* :data:`EXIT_OK, EXIT_WARNING, EXIT_FAILURE, EXIT_INCONCLUSIVE, EXIT_USAGE`

See ``docs/AUDIT.md`` for the policy format and report contract.
"""

from .engine import (
    EXIT_FAILURE,
    EXIT_INCONCLUSIVE,
    EXIT_OK,
    EXIT_USAGE,
    EXIT_WARNING,
    AuditError,
    run_audit,
)
from .policy import MetricSpec, Policy, Window, load_policy, load_policy_text

__all__ = [
    "AuditError",
    "EXIT_FAILURE",
    "EXIT_INCONCLUSIVE",
    "EXIT_OK",
    "EXIT_USAGE",
    "EXIT_WARNING",
    "MetricSpec",
    "Policy",
    "Window",
    "load_policy",
    "load_policy_text",
    "run_audit",
]
