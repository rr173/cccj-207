"""Manual decision management: validation, active-set materialisation.

Decisions are an append-only durable log.  A request may combine group
attribution and observation locks in one transaction; the whole request is
rejected when any edge is illegal.  The caller names the version it looked
at, so a stale request can never overwrite another operator's choice.

Active sets derived from the log at a given point:

* ``active_group_assignment[group_key] = work_order_id``
* ``active_locks[obs_key] = item_id``

Withdrawals remove the entry.  Every action (including withdrawal) keeps its
own stable record with reason, author and base version.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .errors import DecisionRejectedError, StaleVersionError
from . import vocab


@dataclass(frozen=True)
class ActiveDecisions:
    group_assignment: dict[str, str]
    locks: dict[str, str]          # obs_key -> item_id
    lock_items: dict[str, str]     # item_id -> obs_key (inverse)


def head_version(conn: sqlite3.Connection, ledger_id: str) -> int:
    row = conn.execute(
        "SELECT head_version FROM ledgers WHERE ledger_id=?", (ledger_id,)
    ).fetchone()
    return int(row[0])


def materialise_active(conn: sqlite3.Connection, ledger_id: str) -> ActiveDecisions:
    """Fold the append-only decision log into the currently effective sets."""
    assigns: dict[str, str] = {}
    locks: dict[str, str] = {}
    rows = conn.execute(
        "SELECT action, group_key, work_order_id, obs_key, item_id"
        " FROM decisions WHERE ledger_id=? ORDER BY decision_id",
        (ledger_id,),
    ).fetchall()
    for r in rows:
        if r["action"] == vocab.DEC_ASSIGN_GROUP:
            assigns[r["group_key"]] = r["work_order_id"]
        elif r["action"] == vocab.DEC_WITHDRAW_GROUP:
            assigns.pop(r["group_key"], None)
        elif r["action"] == vocab.DEC_LOCK_OBS:
            locks[r["obs_key"]] = r["item_id"]
        elif r["action"] == vocab.DEC_WITHDRAW_LOCK:
            locks.pop(r["obs_key"], None)
    lock_items = {item: obs for obs, item in locks.items()}
    return ActiveDecisions(assigns, locks, lock_items)


def validate_request_shape(request: dict) -> list[dict]:
    """Validate the envelope and return the action list (possibly empty)."""
    if not isinstance(request, dict):
        raise DecisionRejectedError("decision request must be a JSON object")
    for key in ("ledger_id", "base_version", "requested_by", "actions"):
        if key not in request:
            raise DecisionRejectedError(f"decision request missing field {key!r}")
    if not isinstance(request["base_version"], int) or request["base_version"] < 1:
        raise DecisionRejectedError("base_version must be a positive integer version")
    if not isinstance(request["requested_by"], str) or not request["requested_by"]:
        raise DecisionRejectedError("requested_by must be a non-empty string")
    actions = request["actions"]
    if not isinstance(actions, list) or not actions:
        raise DecisionRejectedError("actions must be a non-empty list")
    valid_actions = {
        vocab.DEC_ASSIGN_GROUP, vocab.DEC_WITHDRAW_GROUP,
        vocab.DEC_LOCK_OBS, vocab.DEC_WITHDRAW_LOCK,
    }
    for i, act in enumerate(actions):
        if not isinstance(act, dict) or act.get("action") not in valid_actions:
            raise DecisionRejectedError(f"actions[{i}]: invalid action")
        reason = act.get("reason")
        if not isinstance(reason, str) or not reason:
            raise DecisionRejectedError(f"actions[{i}]: reason must be non-empty")
    return actions


def check_version_current(
    conn: sqlite3.Connection, ledger_id: str, base_version: int
) -> None:
    current = head_version(conn, ledger_id)
    if base_version != current:
        raise StaleVersionError(
            f"base_version={base_version} is stale; current head is v{current}",
        )


def append_decisions(
    conn: sqlite3.Connection,
    *,
    ledger_id: str,
    actions: list[dict],
    requested_by: str,
    base_version: int,
    created_at_iso: str,
) -> None:
    """Append every action row (call within the publishing transaction)."""
    rows = []
    for act in actions:
        rows.append((
            ledger_id, act["action"],
            act.get("group_key"), act.get("work_order_id"),
            act.get("obs_key"), act.get("item_id"),
            act["reason"], requested_by, created_at_iso, base_version,
        ))
    conn.executemany(
        "INSERT INTO decisions(ledger_id, action, group_key, work_order_id,"
        " obs_key, item_id, reason, requested_by, created_at, base_version)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
