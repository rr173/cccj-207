"""Fact intake: copy one bounded read of the receiver into an immutable batch.

The receiver database is opened **read-only** and never written, locked or
reconfigured.  Only the settled ``messages`` and ``results`` tables are read
-- never inbox, raw payloads or audit reports.  The window is
``[shift_start, cutoff)`` on ``messages.received_at`` (instant comparison),
restricted to the manifest's participating senders.

Rows are streamed in stable keyset order (message rowid, result id) and
inserted in bounded batches.  The copied snapshot means a later settle that
reuses this batch is bit-for-bit independent of facts arriving afterwards;
only an explicit cut-off extension reads a fresh batch.
"""

from __future__ import annotations

import hashlib
import sqlite3

from .errors import ManifestError
from .store import BatchInserter

_BATCH = 1000
_PAGE = 1000

PLACER = "placer"
FILLER = "filler"


def make_group_key(sender: str, control_id: str, obr_index: int) -> str:
    return f"{sender}\x1f{control_id}\x1f{obr_index}"


def make_obs_key(group_key: str, obx_index: int) -> str:
    return f"{group_key}\x1f{obx_index}"


def _fingerprint(conn: sqlite3.Connection, table: str, columns: tuple[str, ...],
                 order_col: str) -> str:
    """Streaming SHA-256 over a table in stable order (bounded memory)."""
    digest = hashlib.sha256()
    col_list = ", ".join(columns)
    last = 0
    while True:
        rows = conn.execute(
            f"SELECT {col_list} FROM {table} WHERE {order_col} > ?"
            f" ORDER BY {order_col} LIMIT ?",
            (last, _BATCH),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            for col in columns:
                cell = row[col]
                if cell is None:
                    digest.update(b"\x00")
                else:
                    data = str(cell).encode("utf-8")
                    digest.update(len(data).to_bytes(4, "big"))
                    digest.update(data)
        last = rows[-1][order_col]
    return digest.hexdigest()


def _parse_received_us(text: str) -> int:
    # Receiver timestamps are canonical UTC ISO strings written by the
    # product; parse via the same strict rule to get comparable integers.
    from .timeutil import parse_instant
    return parse_instant(text, field="messages.received_at")


def ingest_facts(
    ledger_conn: sqlite3.Connection,
    recv_conn: sqlite3.Connection,
    *,
    senders: tuple[str, ...],
    shift_start_us: int,
    cutoff_us: int,
    created_at_iso: str,
) -> int:
    """Stream facts in ``[shift_start, cutoff)`` into a new batch.

    Returns the new ``batch_id``.  The receiver connection must already be
    inside a read transaction pinning one snapshot.
    """
    if cutoff_us <= shift_start_us:
        raise ManifestError("cutoff must be strictly later than shift_start")

    sender_ph = ",".join("?" for _ in senders)
    # One keyset-paginated join scan in stable (message, result) order.  The
    # window is tested on *parsed* instants (never on original strings); we
    # carry only a bounded current-group state as rows stream past.
    join_sql = (
        "SELECT m.rowid AS msg_rowid, r.id AS result_id, m.sender AS sender,"
        "       m.control_id AS control_id, m.received_at AS received_at,"
        "       r.obr_index AS obr_index, r.obx_index AS obx_index,"
        "       r.patient_authority AS patient_authority,"
        "       r.patient_id AS patient_id, r.placer_order AS placer_order,"
        "       r.filler_order AS filler_order, r.obs_code AS obs_code,"
        "       r.obs_system AS obs_system, r.sub_id AS sub_id"
        " FROM messages m JOIN results r"
        "   ON m.sender = r.sender AND m.control_id = r.control_id"
        f" WHERE m.sender IN ({sender_ph})"
    )
    params = (*senders,)

    # Snapshot descriptor (whole-table fingerprints, like the audit feature).
    recv_msg_max = int(
        recv_conn.execute("SELECT COALESCE(MAX(rowid),0) FROM messages").fetchone()[0]
    )
    recv_res_max = int(
        recv_conn.execute("SELECT COALESCE(MAX(id),0) FROM results").fetchone()[0]
    )
    msg_sha = _fingerprint(
        recv_conn, "messages",
        ("rowid", "sender", "control_id", "received_at", "status"), "rowid",
    )
    res_sha = _fingerprint(
        recv_conn, "results",
        ("id", "sender", "control_id", "obr_index", "obx_index",
         "patient_authority", "patient_id", "placer_order", "filler_order",
         "obs_code", "obs_system", "sub_id"),
        "id",
    )

    cur = ledger_conn.execute(
        "INSERT INTO fact_batches(created_at, cutoff_us, shift_start_us,"
        " recv_message_max, recv_result_max, recv_messages_sha, recv_results_sha,"
        " n_groups, n_observations) VALUES (?,?,?,?,?,?,?,0,0)",
        (created_at_iso, cutoff_us, shift_start_us,
         recv_msg_max, recv_res_max, msg_sha, res_sha),
    )
    batch_id = int(cur.lastrowid)

    ins_group = BatchInserter(
        ledger_conn,
        "INSERT INTO fact_groups(batch_id, group_key, sender, control_id,"
        " obr_index, received_at_us, patient_domain, patient_id)"
        " VALUES (?,?,?,?,?,?,?,?)",
    )
    ins_order = BatchInserter(
        ledger_conn,
        "INSERT INTO fact_group_orders(batch_id, group_key, number_class, local_number)"
        " VALUES (?,?,?,?)",
    )
    ins_obs = BatchInserter(
        ledger_conn,
        "INSERT INTO fact_observations(batch_id, obs_key, group_key, obx_index,"
        " code_system, code, sub_id) VALUES (?,?,?,?,?,?,?)",
    )

    n_groups = 0
    n_obs = 0
    last_result_id = 0
    # Current-group carry state (bounded: only the group being streamed).
    cur_key = None
    cur_state: dict | None = None

    def flush_group(state):
        nonlocal n_groups
        if state is None:
            return
        ins_group.add((
            batch_id, state["group_key"], state["sender"], state["control_id"],
            state["obr_index"], state["received_us"],
            state["patient_domain"], state["patient_id"],
        ))
        n_groups += 1
        if state["placer"]:
            ins_order.add((batch_id, state["group_key"], PLACER, state["placer"]))
        if state["filler"]:
            ins_order.add((batch_id, state["group_key"], FILLER, state["filler"]))

    while True:
        # Keyset pagination on the monotonic result id alone; the join order
        # (m.rowid, r.id) still yields stable grouping within a page.
        rows = recv_conn.execute(
            join_sql + " AND r.id > ? ORDER BY r.id LIMIT ?",
            (*params, last_result_id, _PAGE),
        ).fetchall()
        if not rows:
            break
        for r in rows:
            last_result_id = r["result_id"]
            received_us = _parse_received_us(r["received_at"])
            if not (shift_start_us <= received_us < cutoff_us):
                continue
            gk = make_group_key(r["sender"], r["control_id"], int(r["obr_index"]))
            if gk != cur_key:
                flush_group(cur_state)
                p_domain = r["patient_authority"]
                p_id = r["patient_id"]
                if not p_domain or not p_id:
                    p_domain, p_id = None, None
                cur_state = {
                    "group_key": gk, "sender": r["sender"],
                    "control_id": r["control_id"], "obr_index": int(r["obr_index"]),
                    "received_us": received_us,
                    "patient_domain": p_domain, "patient_id": p_id,
                    "placer": r["placer_order"], "filler": r["filler_order"],
                }
                cur_key = gk
            obs_key = make_obs_key(gk, int(r["obx_index"]))
            ins_obs.add((
                batch_id, obs_key, gk, int(r["obx_index"]),
                r["obs_system"], r["obs_code"], r["sub_id"],
            ))
            n_obs += 1
    flush_group(cur_state)

    ins_group.flush()
    ins_order.flush()
    ins_obs.flush()
    ledger_conn.execute(
        "UPDATE fact_batches SET n_groups=?, n_observations=? WHERE batch_id=?",
        (n_groups, n_obs, batch_id),
    )
    return batch_id
