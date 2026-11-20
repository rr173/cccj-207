"""Settlement engine: attribution first, then receivable-item write-off.

Pipeline for one settle on one fact batch::

  1. evaluate every group's attribution (identity + order mappings, strict);
  2. build matching edges per attributed work order (exact system/code/sub id,
     where a null sub id is an explicit null-to-null condition);
  3. validate and apply manual decisions (group assignment, observation lock);
  4. auto write-off: among what remains, only unique 1:1 relations;
     ambiguous groups' *potential* matches block auto write-off but never
     settle anything themselves;
  5. classify every item and give every included observation exactly one
     disposition;
  6. publish one complete, immutable version atomically.

Memory model
------------
Manual decisions are human-scale and stay in small Python dicts.  All
potentially large intermediate sets (verdicts, candidate/potential edges,
adoption state, dispositions) live in **connection-level SQLite temporary
tables** with covering indexes; every scan is keyset-paginated and the
matching fixed point is solved one work order at a time.  The full fact set,
the whole candidate graph and a full version are never materialised in
Python memory.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from . import vocab
from .errors import DecisionRejectedError
from .store import BatchInserter

_PAGE = 1000


class _InvalidDecision(Exception):
    """Raised internally when a previously made decision no longer holds."""


# ---------------------------------------------------------------------------
# Group attribution
# ---------------------------------------------------------------------------
@dataclass
class GroupVerdict:
    group_key: str
    status: str
    work_order_id: str | None = None
    decided: bool = False
    detail: dict = field(default_factory=dict)


class AttributionContext:
    """Per-settlement caches for attribution lookups.

    Group count can reach hundreds of thousands but the *distinct* order
    numbers and work orders are far fewer.  Work-order patient identity is
    loaded once (human/manifest scale), and per-number mapping rows are
    cached after their first read; mapping effectiveness still varies with
    the arrival instant, so the time test stays per group.
    """

    def __init__(self, conn: sqlite3.Connection, ledger_id: str):
        self.conn = conn
        self.ledger_id = ledger_id
        self.wo_patient = {
            r["work_order_id"]: (r["patient_domain"], r["patient_id"])
            for r in conn.execute(
                "SELECT work_order_id, patient_domain, patient_id"
                " FROM work_orders WHERE ledger_id=?", (ledger_id,))
        }
        # (sender, number_class, local_number) -> list of (wo, from, to)
        self._map_cache: dict[tuple, list[tuple]] = {}

    def mapping_rows(self, sender: str, nclass: str,
                     lnum: str) -> list[tuple]:
        key = (sender, nclass, lnum)
        cached = self._map_cache.get(key)
        if cached is None:
            cached = [
                (r["work_order_id"], int(r["valid_from_us"]),
                 int(r["valid_to_us"]))
                for r in self.conn.execute(
                    "SELECT work_order_id, valid_from_us, valid_to_us"
                    " FROM mappings WHERE ledger_id=? AND sender=?"
                    " AND number_class=? AND local_number=?",
                    (self.ledger_id, sender, nclass, lnum))
            ]
            self._map_cache[key] = cached
        return cached

    def evaluate_group(
        self, grow: sqlite3.Row, active_assign: dict[str, str]
    ) -> GroupVerdict:
        conn = self.conn
        ledger_id = self.ledger_id
        gk = grow["group_key"]
        sender = grow["sender"]
        received_us = int(grow["received_at_us"])
        detail: dict = {"numbers": []}

        # 1) patient identity present?
        if not grow["patient_domain"] or not grow["patient_id"]:
            return GroupVerdict(gk, vocab.GRP_NO_PATIENT, detail=detail)

        # 2) any order number at all?
        order_rows = conn.execute(
            "SELECT number_class, local_number FROM fact_group_orders"
            " WHERE batch_id=? AND group_key=? ORDER BY number_class",
            (grow["batch_id"], gk),
        ).fetchall()
        if not order_rows:
            return GroupVerdict(gk, vocab.GRP_NO_ORDER, detail=detail)

        # 3) per-number effective mappings; a provided number without a
        #    mapping can never be back-stopped by the other number.
        per_number_sets: list[tuple[str, str, set[str], str]] = []
        for orow in order_rows:
            nclass, lnum = orow["number_class"], orow["local_number"]
            all_rows = self.mapping_rows(sender, nclass, lnum)
            wos = {wo for wo, vfrom, vto in all_rows
                   if vfrom <= received_us < vto}
            if not wos:
                reason = (vocab.NUM_NOT_EFFECTIVE if all_rows
                          else vocab.NUM_NO_MAPPING)
                detail["numbers"].append(
                    {"number_class": nclass, "local_number": lnum,
                     "reason": reason, "work_orders": []})
                per_number_sets.append((nclass, lnum, set(), reason))
            else:
                detail["numbers"].append(
                    {"number_class": nclass, "local_number": lnum,
                     "reason": vocab.NUM_MAPPED, "work_orders": sorted(wos)})
                per_number_sets.append((nclass, lnum, wos, vocab.NUM_MAPPED))

        if any(not wos for _, _, wos, _ in per_number_sets):
            bad = [d for d in detail["numbers"]
                   if d["reason"] != vocab.NUM_MAPPED]
            if all(d["reason"] == vocab.NUM_NOT_EFFECTIVE for d in bad) and \
                    not any(d["reason"] == vocab.NUM_NO_MAPPING for d in bad):
                return GroupVerdict(gk, vocab.GRP_OUT_OF_WINDOW, detail=detail)
            return GroupVerdict(gk, vocab.GRP_UNREGISTERED_ORDER, detail=detail)

        # 4) intersection of all provided numbers' effective work-order sets
        candidates = set(per_number_sets[0][2])
        for _, _, wos, _ in per_number_sets[1:]:
            candidates &= wos
        if not candidates:
            detail["conflict_sets"] = [
                {"number_class": nc, "work_orders": sorted(wos)}
                for nc, _, wos, _ in per_number_sets
            ]
            return GroupVerdict(gk, vocab.GRP_MAPPING_CONFLICT, detail=detail)

        # 5) strict patient filter against every candidate work order
        group_patient = (grow["patient_domain"], grow["patient_id"])
        matching = {w for w in candidates
                    if self.wo_patient.get(w) == group_patient}
        cand_list = sorted(candidates)
        if not matching:
            detail["candidate_work_orders"] = cand_list
            detail["group_patient"] = list(group_patient)
            return GroupVerdict(gk, vocab.GRP_PATIENT_MISMATCH, detail=detail)

        manual = active_assign.get(gk)
        if manual is not None:
            # A surviving manual decision must still be legal in this batch.
            if manual not in matching:
                detail["manual_work_order"] = manual
                detail["legal_work_orders"] = sorted(matching)
                raise _InvalidDecision(
                    f"manual assignment of group {gk} to {manual} is no longer"
                    " a legal candidate in this fact batch"
                )
            detail["candidate_work_orders"] = sorted(matching)
            return GroupVerdict(gk, vocab.GRP_ASSIGNED, manual, True, detail)

        if len(matching) == 1:
            (only,) = matching
            detail["candidate_work_orders"] = [only]
            return GroupVerdict(gk, vocab.GRP_ASSIGNED, only, False, detail)

        detail["candidate_work_orders"] = sorted(matching)
        return GroupVerdict(gk, vocab.GRP_AMBIGUOUS, None, False, detail)


# ---------------------------------------------------------------------------
# Scratch temp tables (one settlement, dropped at the end)
# ---------------------------------------------------------------------------
_SCRATCH_SQL = """
CREATE TEMP TABLE s_verdicts (
  group_key TEXT PRIMARY KEY, status TEXT NOT NULL,
  work_order_id TEXT, decided INTEGER NOT NULL, detail TEXT NOT NULL
);
CREATE TEMP TABLE s_cand (
  work_order_id TEXT NOT NULL, obs_key TEXT NOT NULL, item_id TEXT NOT NULL,
  group_key TEXT NOT NULL
);
CREATE INDEX idx_s_cand_obs ON s_cand(obs_key);
CREATE INDEX idx_s_cand_item ON s_cand(item_id);
CREATE INDEX idx_s_cand_wo ON s_cand(work_order_id);
CREATE TEMP TABLE s_pot (item_id TEXT NOT NULL, obs_key TEXT NOT NULL,
  group_key TEXT NOT NULL);
CREATE INDEX idx_s_pot_item ON s_pot(item_id);
CREATE TEMP TABLE s_adopt (
  obs_key TEXT PRIMARY KEY, item_id TEXT NOT NULL, edge_kind TEXT NOT NULL
);
CREATE INDEX idx_s_adopt_item ON s_adopt(item_id);
"""


def _iter_groups(conn, batch_id: int):
    last = ""
    while True:
        rows = conn.execute(
            "SELECT * FROM fact_groups WHERE batch_id=? AND group_key > ?"
            " ORDER BY group_key LIMIT ?",
            (batch_id, last, _PAGE),
        ).fetchall()
        if not rows:
            return
        for r in rows:
            yield r
        last = rows[-1]["group_key"]


def _drop_scratch(conn) -> None:
    for name in ("s_verdicts", "s_cand", "s_pot", "s_adopt",
                 "s_adopt_full", "s_pot_block"):
        conn.execute(f"DROP TABLE IF EXISTS temp.{name}")


# ---------------------------------------------------------------------------
# Full settlement (temp-table based, per-work-order streaming)
# ---------------------------------------------------------------------------
def _build_scratch(conn) -> None:
    conn.executescript(_SCRATCH_SQL)


def _write_verdicts(conn, batch_id: int, ledger_id: str,
                    active_assign: dict[str, str]) -> None:
    """Phase 1: evaluate every group; stream rows into s_verdicts."""
    ctx = AttributionContext(conn, ledger_id)
    ins = BatchInserter(
        conn,
        "INSERT INTO s_verdicts(group_key, status, work_order_id, decided, detail)"
        " VALUES (?,?,?,?,?)",
    )
    try:
        for grow in _iter_groups(conn, batch_id):
            v = ctx.evaluate_group(grow, active_assign)
            ins.add((
                v.group_key, v.status, v.work_order_id,
                1 if v.decided else 0, json.dumps(v.detail, sort_keys=True),
            ))
        ins.flush()
    except _InvalidDecision as exc:
        ins.flush()
        raise DecisionRejectedError(str(exc)) from exc


def _iter_verdicts(conn):
    last = ""
    while True:
        rows = conn.execute(
            "SELECT group_key, status, work_order_id, detail, decided"
            " FROM s_verdicts WHERE group_key > ? ORDER BY group_key LIMIT ?",
            (last, _PAGE),
        ).fetchall()
        if not rows:
            return
        for r in rows:
            yield r
        last = rows[-1]["group_key"]


def _build_edges(conn, batch_id: int, ledger_id: str) -> None:
    """Phase 2: candidate edges (attributed) and potential edges (ambiguous).

    Fully set-based: each edge set is one INSERT ... SELECT joining
    observations to receivable items on exact (code_system, code) with a
    null-safe sub-id comparison (``IS``), so an explicit NULL sub id matches
    only a NULL item sub id.  No per-observation Python round trip.
    """
    # Candidate edges for uniquely/manually attributed groups.
    conn.execute(
        "INSERT INTO s_cand(work_order_id, obs_key, item_id, group_key)"
        " SELECT v.work_order_id, o.obs_key, i.item_id, o.group_key"
        " FROM s_verdicts v"
        " JOIN fact_observations o ON o.batch_id=? AND o.group_key=v.group_key"
        " JOIN items i ON i.ledger_id=?"
        "   AND i.work_order_id=v.work_order_id"
        "   AND i.code_system=o.code_system AND i.code=o.code"
        "   AND (i.sub_id IS o.sub_id)"
        " WHERE v.status=?",
        (batch_id, ledger_id, vocab.GRP_ASSIGNED),
    )
    # Potential edges for ambiguous groups: one row per candidate work order
    # the group could still belong to, expanded from the verdict detail.
    conn.execute(
        "INSERT INTO s_pot(item_id, obs_key, group_key)"
        " SELECT i.item_id, o.obs_key, o.group_key"
        " FROM s_verdicts v"
        " JOIN fact_observations o ON o.batch_id=? AND o.group_key=v.group_key"
        " JOIN json_each(json_extract(v.detail, '$.candidate_work_orders')) AS wo"
        " JOIN items i ON i.ledger_id=?"
        "   AND i.work_order_id=wo.value"
        "   AND i.code_system=o.code_system AND i.code=o.code"
        "   AND (i.sub_id IS o.sub_id)"
        " WHERE v.status=?",
        (batch_id, ledger_id, vocab.GRP_AMBIGUOUS),
    )


def _validate_and_apply_locks(
    conn, batch_id: int, ledger_id: str, active
) -> None:
    """Phase 3: every active lock must still be a legal exact match.

    Locks are human-scale; validate each with targeted queries rather than
    materialising the whole observation table.
    """
    if len(set(active.locks.values())) != len(active.locks):
        raise DecisionRejectedError("the same item is locked by two observations")
    for obs_key, item_id in active.locks.items():
        orow = conn.execute(
            "SELECT group_key FROM fact_observations WHERE batch_id=? AND obs_key=?",
            (batch_id, obs_key),
        ).fetchone()
        if orow is None:
            raise DecisionRejectedError(
                f"locked observation {obs_key} is not present in fact batch"
                f" {batch_id}"
            )
        gk = orow["group_key"]
        v = conn.execute(
            "SELECT status, work_order_id FROM s_verdicts WHERE group_key=?",
            (gk,),
        ).fetchone()
        if v is None or v["status"] != vocab.GRP_ASSIGNED:
            raise DecisionRejectedError(
                f"lock of {obs_key} -> {item_id}: its group has no valid"
                " attribution in this batch; withdraw the lock first"
            )
        irow = conn.execute(
            "SELECT work_order_id FROM items WHERE ledger_id=? AND item_id=?",
            (ledger_id, item_id),
        ).fetchone()
        if irow is None:
            raise DecisionRejectedError(f"locked item {item_id} does not exist")
        if irow["work_order_id"] != v["work_order_id"]:
            raise DecisionRejectedError(
                f"lock of {obs_key} -> {item_id}: item belongs to a different"
                " work order than the observation's attributed group"
            )
        edge = conn.execute(
            "SELECT 1 FROM s_cand WHERE obs_key=? AND item_id=? LIMIT 1",
            (obs_key, item_id),
        ).fetchone()
        if edge is None:
            raise DecisionRejectedError(
                f"lock of {obs_key} -> {item_id}: observation no longer matches"
                " the item's (code_system, code, sub_id) in this fact batch"
            )
        conn.execute(
            "INSERT INTO s_adopt(obs_key, item_id, edge_kind) VALUES (?,?,?)",
            (obs_key, item_id, vocab.EDGE_LOCK),
        )


# Insert *every* currently unique 1:1 still-free pair in one set-based
# statement.  Locked rows already live in s_adopt, so the NOT EXISTS clauses
# exclude them; items touched by an ambiguous group (s_pot) stay blocked
# unless locked.  Repeated until a fixed point, this is bipartite degree-one
# peeling and matches the "unique 1:1 only" rule exactly.
_PEEL_SQL = (
    "INSERT INTO s_adopt(obs_key, item_id, edge_kind)"
    " SELECT c.obs_key, c.item_id, ?"
    " FROM s_cand c"
    " WHERE NOT EXISTS (SELECT 1 FROM s_pot p WHERE p.item_id=c.item_id)"
    "   AND NOT EXISTS (SELECT 1 FROM s_adopt a WHERE a.obs_key=c.obs_key)"
    "   AND NOT EXISTS (SELECT 1 FROM s_adopt a WHERE a.item_id=c.item_id)"
    "   AND (SELECT COUNT(DISTINCT c2.item_id) FROM s_cand c2"
    "         WHERE c2.obs_key=c.obs_key"
    "           AND NOT EXISTS (SELECT 1 FROM s_adopt a"
    "                           WHERE a.item_id=c2.item_id)) = 1"
    "   AND (SELECT COUNT(DISTINCT c3.obs_key) FROM s_cand c3"
    "         WHERE c3.item_id=c.item_id"
    "           AND NOT EXISTS (SELECT 1 FROM s_adopt a"
    "                           WHERE a.obs_key=c3.obs_key)) = 1"
    " GROUP BY c.obs_key, c.item_id"
)


def _auto_settle_per_work_order(conn, ledger_id: str) -> None:
    """Phase 4: unique 1:1 auto write-off, potential edges block.

    Set-based degree-one peeling over the whole ledger, repeated to a fixed
    point.  Each iteration is one SQL statement; there is no per-work-order
    or per-row Python round trip.
    """
    while True:
        cur = conn.execute(_PEEL_SQL, (vocab.EDGE_AUTO,))
        if cur.rowcount == 0:
            break


# ---------------------------------------------------------------------------
# Classification + immutable version publishing
# ---------------------------------------------------------------------------
def _publish(
    conn, *, ledger_id: str, batch_id: int, cutoff_us: int, kind: str,
    base_version: int | None, created_at_iso: str,
) -> int:
    """Publish one complete version with set-based INSERT ... SELECT.

    No per-row Python round trips: every version table is populated by a
    single statement (or a handful), and counts come from GROUP BY.  This is
    what keeps the 200k/500k scale within the memory and time budgets.
    """
    cur = conn.execute(
        "INSERT INTO versions(ledger_id, batch_id, cutoff_us, kind, base_version,"
        " created_at, summary) VALUES (?,?,?,?,?,?,?)",
        (ledger_id, batch_id, cutoff_us, kind, base_version, created_at_iso, "{}"),
    )
    version_id = int(cur.lastrowid)

    # A joined view of every adoption with its group arrival instant.
    conn.execute(
        "CREATE TEMP TABLE s_adopt_full AS"
        " SELECT a.obs_key AS obs_key, a.item_id AS item_id, a.edge_kind AS edge_kind,"
        "        o.group_key AS group_key, g.received_at_us AS received_at_us"
        " FROM s_adopt a"
        " JOIN fact_observations o ON o.batch_id=? AND o.obs_key=a.obs_key"
        " JOIN fact_groups g ON g.batch_id=? AND g.group_key=o.group_key",
        (batch_id, batch_id),
    )
    conn.execute("CREATE INDEX idx_saf_item ON s_adopt_full(item_id)")
    conn.execute("CREATE INDEX idx_saf_obs ON s_adopt_full(obs_key)")

    # Pre-aggregate the per-item blocking reasons from ambiguous groups.
    conn.execute(
        "CREATE TEMP TABLE s_pot_block AS"
        " SELECT item_id, json_group_array(obs_key) AS obs_json"
        " FROM (SELECT DISTINCT item_id, obs_key FROM s_pot"
        "       ORDER BY item_id, obs_key) GROUP BY item_id"
    )

    # ---- version_groups ------------------------------------------------
    conn.execute(
        "INSERT INTO version_groups(version_id, group_key, status,"
        " work_order_id, detail, decided)"
        " SELECT ?, group_key, status, work_order_id, detail, decided"
        " FROM s_verdicts", (version_id,))

    # ---- version_edges: adopted, remaining candidates, potentials -------
    conn.execute(
        "INSERT INTO version_edges(version_id, group_key, obs_key, item_id, edge_kind)"
        " SELECT ?, f.group_key, f.obs_key, f.item_id, f.edge_kind"
        " FROM s_adopt_full f", (version_id,))
    conn.execute(
        "INSERT INTO version_edges(version_id, group_key, obs_key, item_id, edge_kind)"
        " SELECT ?, c.group_key, c.obs_key, c.item_id, ?"
        " FROM s_cand c"
        " WHERE NOT EXISTS (SELECT 1 FROM s_adopt a"
        "   WHERE a.obs_key=c.obs_key AND a.item_id=c.item_id)",
        (version_id, vocab.EDGE_CANDIDATE))
    conn.execute(
        "INSERT INTO version_edges(version_id, group_key, obs_key, item_id, edge_kind)"
        " SELECT ?, group_key, obs_key, item_id, ? FROM s_pot",
        (version_id, vocab.EDGE_POTENTIAL))

    # ---- version_items: one statement, full classification -------------
    conn.execute(
        "INSERT INTO version_items(version_id, item_id, work_order_id, status,"
        " edge_kind, obs_key, received_at_us, timeliness, block_reasons)"
        " SELECT ?, i.item_id, i.work_order_id,"
        "   CASE"
        "     WHEN f.obs_key IS NOT NULL AND f.edge_kind=?"
        "       THEN ?"
        "     WHEN f.obs_key IS NOT NULL THEN ?"
        "     WHEN EXISTS (SELECT 1 FROM s_cand c WHERE c.item_id=i.item_id"
        "            AND NOT EXISTS (SELECT 1 FROM s_adopt a"
        "              WHERE a.obs_key=c.obs_key))"
        "       OR EXISTS (SELECT 1 FROM s_pot_block b WHERE b.item_id=i.item_id)"
        "       THEN ?"
        "     WHEN ? <= i.due_at_us THEN ?"
        "     ELSE ?"
        "   END AS status,"
        "   f.edge_kind, f.obs_key, f.received_at_us,"
        "   CASE WHEN f.obs_key IS NULL THEN NULL"
        "        WHEN f.received_at_us <= i.due_at_us THEN ? ELSE ? END,"
        "   (SELECT json_array('potential_from_ambiguous_group:' ||"
        "           (SELECT group_concat(value, ',') FROM json_each(b.obs_json)))"
        "    FROM s_pot_block b WHERE b.item_id=i.item_id)"
        " FROM items i"
        " LEFT JOIN s_adopt_full f ON f.item_id=i.item_id"
        " WHERE i.ledger_id=?",
        (version_id, vocab.EDGE_LOCK, vocab.ITEM_LOCK_SETTLED,
         vocab.ITEM_AUTO_SETTLED, vocab.ITEM_PENDING_DECISION,
         cutoff_us, vocab.ITEM_PENDING_RECEIPT, vocab.ITEM_OVERDUE,
         vocab.SETTLED_ON_TIME, vocab.SETTLED_LATE, ledger_id))

    # ---- version_obs: exactly one disposition per observation -----------
    conn.execute(
        "INSERT INTO version_obs(version_id, obs_key, group_key, disposition,"
        " item_id, detail)"
        " SELECT ?, o.obs_key, o.group_key,"
        "   CASE"
        "     WHEN f.obs_key IS NOT NULL THEN ?"
        "     WHEN v.status=? THEN ?"
        "     WHEN v.status<>? THEN ?"
        "     WHEN NOT EXISTS (SELECT 1 FROM s_cand c WHERE c.obs_key=o.obs_key)"
        "       THEN ?"
        "     WHEN NOT EXISTS (SELECT 1 FROM s_cand c WHERE c.obs_key=o.obs_key"
        "            AND NOT EXISTS (SELECT 1 FROM s_adopt a"
        "              WHERE a.item_id=c.item_id))"
        "       THEN ?"
        "     ELSE ?"
        "   END AS disposition,"
        "   f.item_id,"
        "   CASE"
        "     WHEN f.obs_key IS NOT NULL THEN json_object('edge','adopted')"
        "     WHEN v.status=? THEN v.detail"
        "     WHEN v.status<>? THEN"
        "       json_object('group_status', v.status)"
        "     WHEN NOT EXISTS (SELECT 1 FROM s_cand c WHERE c.obs_key=o.obs_key)"
        "       THEN json_object('work_order_id', v.work_order_id)"
        "     WHEN NOT EXISTS (SELECT 1 FROM s_cand c WHERE c.obs_key=o.obs_key"
        "            AND NOT EXISTS (SELECT 1 FROM s_adopt a"
        "              WHERE a.item_id=c.item_id))"
        "       THEN json_object('work_order_id', v.work_order_id,"
        "            'matching_items', (SELECT json_group_array(c2.item_id)"
        "              FROM (SELECT DISTINCT item_id FROM s_cand"
        "                    WHERE obs_key=o.obs_key ORDER BY item_id) c2))"
        "     ELSE json_object('work_order_id', v.work_order_id,"
        "          'candidate_items', (SELECT json_group_array(c3.item_id)"
        "            FROM (SELECT DISTINCT c4.item_id FROM s_cand c4"
        "              WHERE c4.obs_key=o.obs_key"
        "                AND NOT EXISTS (SELECT 1 FROM s_adopt a"
        "                  WHERE a.item_id=c4.item_id)"
        "              ORDER BY c4.item_id) c3))"
        "   END AS detail"
        " FROM fact_observations o"
        " JOIN s_verdicts v ON v.group_key=o.group_key"
        " LEFT JOIN s_adopt_full f ON f.obs_key=o.obs_key"
        " WHERE o.batch_id=?",
        (version_id, vocab.OBS_ADOPTED,
         vocab.GRP_AMBIGUOUS, vocab.OBS_GROUP_PENDING,
         vocab.GRP_ASSIGNED, vocab.OBS_UNATTRIBUTABLE,
         vocab.OBS_NO_MATCHING_ITEM, vocab.OBS_ITEMS_OCCUPIED,
         vocab.OBS_CANDIDATE_UNASSIGNED,
         vocab.GRP_AMBIGUOUS, vocab.GRP_ASSIGNED, batch_id))

    # ---- counts via GROUP BY (no Python accumulation) -------------------
    item_counts = {r["status"]: r["n"] for r in conn.execute(
        "SELECT status, COUNT(*) n FROM version_items WHERE version_id=?"
        " GROUP BY status", (version_id,))}
    obs_counts = {r["disposition"]: r["n"] for r in conn.execute(
        "SELECT disposition, COUNT(*) n FROM version_obs WHERE version_id=?"
        " GROUP BY disposition", (version_id,))}
    counts = {
        "auto_settled": 0, "lock_settled": 0, "pending_decision": 0,
        "pending_receipt": 0, "overdue": 0,
    }
    counts.update(item_counts)
    obs_summary = {
        "adopted": 0, "group_pending": 0, "unattributable": 0,
        "candidate_unassigned": 0, "matching_items_occupied": 0,
        "no_matching_item": 0,
    }
    obs_summary.update(obs_counts)

    conn.execute("DROP TABLE s_adopt_full")
    conn.execute("DROP TABLE s_pot_block")

    summary = {
        "version_id": version_id, "kind": kind,
        "base_version": base_version, "cutoff_us": cutoff_us,
        "batch_id": batch_id, "item_counts": counts,
        "observation_counts": obs_summary,
    }
    conn.execute("UPDATE versions SET summary=? WHERE version_id=?",
                 (json.dumps(summary, sort_keys=True), version_id))
    conn.execute(
        "UPDATE ledgers SET head_version=? WHERE ledger_id=?",
        (version_id, ledger_id),
    )
    return version_id




def settle(
    conn, *, ledger_id: str, batch_id: int, cutoff_us: int, kind: str,
    base_version: int | None, created_at_iso: str, active,
) -> int:
    """Run the whole pipeline and publish one complete version.

    The caller owns the surrounding transaction (so registration and a
    decision request can each be atomic).  Any failure propagates; scratch
    temp tables are always dropped here, and the caller rolls back, so no
    half version can remain.
    """
    _drop_scratch(conn)
    try:
        _build_scratch(conn)
        _write_verdicts(conn, batch_id, ledger_id, active.group_assignment)
        _build_edges(conn, batch_id, ledger_id)
        _validate_and_apply_locks(conn, batch_id, ledger_id, active)
        _auto_settle_per_work_order(conn, ledger_id)
        return _publish(
            conn, ledger_id=ledger_id, batch_id=batch_id, cutoff_us=cutoff_us,
            kind=kind, base_version=base_version,
            created_at_iso=created_at_iso,
        )
    finally:
        _drop_scratch(conn)
