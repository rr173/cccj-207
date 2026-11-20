"""Top-level handover-ledger service: register, settle, decide, extend.

All four scriptable entry points are here.  Every mutating operation ends in
exactly one committed transaction that publishes a complete version; a
failure rolls back and leaves no partial version.  The receiver database is
only ever opened read-only and is never written, locked or reconfigured.

Versioning contract
-------------------
* ``register_and_settle`` publishes v1 from a fresh fact batch;
* a decision request must name the version the caller saw; a stale base is
  rejected before any change;
* re-settling after decisions reuses the *same* fact batch, so facts written
  to the receiver later never leak in; only an explicit ``extend_cutoff``
  reads a fresh batch;
* decisions that have become invalid against a batch reject publication with
  the reason; the same request may withdraw the now-invalid decisions and
  settle again.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from . import manifest as mani
from . import vocab
from .decisions import (
    ActiveDecisions,
    append_decisions,
    check_version_current,
    materialise_active,
    validate_request_shape,
)
from .engine import settle
from .errors import (
    DecisionRejectedError,
    LedgerError,
    ManifestError,
    StaleVersionError,
)
from .intake import ingest_facts
from .store import BatchInserter, open_ledger, open_receiver_readonly
from .timeutil import instant_to_iso, parse_instant


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
def _stream_manifest(conn, ledger_id: str, manifest_dir: str,
                     meta: mani.ManifestMeta) -> tuple[int, int, int]:
    """Validate cross-references/uniqueness and stream the manifest in.

    Returns counts (work_orders, items, mappings).  Runs inside the caller's
    transaction; any ManifestError aborts before a ledger row exists.
    """
    sender_set = meta.sender_set
    # work orders
    wo_ids: set[str] = set()
    wo_ins = BatchInserter(
        conn,
        "INSERT INTO work_orders(ledger_id, work_order_id, patient_domain,"
        " patient_id) VALUES (?,?,?,?)",
    )
    for wo in mani.iter_work_orders(manifest_dir):
        wid = wo["work_order_id"]
        if wid in wo_ids:
            raise ManifestError(f"duplicate work_order_id {wid!r}")
        wo_ids.add(wid)
        wo_ins.add((ledger_id, wid, wo["patient_domain"], wo["patient_id"]))
    wo_ins.flush()
    if not wo_ids:
        raise ManifestError("manifest declares no work orders")

    # items: stable globally unique ids, references must resolve
    item_ids: set[str] = set()
    item_ins = BatchInserter(
        conn,
        "INSERT INTO items(ledger_id, item_id, work_order_id, code_system,"
        " code, sub_id, due_at_us) VALUES (?,?,?,?,?,?,?)",
    )
    n_items = 0
    for it in mani.iter_items(manifest_dir):
        if it["item_id"] in item_ids:
            raise ManifestError(f"duplicate item_id {it['item_id']!r}")
        if it["work_order_id"] not in wo_ids:
            raise ManifestError(
                f"item {it['item_id']!r} references unknown work order"
                f" {it['work_order_id']!r}"
            )
        item_ids.add(it["item_id"])
        item_ins.add((
            ledger_id, it["item_id"], it["work_order_id"], it["code_system"],
            it["code"], it["sub_id"], it["due_at"],
        ))
        n_items += 1
    item_ins.flush()
    if not item_ids:
        raise ManifestError("manifest declares no receivable items")

    # mappings: sender participation, work-order references, sane windows
    map_ins = BatchInserter(
        conn,
        "INSERT INTO mappings(ledger_id, sender, number_class, local_number,"
        " work_order_id, valid_from_us, valid_to_us) VALUES (?,?,?,?,?,?,?)",
    )
    n_maps = 0
    for mp in mani.iter_mappings(manifest_dir):
        if mp["sender"] not in sender_set:
            raise ManifestError(
                f"mapping sender {mp['sender']!r} is not among participating"
                " senders"
            )
        if mp["work_order_id"] not in wo_ids:
            raise ManifestError(
                f"mapping references unknown work order {mp['work_order_id']!r}"
            )
        if mp["valid_to"] <= mp["valid_from"]:
            raise ManifestError("mapping window must be half-open [from, to)")
        map_ins.add((
            ledger_id, mp["sender"], mp["number_class"], mp["local_number"],
            mp["work_order_id"], mp["valid_from"], mp["valid_to"],
        ))
        n_maps += 1
    map_ins.flush()
    return len(wo_ids), n_items, n_maps


def register_and_settle(
    ledger_path: str, receiver_path: str, manifest_dir: str,
    *, cutoff: str | None = None, ledger_id: str | None = None,
) -> dict:
    """Validate a manifest, create the ledger, ingest facts and publish v1.

    ``cutoff`` overrides the manifest's optional cut-off; either way it must
    be strictly later than the shift start.  On any validation failure no
    ledger database content is committed (the whole operation is one
    transaction around an empty/new ledger).
    """
    meta = mani.load_meta(manifest_dir)
    cutoff_us = meta.cutoff
    if cutoff is not None:
        cutoff_us = parse_instant(cutoff, field="cutoff")
    if cutoff_us is None:
        raise ManifestError(
            "a cut-off is required to settle; provide it in the manifest or"
            " via the cutoff argument"
        )
    if cutoff_us <= meta.shift_start:
        raise ManifestError("cutoff must be strictly later than shift_start")

    lid = ledger_id or meta.shift_id
    conn = open_ledger(ledger_path)
    try:
        if conn.execute(
            "SELECT 1 FROM ledgers WHERE ledger_id=?", (lid,)
        ).fetchone():
            raise LedgerError(f"ledger {lid!r} already exists in {ledger_path}")
        now = _now_iso()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO ledgers(ledger_id, shift_id, shift_start_us, created_at)"
            " VALUES (?,?,?,?)",
            (lid, meta.shift_id, meta.shift_start, now),
        )
        conn.executemany(
            "INSERT INTO senders(ledger_id, sender) VALUES (?,?)",
            [(lid, s) for s in meta.senders],
        )
        n_wo, n_items, n_maps = _stream_manifest(conn, lid, manifest_dir, meta)

        recv = open_receiver_readonly(receiver_path)
        try:
            batch_id = ingest_facts(
                conn, recv, senders=meta.senders,
                shift_start_us=meta.shift_start, cutoff_us=cutoff_us,
                created_at_iso=now,
            )
        finally:
            recv.rollback()
            recv.close()

        active = ActiveDecisions({}, {}, {})
        version_id = settle(
            conn, ledger_id=lid, batch_id=batch_id, cutoff_us=cutoff_us,
            kind="initial", base_version=None, created_at_iso=now, active=active,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "ledger_id": lid, "version_id": version_id, "batch_id": batch_id,
        "work_orders": n_wo, "items": n_items, "mappings": n_maps,
        "cutoff": instant_to_iso(cutoff_us),
    }


# ---------------------------------------------------------------------------
# Manual decisions
# ---------------------------------------------------------------------------
def _check_action_targets(conn, ledger_id: str, batch_id: int,
                          actions: list[dict]) -> None:
    """Targeted legality checks beyond what settlement re-validation covers."""
    for i, act in enumerate(actions):
        kind = act["action"]
        if kind in (vocab.DEC_ASSIGN_GROUP, vocab.DEC_WITHDRAW_GROUP):
            gk = act.get("group_key")
            if not isinstance(gk, str) or not gk:
                raise DecisionRejectedError(f"actions[{i}]: group_key required")
            exists = conn.execute(
                "SELECT 1 FROM fact_groups WHERE batch_id=? AND group_key=?",
                (batch_id, gk),
            ).fetchone()
            if not exists:
                raise DecisionRejectedError(
                    f"actions[{i}]: unknown group {gk!r} in this fact batch"
                )
            if kind == vocab.DEC_ASSIGN_GROUP:
                wid = act.get("work_order_id")
                if not isinstance(wid, str) or not wid:
                    raise DecisionRejectedError(
                        f"actions[{i}]: work_order_id required for assignment")
                if not conn.execute(
                    "SELECT 1 FROM work_orders WHERE ledger_id=? AND work_order_id=?",
                    (ledger_id, wid),
                ).fetchone():
                    raise DecisionRejectedError(
                        f"actions[{i}]: unknown work order {wid!r}")
        if kind in (vocab.DEC_LOCK_OBS, vocab.DEC_WITHDRAW_LOCK):
            ok = act.get("obs_key")
            if not isinstance(ok, str) or not ok:
                raise DecisionRejectedError(f"actions[{i}]: obs_key required")
            if kind == vocab.DEC_LOCK_OBS:
                iid = act.get("item_id")
                if not isinstance(iid, str) or not iid:
                    raise DecisionRejectedError(
                        f"actions[{i}]: item_id required to lock an observation")
                if not conn.execute(
                    "SELECT 1 FROM fact_observations WHERE batch_id=? AND obs_key=?",
                    (batch_id, ok),
                ).fetchone():
                    raise DecisionRejectedError(
                        f"actions[{i}]: observation {ok!r} not in fact batch")
                if not conn.execute(
                    "SELECT 1 FROM items WHERE ledger_id=? AND item_id=?",
                    (ledger_id, iid),
                ).fetchone():
                    raise DecisionRejectedError(
                        f"actions[{i}]: item {iid!r} does not exist")


def _head_batch_and_cutoff(conn, ledger_id: str) -> tuple[int, int, int]:
    row = conn.execute(
        "SELECT v.version_id, v.batch_id, v.cutoff_us FROM versions v"
        " JOIN ledgers l ON l.ledger_id=v.ledger_id"
        " WHERE l.ledger_id=? ORDER BY v.version_id DESC LIMIT 1",
        (ledger_id,),
    ).fetchone()
    if row is None:
        raise LedgerError(f"ledger {ledger_id!r} has no published version")
    return int(row["version_id"]), int(row["batch_id"]), int(row["cutoff_us"])


def submit_decisions(ledger_path: str, request: dict) -> dict:
    """Validate and apply a decision request, then re-settle the same batch.

    The request combines group attribution and observation locks freely.
    Stale ``base_version``, illegal edges, one observation settling two
    items, double-locking one item, or a lock orphaned by a group withdrawal
    reject the *whole* request; no new version is created.
    """
    actions = validate_request_shape(request)
    lid = request["ledger_id"]
    base_version = request["base_version"]
    conn = open_ledger(ledger_path)
    try:
        if not conn.execute("SELECT 1 FROM ledgers WHERE ledger_id=?",
                            (lid,)).fetchone():
            raise LedgerError(f"unknown ledger {lid!r}")
        check_version_current(conn, lid, base_version)
        head_v, batch_id, cutoff_us = _head_batch_and_cutoff(conn, lid)

        conn.execute("BEGIN IMMEDIATE")
        _check_action_targets(conn, lid, batch_id, actions)
        now = _now_iso()
        append_decisions(
            conn, ledger_id=lid, actions=actions,
            requested_by=request["requested_by"], base_version=base_version,
            created_at_iso=now,
        )
        active = materialise_active(conn, lid)
        try:
            version_id = settle(
                conn, ledger_id=lid, batch_id=batch_id, cutoff_us=cutoff_us,
                kind="decision", base_version=base_version,
                created_at_iso=now, active=active,
            )
        except DecisionRejectedError as exc:
            # Surface a structured reason so the caller can withdraw invalid
            # decisions in a follow-up (or the same logical request retried).
            raise DecisionRejectedError(
                f"decision set invalid against current facts: {exc}"
            ) from exc
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ledger_id": lid, "version_id": version_id, "batch_id": batch_id,
            "base_version": base_version}


def extend_cutoff(
    ledger_path: str, receiver_path: str, *, ledger_id: str,
    base_version: int, new_cutoff: str, requested_by: str,
    withdrawals: list[dict] | None = None,
) -> dict:
    """Read a fresh fact batch up to a later cut-off and re-settle.

    Only this operation changes the fact input.  Previously effective manual
    decisions are carried over; if one has become invalid against the new
    batch, publication is refused with the reason unless the same request
    withdraws the now-invalid decision(s) via ``withdrawals``.  Old versions
    and their per-item evidence stay queryable.
    """
    new_cutoff_us = parse_instant(new_cutoff, field="new_cutoff")
    conn = open_ledger(ledger_path)
    try:
        row = conn.execute(
            "SELECT shift_start_us, head_version FROM ledgers WHERE ledger_id=?",
            (ledger_id,),
        ).fetchone()
        if row is None:
            raise LedgerError(f"unknown ledger {ledger_id!r}")
        shift_start_us = int(row["shift_start_us"])
        if new_cutoff_us <= shift_start_us:
            raise ManifestError("new cutoff must be later than shift start")
        check_version_current(conn, ledger_id, base_version)
        head_v, old_batch_id, old_cutoff_us = _head_batch_and_cutoff(conn, ledger_id)
        if new_cutoff_us <= old_cutoff_us:
            raise ManifestError(
                "new cutoff must be strictly later than the current cutoff"
            )
        senders = tuple(r["sender"] for r in conn.execute(
            "SELECT sender FROM senders WHERE ledger_id=? ORDER BY sender",
            (ledger_id,),
        ).fetchall())

        conn.execute("BEGIN IMMEDIATE")
        now = _now_iso()
        # Optional withdrawals of decisions invalidated by the new facts.
        if withdrawals:
            for w in withdrawals:
                if not isinstance(w, dict) or "action" not in w:
                    raise DecisionRejectedError("invalid withdrawal entry")
                w.setdefault("reason", "withdrawn at cutoff extension")
            validate_request_shape({
                "ledger_id": ledger_id, "base_version": base_version,
                "requested_by": requested_by, "actions": withdrawals,
            })
            append_decisions(
                conn, ledger_id=ledger_id, actions=withdrawals,
                requested_by=requested_by, base_version=base_version,
                created_at_iso=now,
            )

        recv = open_receiver_readonly(receiver_path)
        try:
            batch_id = ingest_facts(
                conn, recv, senders=senders, shift_start_us=shift_start_us,
                cutoff_us=new_cutoff_us, created_at_iso=now,
            )
        finally:
            recv.rollback()
            recv.close()

        # Validate withdrawal targets against the new batch explicitly.
        if withdrawals:
            _check_action_targets(conn, ledger_id, batch_id, withdrawals)

        active = materialise_active(conn, ledger_id)
        version_id = settle(
            conn, ledger_id=ledger_id, batch_id=batch_id,
            cutoff_us=new_cutoff_us, kind="extend", base_version=base_version,
            created_at_iso=now, active=active,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ledger_id": ledger_id, "version_id": version_id,
            "batch_id": batch_id, "base_version": base_version,
            "cutoff": instant_to_iso(new_cutoff_us)}
