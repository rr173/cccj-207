"""Read-only views over published ledger versions.

Everything here opens the ledger database read-only and never mutates it.
Lists are keyset-paginated so callers can consume a large ledger item by
item or in batches without loading the whole version.  Each view returns
plain JSON-serialisable dicts with stable ordering and the exact evidence
behind every write-off (the adopted observation and its group's arrival
instant).
"""

from __future__ import annotations

import json
import sqlite3

from .errors import LedgerError
from .store import open_ledger
from .timeutil import instant_to_iso

_PAGE = 500


def _open_ro(path: str) -> sqlite3.Connection:
    # A plain connection with query_only still rejects every write, while
    # mode=ro cannot create the -wal/-shm sidecars of a WAL database.
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _resolve_version(conn, ledger_id: str, version: int | None) -> int:
    if version is None:
        row = conn.execute(
            "SELECT head_version FROM ledgers WHERE ledger_id=?", (ledger_id,)
        ).fetchone()
        if row is None:
            raise LedgerError(f"unknown ledger {ledger_id!r}")
        return int(row[0])
    exists = conn.execute(
        "SELECT 1 FROM versions WHERE ledger_id=? AND version_id=?",
        (ledger_id, version),
    ).fetchone()
    if not exists:
        raise LedgerError(f"ledger {ledger_id!r} has no version {version}")
    return version


def list_versions(ledger_path: str, ledger_id: str) -> list[dict]:
    conn = _open_ro(ledger_path)
    try:
        rows = conn.execute(
            "SELECT version_id, batch_id, cutoff_us, kind, base_version,"
            " created_at, summary FROM versions WHERE ledger_id=?"
            " ORDER BY version_id",
            (ledger_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["cutoff"] = instant_to_iso(d.pop("cutoff_us"))
            d["summary"] = json.loads(d["summary"])
            out.append(d)
        return out
    finally:
        conn.close()


def version_overview(ledger_path: str, ledger_id: str,
                     version: int | None = None) -> dict:
    """Header + counts for one version (the batch it was computed on)."""
    conn = _open_ro(ledger_path)
    try:
        vid = _resolve_version(conn, ledger_id, version)
        v = conn.execute(
            "SELECT * FROM versions WHERE ledger_id=? AND version_id=?",
            (ledger_id, vid),
        ).fetchone()
        batch = conn.execute(
            "SELECT * FROM fact_batches WHERE batch_id=?", (v["batch_id"],)
        ).fetchone()
        return {
            "ledger_id": ledger_id,
            "version_id": vid,
            "kind": v["kind"],
            "base_version": v["base_version"],
            "created_at": v["created_at"],
            "cutoff": instant_to_iso(v["cutoff_us"]),
            "summary": json.loads(v["summary"]),
            "fact_batch": {
                "batch_id": batch["batch_id"],
                "cutoff": instant_to_iso(batch["cutoff_us"]),
                "n_groups": batch["n_groups"],
                "n_observations": batch["n_observations"],
                "recv_message_max": batch["recv_message_max"],
                "recv_result_max": batch["recv_result_max"],
                "recv_messages_sha256": batch["recv_messages_sha"],
                "recv_results_sha256": batch["recv_results_sha"],
            },
        }
    finally:
        conn.close()


def iter_items(ledger_path: str, ledger_id: str, version: int | None = None,
               *, status: str | None = None, page: int = _PAGE):
    """Yield receivable items of a version in stable item_id order."""
    conn = _open_ro(ledger_path)
    try:
        vid = _resolve_version(conn, ledger_id, version)
        last = ""
        while True:
            sql = (
                "SELECT vi.*, i.code_system, i.code, i.sub_id, i.due_at_us,"
                "       fg.received_at_us AS group_received"
                " FROM version_items vi"
                " JOIN items i ON i.ledger_id=? AND i.item_id=vi.item_id"
                " LEFT JOIN fact_observations fo"
                "   ON fo.batch_id=(SELECT batch_id FROM versions"
                "      WHERE version_id=?) AND fo.obs_key=vi.obs_key"
                " LEFT JOIN fact_groups fg"
                "   ON fg.batch_id=fo.batch_id AND fg.group_key=fo.group_key"
                " WHERE vi.version_id=? AND vi.item_id > ?")
            params: list = [ledger_id, vid, vid, last]
            if status is not None:
                sql += " AND vi.status=?"
                params.append(status)
            sql += " ORDER BY vi.item_id LIMIT ?"
            params.append(page)
            rows = conn.execute(sql, params).fetchall()
            if not rows:
                return
            for r in rows:
                d = dict(r)
                d["due_at"] = instant_to_iso(d.pop("due_at_us"))
                if d.get("received_at_us") is not None:
                    d["received_at"] = instant_to_iso(d.pop("received_at_us"))
                else:
                    d.pop("received_at_us", None)
                if d.get("group_received") is not None:
                    d["group_received_at"] = instant_to_iso(
                        d.pop("group_received"))
                else:
                    d.pop("group_received", None)
                if d.get("block_reasons"):
                    d["block_reasons"] = json.loads(d["block_reasons"])
                yield d
            last = rows[-1]["item_id"]
    finally:
        conn.close()


def item_detail(ledger_path: str, ledger_id: str, item_id: str,
                version: int | None = None) -> dict:
    """One item with every candidate/potential edge and the adopted evidence."""
    conn = _open_ro(ledger_path)
    try:
        vid = _resolve_version(conn, ledger_id, version)
        vi = conn.execute(
            "SELECT * FROM version_items WHERE version_id=? AND item_id=?",
            (vid, item_id),
        ).fetchone()
        if vi is None:
            raise LedgerError(f"no item {item_id!r} in version {vid}")
        edges = conn.execute(
            "SELECT group_key, obs_key, edge_kind FROM version_edges"
            " WHERE version_id=? AND item_id=? ORDER BY edge_kind, obs_key",
            (vid, item_id),
        ).fetchall()
        d = dict(vi)
        if d.get("block_reasons"):
            d["block_reasons"] = json.loads(d["block_reasons"])
        if d.get("received_at_us") is not None:
            d["received_at"] = instant_to_iso(d.pop("received_at_us"))
        d["edges"] = [dict(e) for e in edges]
        return d
    finally:
        conn.close()


def iter_observations(ledger_path: str, ledger_id: str,
                      version: int | None = None, *,
                      disposition: str | None = None, page: int = _PAGE):
    """Yield every included observation with its single current disposition."""
    conn = _open_ro(ledger_path)
    try:
        vid = _resolve_version(conn, ledger_id, version)
        last = ""
        while True:
            sql = ("SELECT * FROM version_obs WHERE version_id=?"
                   " AND obs_key > ?")
            params: list = [vid, last]
            if disposition is not None:
                sql += " AND disposition=?"
                params.append(disposition)
            sql += " ORDER BY obs_key LIMIT ?"
            params.append(page)
            rows = conn.execute(sql, params).fetchall()
            if not rows:
                return
            for r in rows:
                d = dict(r)
                d["detail"] = json.loads(d["detail"])
                yield d
            last = rows[-1]["obs_key"]
    finally:
        conn.close()


def iter_groups(ledger_path: str, ledger_id: str, version: int | None = None,
                *, status: str | None = None, page: int = _PAGE):
    """Yield group attributions of a version with per-number mapping detail."""
    conn = _open_ro(ledger_path)
    try:
        vid = _resolve_version(conn, ledger_id, version)
        last = ""
        while True:
            sql = ("SELECT * FROM version_groups WHERE version_id=?"
                   " AND group_key > ?")
            params: list = [vid, last]
            if status is not None:
                sql += " AND status=?"
                params.append(status)
            sql += " ORDER BY group_key LIMIT ?"
            params.append(page)
            rows = conn.execute(sql, params).fetchall()
            if not rows:
                return
            for r in rows:
                d = dict(r)
                d["detail"] = json.loads(d["detail"])
                d["manually_decided"] = bool(d.pop("decided"))
                yield d
            last = rows[-1]["group_key"]
    finally:
        conn.close()


def list_decisions(ledger_path: str, ledger_id: str) -> list[dict]:
    """The full append-only manual decision log (stable records + reasons)."""
    conn = _open_ro(ledger_path)
    try:
        rows = conn.execute(
            "SELECT decision_id, action, group_key, work_order_id, obs_key,"
            " item_id, reason, requested_by, created_at, base_version"
            " FROM decisions WHERE ledger_id=? ORDER BY decision_id",
            (ledger_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
