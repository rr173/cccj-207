"""Publisher side of the result subscription (docs/SYNC.md).

* ``upgrade``    -- one explicit OFFLINE migration that adds publishing
                    metadata to an existing receiver database and gives
                    every historical message its durable commit-order
                    position in the documented fixed order
                    (``messages.rowid`` ascending).  The whole upgrade is
                    a single SQLite transaction: an interrupted run
                    exposes nothing, and a re-run never renumbers
                    positions that were already published.
* ``watermark``  -- read the current publish upper bound.  Callers fetch
                    pages up to a bound they obtained earlier; this
                    command only *reports*, it never moves anything.
* ``fetch_page`` -- emit one deterministic page file covering commits in
                    ``(start, bound]``, scanning at most ``budget``
                    committed messages (matching or not).  The same
                    (source, subscription, start, bound, budget) always
                    yields byte-identical output, no matter what the
                    receiver has committed since, across restarts and
                    ``VACUUM``.

Only committed ``messages``/``results`` rows plus the ``sync_positions``
metadata are read -- never the inbox attempt log, never ACK state.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from typing import Callable, Iterable

from ..store import _SCHEMA
from . import protocol

# positions are backfilled in chunks so an interrupted upgrade has a
# well-defined halfway state (which the single transaction then drops)
_BACKFILL_CHUNK = 1024


class SyncNotEnabled(Exception):
    """The receiver database has not been upgraded for publishing."""


class BoundError(ValueError):
    """Requested page window is invalid or reaches into the future."""


def _connect(path: str, *, readonly: bool) -> sqlite3.Connection:
    if readonly:
        uri = "file:" + os.path.abspath(path) + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _sync_meta(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT value FROM meta WHERE key='sync_enabled'"
    ).fetchone()
    if row is None or row[0] != "1":
        return None
    source = conn.execute(
        "SELECT value FROM meta WHERE key='sync_source_id'"
    ).fetchone()[0]
    nxt = int(
        conn.execute(
            "SELECT value FROM meta WHERE key='sync_next_seq'"
        ).fetchone()[0]
    )
    return {"source_id": source, "next_seq": nxt, "bound": nxt - 1}


def _verify_published(conn: sqlite3.Connection) -> dict:
    """Consistency check for an already-enabled database."""
    meta = _sync_meta(conn)
    n_msg = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    n_pos = conn.execute("SELECT COUNT(*) FROM sync_positions").fetchone()[0]
    if n_msg != n_pos:
        raise RuntimeError(
            f"sync metadata corrupt: {n_msg} messages but {n_pos} positions"
        )
    orphan = conn.execute(
        "SELECT COUNT(*) FROM messages m WHERE NOT EXISTS "
        "(SELECT 1 FROM sync_positions sp WHERE sp.sender=m.sender"
        " AND sp.control_id=m.control_id)"
    ).fetchone()[0]
    if orphan:
        raise RuntimeError(f"sync metadata corrupt: {orphan} messages lack positions")
    max_seq = conn.execute(
        "SELECT COALESCE(MAX(seq),0) FROM sync_positions"
    ).fetchone()[0]
    if max_seq + 1 != meta["next_seq"]:
        raise RuntimeError(
            f"sync metadata corrupt: max seq {max_seq} but next {meta['next_seq']}"
        )
    gap = conn.execute(
        "SELECT COUNT(*) FROM (SELECT seq FROM sync_positions"
        " WHERE seq BETWEEN 1 AND ?) WHERE 1=1",
        (max_seq,),
    ).fetchone()[0]
    if gap != max_seq:
        raise RuntimeError("sync metadata corrupt: positions are not dense")
    return meta


def upgrade(
    db_path: str,
    *,
    source_id: str | None = None,
    fault_hook: Callable[[int], None] | None = None,
) -> dict:
    """Offline upgrade: enable publishing and backfill positions.

    Idempotent: a database that is already enabled is verified and
    reported (``status: "already"``) without touching any published
    position.  ``fault_hook`` is a drill-only callback invoked after each
    backfilled chunk with the number of rows assigned so far; it may
    raise or ``os._exit`` to simulate an interrupted upgrade.
    """
    conn = _connect(db_path, readonly=False)
    try:
        # bootstrap the receiver schema on a brand-new path so upgrading an
        # empty database is equivalent to upgrading a populated old one
        conn.executescript(_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT value FROM meta WHERE key='sync_enabled'"
        ).fetchone()
        if existing is not None and existing[0] == "1":
            meta = _verify_published(conn)
            conn.commit()
            return {"status": "already", **meta}

        conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_positions ("
            " seq INTEGER PRIMARY KEY,"
            " sender TEXT NOT NULL,"
            " control_id TEXT NOT NULL,"
            " UNIQUE (sender, control_id))"
        )
        # old databases predate this access-path index; it changes no facts
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_message"
            " ON results (sender, control_id)"
        )
        sid = source_id or uuid.uuid4().hex
        seq = 0
        last_rowid = 0
        while True:
            rows = conn.execute(
                "SELECT rowid, sender, control_id FROM messages"
                " WHERE rowid > ? ORDER BY rowid LIMIT ?",
                (last_rowid, _BACKFILL_CHUNK),
            ).fetchall()
            if not rows:
                break
            conn.executemany(
                "INSERT INTO sync_positions(seq, sender, control_id)"
                " VALUES (?,?,?)",
                [(seq + i + 1, row["sender"], row["control_id"])
                 for i, row in enumerate(rows)],
            )
            seq += len(rows)
            last_rowid = rows[-1]["rowid"]
            if fault_hook is not None:
                fault_hook(seq)
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('sync_enabled', '1')"
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('sync_source_id', ?)", (sid,)
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('sync_next_seq', ?)",
            (str(seq + 1),),
        )
        conn.commit()
        return {"status": "upgraded", "source_id": sid,
                "next_seq": seq + 1, "bound": seq}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def watermark(db_path: str, senders: Iterable[str]) -> dict:
    """Report the current publish upper bound for a subscription."""
    conn = _connect(db_path, readonly=True)
    try:
        meta = _sync_meta(conn)
        if meta is None:
            raise SyncNotEnabled(
                "receiver database has not been upgraded for sync"
            )
        return {
            "protocol": protocol.PROTOCOL,
            "source_id": meta["source_id"],
            "subscription_id": protocol.subscription_id(senders),
            "bound": meta["bound"],
        }
    finally:
        conn.close()


def fetch_page(
    db_path: str,
    senders: Iterable[str],
    *,
    start: int,
    bound: int,
    budget: int,
) -> bytes:
    """Fetch one deterministic page covering commits in (start, bound].

    At most ``budget`` committed messages are scanned, matching or not;
    the page ends at the last scanned position (``bound`` when the scan
    reaches it).  The bound is used exactly as given -- it is never
    silently replaced by a newer watermark, and a bound beyond the
    published watermark is rejected instead of fetching future commits.
    """
    subscription = protocol.normalize_subscription(senders)
    if start < 0:
        raise BoundError("start must be >= 0")
    if bound < start:
        raise BoundError("bound must be >= start")
    if budget < 1:
        raise BoundError("budget must be >= 1")

    conn = _connect(db_path, readonly=True)
    try:
        meta = _sync_meta(conn)
        if meta is None:
            raise SyncNotEnabled(
                "receiver database has not been upgraded for sync"
            )
        if bound > meta["bound"]:
            raise BoundError(
                f"bound {bound} exceeds the published watermark {meta['bound']}"
            )
        # Bounded range scan over the durable commit order: only commits
        # in (start, bound] are touched, never a rescan from position 0,
        # and the read transaction ends when this call returns.
        scanned = conn.execute(
            "SELECT sp.seq AS seq, m.sender AS sender, m.control_id AS control_id,"
            " m.received_at AS received_at"
            " FROM sync_positions sp"
            " JOIN messages m ON m.sender=sp.sender AND m.control_id=sp.control_id"
            " WHERE sp.seq > ? AND sp.seq <= ?"
            " ORDER BY sp.seq LIMIT ?",
            (start, bound, budget),
        ).fetchall()
        end = scanned[-1]["seq"] if scanned else start
        wanted = set(subscription)
        messages = []
        for row in scanned:
            if row["sender"] not in wanted:
                continue  # scanned but not subscribed: advances the page only
            results = conn.execute(
                "SELECT obr_index, obx_index, obs_system, obs_code, sub_id,"
                " value_type, value_text, units FROM results"
                " WHERE sender=? AND control_id=?"
                " ORDER BY obr_index, obx_index",
                (row["sender"], row["control_id"]),
            ).fetchall()
            messages.append(
                {
                    "position": row["seq"],
                    "sender": row["sender"],
                    "control_id": row["control_id"],
                    "received_at": row["received_at"],
                    "results": [
                        {
                            "obr_index": r["obr_index"],
                            "obx_index": r["obx_index"],
                            "obs_system": r["obs_system"],
                            "obs_code": r["obs_code"],
                            "sub_id": r["sub_id"],
                            "value_type": r["value_type"],
                            "value_text": r["value_text"],
                            "units": r["units"],
                        }
                        for r in results
                    ],
                }
            )
        return protocol.build_page(
            source_id=meta["source_id"],
            subscription=subscription,
            bound=bound,
            start=start,
            end=end,
            budget=budget,
            terminal=end >= bound,
            messages=messages,
        )
    finally:
        conn.close()
