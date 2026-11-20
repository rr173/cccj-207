"""Analytics-side replica of the result subscription (docs/SYNC.md §4).

The replica is a standalone SQLite database.  It applies page files
produced by the publisher and exposes the landed facts plus the current
cursor (last applied position).  Guarantees:

* a page is applied in ONE transaction -- readers opening the database
  see either the state before the page or after it, never half a page
  of facts and never a cursor without its facts;
* a new page is applied only when its start equals the current cursor,
  so pages tile the commit order without gaps or overlaps;
* re-importing a page that was already fully applied is a side-effect
  free no-op (verified against the recorded page digest); a stale file
  covering the same progress with different bytes is rejected, not
  treated as a replay;
* two processes racing different valid pages from the same cursor are
  serialised by the write transaction: at most one advances the cursor.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Callable, Iterable, Iterator

from . import protocol

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sub_messages (
  sender      TEXT NOT NULL,
  control_id  TEXT NOT NULL,
  received_at TEXT NOT NULL,
  position    INTEGER NOT NULL,
  PRIMARY KEY (sender, control_id)
);
CREATE INDEX IF NOT EXISTS idx_sub_messages_position
  ON sub_messages (position);
CREATE TABLE IF NOT EXISTS sub_results (
  id          INTEGER PRIMARY KEY,
  sender      TEXT NOT NULL,
  control_id  TEXT NOT NULL,
  obr_index   INTEGER NOT NULL,
  obx_index   INTEGER NOT NULL,
  obs_system  TEXT NOT NULL,
  obs_code    TEXT NOT NULL,
  sub_id      TEXT,
  value_type  TEXT NOT NULL,
  value_text  TEXT NOT NULL,
  units       TEXT,
  FOREIGN KEY (sender, control_id)
    REFERENCES sub_messages(sender, control_id)
);
CREATE INDEX IF NOT EXISTS idx_sub_results_message
  ON sub_results (sender, control_id);
CREATE TABLE IF NOT EXISTS applied_pages (
  start  INTEGER NOT NULL,
  end    INTEGER NOT NULL,
  digest TEXT NOT NULL,
  PRIMARY KEY (start, end)
);
"""


class ReplicaError(Exception):
    """Base class for replica-side failures."""


class IdentityMismatch(ReplicaError):
    """Page source/subscription does not match this replica."""


class CursorMismatch(ReplicaError):
    """Page does not start at the current cursor (gap or straddle)."""


class StalePageConflict(ReplicaError):
    """Covers already-applied progress but is not the recorded page."""


def _connect(path: str, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = "file:" + os.path.abspath(path) + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_replica(db_path: str, *, source_id: str, senders: Iterable[str]) -> dict:
    """Create (or verify) a replica bound to one source and subscription."""
    subscription = protocol.normalize_subscription(senders)
    sub_id = protocol.subscription_id(subscription)
    conn = _connect(db_path)
    try:
        conn.executescript(_SCHEMA)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='source_id'"
        ).fetchone()
        if row is not None:
            state = get_position_conn(conn)
            if state["source_id"] != source_id or state["subscription_id"] != sub_id:
                raise IdentityMismatch(
                    "replica is bound to a different source or subscription"
                )
            conn.commit()
            return {"status": "already", **state}
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('protocol', ?)",
            (protocol.PROTOCOL,),
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('source_id', ?)", (source_id,)
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('subscription_id', ?)", (sub_id,)
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('subscription_senders', ?)",
            (protocol.canonical_bytes(subscription).decode("utf-8"),),
        )
        conn.execute("INSERT INTO meta(key, value) VALUES ('cursor', '0')")
        conn.commit()
        return {
            "status": "initialized",
            "source_id": source_id,
            "subscription_id": sub_id,
            "cursor": 0,
        }
    finally:
        conn.close()


def get_position_conn(conn: sqlite3.Connection) -> dict:
    def _get(key: str) -> str | None:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    try:
        source = _get("source_id")
    except sqlite3.OperationalError as exc:
        raise ReplicaError(f"replica is not initialized: {exc}") from exc
    if source is None:
        raise ReplicaError("replica is not initialized")
    return {
        "protocol": _get("protocol"),
        "source_id": source,
        "subscription_id": _get("subscription_id"),
        "cursor": int(_get("cursor")),
    }


def get_position(db_path: str) -> dict:
    """Current cursor and identities (read-only)."""
    try:
        conn = _connect(db_path, readonly=True)
    except sqlite3.OperationalError as exc:
        raise ReplicaError(f"cannot open replica: {exc}") from exc
    try:
        return get_position_conn(conn)
    finally:
        conn.close()


def import_page(
    db_path: str,
    data: bytes,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> dict:
    """Validate and apply one page file; see module docstring for rules.

    Returns ``{"status": "applied" | "duplicate", ...}``.  Raises
    PageError (corrupt file), IdentityMismatch, CursorMismatch or
    StalePageConflict; a rejected page never moves the cursor and never
    lands any fact.
    """
    page = protocol.parse_page(data)  # raises PageError on corruption
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        state = get_position_conn(conn)
        if page["source_id"] != state["source_id"]:
            raise IdentityMismatch("page is from a different source")
        if page["subscription_id"] != state["subscription_id"]:
            raise IdentityMismatch("page is for a different subscription")
        cursor = state["cursor"]

        # an already-recorded page decides replay vs stale lookalike first
        row = conn.execute(
            "SELECT digest FROM applied_pages WHERE start=? AND end=?",
            (page["start"], page["end"]),
        ).fetchone()
        if row is not None:
            conn.rollback()
            if row[0] == page["digest"]:
                return {"status": "duplicate", "cursor": cursor, "messages": 0}
            raise StalePageConflict(
                f"page ({page['start']},{page['end']}] covers already-applied"
                " progress but does not match the recorded page"
            )

        if page["start"] == cursor:
            msg_rows = [
                (msg["sender"], msg["control_id"], msg["received_at"],
                 msg["position"])
                for msg in page["messages"]
            ]
            res_rows = [
                (msg["sender"], msg["control_id"], r["obr_index"],
                 r["obx_index"], r["obs_system"], r["obs_code"],
                 r["sub_id"], r["value_type"], r["value_text"], r["units"])
                for msg in page["messages"] for r in msg["results"]
            ]
            conn.executemany(
                "INSERT INTO sub_messages(sender, control_id, received_at,"
                " position) VALUES (?,?,?,?)",
                msg_rows,
            )
            conn.executemany(
                "INSERT INTO sub_results(sender, control_id, obr_index,"
                " obx_index, obs_system, obs_code, sub_id, value_type,"
                " value_text, units) VALUES (?,?,?,?,?,?,?,?,?,?)",
                res_rows,
            )
            conn.execute(
                "INSERT INTO applied_pages(start, end, digest) VALUES (?,?,?)",
                (page["start"], page["end"], page["digest"]),
            )
            conn.execute(
                "UPDATE meta SET value=? WHERE key='cursor'", (str(page["end"]),)
            )
            if fault_hook is not None:
                fault_hook("import_before_commit")
            conn.commit()
            if fault_hook is not None:
                fault_hook("import_after_commit")
            return {"status": "applied", "cursor": page["end"],
                    "messages": len(page["messages"])}

        if page["end"] <= cursor:
            raise StalePageConflict(
                f"page ({page['start']},{page['end']}] covers already-applied"
                " progress but is not the recorded page"
            )

        raise CursorMismatch(
            f"page starts at {page['start']} but replica cursor is {cursor}"
        )
    finally:
        conn.close()


def iter_facts(db_path: str) -> Iterator[dict]:
    """Yield landed messages with their results, in commit order."""
    try:
        conn = _connect(db_path, readonly=True)
    except sqlite3.OperationalError as exc:
        raise ReplicaError(f"cannot open replica: {exc}") from exc
    try:
        msgs = conn.execute(
            "SELECT sender, control_id, received_at, position"
            " FROM sub_messages ORDER BY position"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        conn.close()
        raise ReplicaError(f"replica is not initialized: {exc}") from exc
    try:
        for m in msgs:
            rows = conn.execute(
                "SELECT obr_index, obx_index, obs_system, obs_code, sub_id,"
                " value_type, value_text, units FROM sub_results"
                " WHERE sender=? AND control_id=?"
                " ORDER BY obr_index, obx_index",
                (m["sender"], m["control_id"]),
            ).fetchall()
            yield {
                "position": m["position"],
                "sender": m["sender"],
                "control_id": m["control_id"],
                "received_at": m["received_at"],
                "results": [dict(r) for r in rows],
            }
    finally:
        conn.close()
