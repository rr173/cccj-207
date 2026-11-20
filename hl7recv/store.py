"""SQLite persistence for the HL7 receiver.

Commit/ACK ordering contract
----------------------------
A message's identity (sender, control id), its raw bytes, *all* parsed
result rows and its success state are persisted in **one** SQLite
transaction (``commit_message``).  The server sends AA only after that
transaction has been committed with ``synchronous=FULL``.  If the commit
fails or the process dies at the commit boundary, no AA is emitted and a
client resend is decided solely by the committed message identity --
never duplicated.

A small durable ``inbox`` log records each processing attempt in its own
autocommit write *before* parsing.  On startup, attempts still in state
``received`` never reached a commit boundary: they are marked
``recovered_incomplete`` (the client will resend).  Attempts committed but
not yet ACKed are marked ``ack_lost``; a resend then resolves against the
committed identity and gets AA without re-inserting.

Diagnostics and evidence are bounded: detail text and evidence bytes are
truncated to fixed limits.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Callable, Iterator

from .builder import ParsedMessage

SCHEMA_VERSION = "1"

MAX_DETAIL_CHARS = 1000
MAX_EVIDENCE_BYTES = 8192

# inbox / attempt states
ST_RECEIVED = "received"
ST_COMMITTED = "committed"
ST_ACKED = "acked"
ST_DUPLICATE = "duplicate"
ST_REJECTED = "rejected"
ST_CONFLICT = "conflict"
ST_ACK_LOST = "ack_lost"
ST_RECOVERED_INCOMPLETE = "recovered_incomplete"

# commit outcomes
OUT_COMMITTED = "committed"
OUT_DUPLICATE = "duplicate"
OUT_CONFLICT = "conflict"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  sender      TEXT NOT NULL,
  control_id  TEXT NOT NULL,
  version     TEXT NOT NULL,
  msg_type    TEXT NOT NULL,
  sent_at     TEXT,
  received_at TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  raw         BLOB NOT NULL,
  status      TEXT NOT NULL,
  PRIMARY KEY (sender, control_id)
);
CREATE TABLE IF NOT EXISTS patients (
  sender      TEXT NOT NULL,
  control_id  TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  authority   TEXT NOT NULL,
  identifier  TEXT NOT NULL,
  PRIMARY KEY (sender, control_id, seq),
  FOREIGN KEY (sender, control_id) REFERENCES messages(sender, control_id)
);
CREATE TABLE IF NOT EXISTS results (
  id            INTEGER PRIMARY KEY,
  sender        TEXT NOT NULL,
  control_id    TEXT NOT NULL,
  obr_index     INTEGER NOT NULL,
  obx_index     INTEGER NOT NULL,
  patient_authority TEXT,
  patient_id    TEXT,
  visit_id      TEXT,
  placer_order  TEXT,
  filler_order  TEXT,
  test_code     TEXT NOT NULL,
  test_system   TEXT NOT NULL,
  test_name     TEXT,
  obs_code      TEXT NOT NULL,
  obs_system    TEXT NOT NULL,
  obs_name      TEXT,
  sub_id        TEXT,
  value_type    TEXT NOT NULL,
  value_text    TEXT NOT NULL,
  units         TEXT,
  ref_range     TEXT,
  abnormal      TEXT,
  rstatus       TEXT NOT NULL,
  observed_at   TEXT,
  reported_at   TEXT,
  obr_seg       INTEGER NOT NULL,
  obx_seg       INTEGER NOT NULL,
  merged        INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (sender, control_id) REFERENCES messages(sender, control_id)
);
CREATE INDEX IF NOT EXISTS idx_results_patient
  ON results (patient_authority, patient_id);
CREATE INDEX IF NOT EXISTS idx_results_placer ON results (placer_order);
CREATE INDEX IF NOT EXISTS idx_results_filler ON results (filler_order);
-- supports the audit tool's half-open [start, end) window scans
CREATE INDEX IF NOT EXISTS idx_messages_received ON messages (received_at);
CREATE TABLE IF NOT EXISTS notes (
  id         INTEGER PRIMARY KEY,
  sender     TEXT NOT NULL,
  control_id TEXT NOT NULL,
  obr_index  INTEGER NOT NULL,
  obx_index  INTEGER,           -- NULL: note belongs to the OBR
  seg        INTEGER NOT NULL,  -- 1-based segment ordinal in the message
  seq        INTEGER NOT NULL,  -- ordinal among notes of the same scope
  text       TEXT NOT NULL,
  FOREIGN KEY (sender, control_id) REFERENCES messages(sender, control_id)
);
CREATE TABLE IF NOT EXISTS inbox (
  id          INTEGER PRIMARY KEY,
  conn_id     INTEGER NOT NULL,
  frame_no    INTEGER NOT NULL,
  received_at TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  sender      TEXT,
  control_id  TEXT,
  state       TEXT NOT NULL,
  detail      TEXT,
  evidence    BLOB
);
CREATE TABLE IF NOT EXISTS frame_errors (
  id      INTEGER PRIMARY KEY,
  conn_id INTEGER NOT NULL,
  offset  INTEGER NOT NULL,
  reason  TEXT NOT NULL,
  detail  TEXT,
  at      TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _bound_text(text: str | None) -> str | None:
    if text is None:
        return None
    if len(text) > MAX_DETAIL_CHARS:
        return text[:MAX_DETAIL_CHARS] + f"...[truncated {len(text) - MAX_DETAIL_CHARS} chars]"
    return text


def _bound_evidence(raw: bytes | None) -> bytes | None:
    if raw is None:
        return None
    if len(raw) > MAX_EVIDENCE_BYTES:
        return raw[:MAX_EVIDENCE_BYTES]
    return raw


class Store:
    """Thread-safe SQLite store.  One connection, one write lock.

    ``recover=True`` (the receiver's startup) reclassifies interrupted
    attempts; pure query clients should pass ``recover=False`` so opening
    the database never mutates diagnostic state under a running server.
    """

    def __init__(
        self,
        path: str,
        *,
        recover: bool = True,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self.path = path
        # Injectable clock: production keeps millisecond UTC; deterministic
        # drills (and the audit feature's boundary tests) pass a fixed clock
        # so message received_at values land on exact window boundaries.
        self._clock = clock or _utcnow
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('conn_counter', '0')"
            )
            self._conn.commit()
            self.recovered_ack_lost = 0
            self.recovered_incomplete = 0
            if recover:
                self._recover()

    # -- lifecycle -------------------------------------------------------
    def _recover(self) -> None:
        """Reclassify interrupted attempts after a crash/restart."""
        cur = self._conn.execute(
            "UPDATE inbox SET state=? WHERE state=?", (ST_ACK_LOST, ST_COMMITTED)
        )
        lost = cur.rowcount
        cur = self._conn.execute(
            "UPDATE inbox SET state=?, detail=COALESCE(detail,'') || ? WHERE state=?",
            (ST_RECOVERED_INCOMPLETE, " [marked at recovery]", ST_RECEIVED),
        )
        incomplete = cur.rowcount
        self._conn.commit()
        self.recovered_ack_lost = lost
        self.recovered_incomplete = incomplete

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- connection / attempt bookkeeping --------------------------------
    def next_conn_id(self) -> int:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM meta WHERE key='conn_counter'"
                ).fetchone()
                nxt = int(row[0]) + 1
                self._conn.execute(
                    "UPDATE meta SET value=? WHERE key='conn_counter'", (str(nxt),)
                )
                self._conn.commit()
                return nxt
            except Exception:
                self._conn.rollback()
                raise

    def begin_attempt(self, conn_id: int, frame_no: int, raw: bytes) -> int:
        """Persist a durable 'received' marker before any processing."""
        digest = hashlib.sha256(raw).hexdigest()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO inbox(conn_id, frame_no, received_at, sha256, state)"
                " VALUES (?,?,?,?,?)",
                (conn_id, frame_no, self._clock(), digest, ST_RECEIVED),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def record_frame_error(
        self, conn_id: int, offset: int, reason: str, detail: str = ""
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO frame_errors(conn_id, offset, reason, detail, at)"
                " VALUES (?,?,?,?,?)",
                (conn_id, offset, _bound_text(reason), _bound_text(detail), self._clock()),
            )
            self._conn.commit()

    def reject_attempt(
        self, attempt_id: int, state: str, detail: str, raw: bytes | None,
        sender: str | None = None, control_id: str | None = None,
        only_if_state: str | None = None,
    ) -> None:
        """Record a business rejection / identity conflict with bounded
        diagnostics and raw evidence.

        ``only_if_state`` restricts the update to attempts still in that
        state -- used so a late internal failure cannot overwrite an
        already-committed attempt's truth.
        """
        sql = ("UPDATE inbox SET state=?, detail=?, evidence=?, sender=?,"
               " control_id=? WHERE id=?")
        args: tuple = (
            state, _bound_text(detail), _bound_evidence(raw), sender,
            control_id, attempt_id,
        )
        if only_if_state is not None:
            sql += " AND state=?"
            args = args + (only_if_state,)
        with self._lock:
            self._conn.execute(sql, args)
            self._conn.commit()

    # -- the critical section --------------------------------------------
    def commit_message(self, attempt_id: int, msg: ParsedMessage, raw: bytes) -> str:
        """Decide and persist in ONE transaction.

        Returns OUT_COMMITTED / OUT_DUPLICATE / OUT_CONFLICT.  The AA may
        only be sent after this returns OUT_COMMITTED or OUT_DUPLICATE.
        """
        digest = hashlib.sha256(raw).hexdigest()
        now = self._clock()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT sha256 FROM messages WHERE sender=? AND control_id=?",
                    (msg.sender, msg.control_id),
                ).fetchone()
                if row is not None:
                    if row[0] == digest:
                        self._conn.execute(
                            "UPDATE inbox SET state=?, sender=?, control_id=? WHERE id=?",
                            (ST_DUPLICATE, msg.sender, msg.control_id, attempt_id),
                        )
                        self._conn.commit()
                        return OUT_DUPLICATE
                    self._conn.execute(
                        "UPDATE inbox SET state=?, sender=?, control_id=?,"
                        " detail=?, evidence=? WHERE id=?",
                        (
                            ST_CONFLICT,
                            msg.sender,
                            msg.control_id,
                            _bound_text(
                                "identity conflict: same (sender, control id) "
                                "with different payload bytes"
                            ),
                            _bound_evidence(raw),
                            attempt_id,
                        ),
                    )
                    self._conn.commit()
                    return OUT_CONFLICT

                self._conn.execute(
                    "INSERT INTO messages(sender, control_id, version, msg_type,"
                    " sent_at, received_at, sha256, raw, status)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        msg.sender,
                        msg.control_id,
                        msg.version,
                        "ORU^R01",
                        msg.sent_at,
                        now,
                        digest,
                        raw,
                        ST_COMMITTED,
                    ),
                )
                for seq, (auth, ident) in enumerate(msg.patients, start=1):
                    self._conn.execute(
                        "INSERT INTO patients(sender, control_id, seq, authority,"
                        " identifier) VALUES (?,?,?,?,?)",
                        (msg.sender, msg.control_id, seq, auth, ident),
                    )
                primary_auth, primary_id = msg.patients[0]
                for group in msg.groups:
                    for obx in group.obxs:
                        self._conn.execute(
                            "INSERT INTO results(sender, control_id, obr_index,"
                            " obx_index, patient_authority, patient_id, visit_id,"
                            " placer_order, filler_order, test_code, test_system,"
                            " test_name, obs_code, obs_system, obs_name, sub_id,"
                            " value_type, value_text, units, ref_range, abnormal,"
                            " rstatus, observed_at, reported_at, obr_seg, obx_seg,"
                            " merged) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                msg.sender, msg.control_id,
                                group.obr_index, obx.obx_index,
                                primary_auth, primary_id, msg.visit_id,
                                group.placer_order, group.filler_order,
                                group.test_code, group.test_system, group.test_name,
                                obx.obs_code, obx.obs_system, obx.obs_name,
                                obx.sub_id, obx.value_type, obx.value_text,
                                obx.units, obx.ref_range, obx.abnormal, obx.status,
                                obx.observed_at, group.reported_at,
                                obx.obr_seg, obx.obx_seg, obx.merged,
                            ),
                        )
                        for nseq, nte in enumerate(obx.ntes, start=1):
                            self._conn.execute(
                                "INSERT INTO notes(sender, control_id, obr_index,"
                                " obx_index, seg, seq, text) VALUES (?,?,?,?,?,?,?)",
                                (msg.sender, msg.control_id, group.obr_index,
                                 obx.obx_index, nte.seg_index, nseq, nte.text),
                            )
                    for nseq, nte in enumerate(group.ntes, start=1):
                        self._conn.execute(
                            "INSERT INTO notes(sender, control_id, obr_index,"
                            " obx_index, seg, seq, text) VALUES (?,?,?,NULL,?,?,?)",
                            (msg.sender, msg.control_id, group.obr_index,
                             nte.seg_index, nseq, nte.text),
                        )
                self._conn.execute(
                    "UPDATE inbox SET state=?, sender=?, control_id=? WHERE id=?",
                    (ST_COMMITTED, msg.sender, msg.control_id, attempt_id),
                )
                self._conn.commit()
                return OUT_COMMITTED
            except Exception:
                self._conn.rollback()
                raise

    def mark_acked(self, attempt_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE inbox SET state=? WHERE id=?", (ST_ACKED, attempt_id)
            )
            self._conn.commit()

    # -- queries -----------------------------------------------------------
    def list_messages(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT m.sender, m.control_id, m.version, m.msg_type, m.sent_at,"
                " m.received_at, m.sha256, m.status,"
                " (SELECT COUNT(*) FROM results r WHERE r.sender=m.sender"
                "  AND r.control_id=m.control_id) AS n_results"
                " FROM messages m ORDER BY m.rowid"
            ).fetchall()

    def get_message(self, sender: str, control_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM messages WHERE sender=? AND control_id=?",
                (sender, control_id),
            ).fetchone()

    def results_for_message(self, sender: str, control_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM results WHERE sender=? AND control_id=?"
                " ORDER BY obr_index, obx_index",
                (sender, control_id),
            ).fetchall()

    def results_for_patient(self, authority: str, identifier: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT r.* FROM results r"
                " WHERE EXISTS (SELECT 1 FROM patients p"
                "  WHERE p.sender=r.sender AND p.control_id=r.control_id"
                "  AND p.authority=? AND p.identifier=?)"
                " ORDER BY r.rowid",
                (authority, identifier),
            ).fetchall()

    def results_for_order(self, order: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM results WHERE placer_order=? OR filler_order=?"
                " ORDER BY rowid",
                (order, order),
            ).fetchall()

    def iter_results(self, batch: int = 1000) -> Iterator[list[sqlite3.Row]]:
        """Stream all result rows in stable rowid order, in batches."""
        with self._lock:
            last = 0
            while True:
                rows = self._conn.execute(
                    "SELECT * FROM results WHERE rowid > ? ORDER BY rowid LIMIT ?",
                    (last, batch),
                ).fetchall()
                if not rows:
                    return
                last = rows[-1][0]
                yield rows

    def notes_for_message(self, sender: str, control_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM notes WHERE sender=? AND control_id=?"
                " ORDER BY obr_index, COALESCE(obx_index, 0), seq",
                (sender, control_id),
            ).fetchall()

    def patients_for_message(self, sender: str, control_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM patients WHERE sender=? AND control_id=? ORDER BY seq",
                (sender, control_id),
            ).fetchall()

    def diagnostics(self) -> dict[str, list[sqlite3.Row]]:
        with self._lock:
            inbox = self._conn.execute(
                "SELECT * FROM inbox ORDER BY id"
            ).fetchall()
            errors = self._conn.execute(
                "SELECT * FROM frame_errors ORDER BY id"
            ).fetchall()
        return {"inbox": inbox, "frame_errors": errors}

    def count_results(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM results").fetchone()[0])
