"""One report = one immutable database snapshot.

Concurrency contract
--------------------
A report may run while the receiver keeps accepting messages.  Every metric
in the same report -- numerators, denominators, source samples -- must come
from the *same* logical view of the warehouse.

Implementation: the audit opens its **own** read-only SQLite URI connection
(``mode=ro``), starts a deferred transaction immediately and keeps it open
until the report is assembled.  The production database runs in WAL mode,
so readers never block writers and a reader inside a transaction observes a
fixed snapshot taken at its first read, regardless of later commits.  The
audit never writes, never runs recovery, and only reads ``messages`` and
``results`` -- rejected/unconfirmed ``inbox`` rows cannot become facts.

The snapshot descriptor (max rowids plus deterministic SHA-256 fingerprints
of both fact tables, computed in stable order) is embedded in the report so
two runs on the same snapshot can be proven identical byte-for-byte.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass

# Columns pulled from the fact tables, in stable order.  Only these columns
# exist for result facts; raw message bytes and inbox evidence are never read.
MESSAGE_COLS = (
    "rowid", "sender", "control_id", "version", "msg_type", "sent_at",
    "received_at", "status",
)
# results.id is declared INTEGER PRIMARY KEY, hence an alias for rowid;
# the driver only exposes it under its declared name.
RESULT_COLS = (
    "id", "sender", "control_id", "obr_index", "obx_index",
    "patient_authority", "patient_id", "visit_id", "placer_order",
    "filler_order", "test_code", "test_system", "test_name", "obs_code",
    "obs_system", "obs_name", "sub_id", "value_type", "value_text", "units",
    "ref_range", "abnormal", "rstatus", "observed_at", "reported_at",
    "obr_seg", "obx_seg", "merged",
)
_BATCH = 1000


@dataclass(frozen=True)
class SnapshotInfo:
    journal_mode: str
    message_max_rowid: int
    result_max_rowid: int
    message_count: int
    result_count: int
    messages_sha256: str
    results_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "journal_mode": self.journal_mode,
            "message_max_rowid": self.message_max_rowid,
            "result_max_rowid": self.result_max_rowid,
            "message_count": self.message_count,
            "result_count": self.result_count,
            "messages_sha256": self.messages_sha256,
            "results_sha256": self.results_sha256,
        }


class AuditSnapshot:
    """A single read-only connection pinned to one WAL snapshot."""

    def __init__(self, db_path: str) -> None:
        uri = f"file:{db_path}?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True)
        self._conn.row_factory = sqlite3.Row
        # Read-only defensive pragmas; the URI already forbids writes.
        self._conn.execute("PRAGMA query_only=ON")
        self._closed = False
        # First read establishes the WAL snapshot held by this transaction.
        self._conn.execute("BEGIN")
        mode = str(
            self._conn.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        self.snapshot = self._describe(mode)

    # -- internals --------------------------------------------------------
    def _describe(self, journal_mode: str) -> SnapshotInfo:
        m_max = self._conn.execute(
            "SELECT COALESCE(MAX(rowid), 0) FROM messages"
        ).fetchone()[0]
        r_max = self._conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM results"
        ).fetchone()[0]
        m_total = self._conn.execute(
            "SELECT COUNT(*) FROM messages"
        ).fetchone()[0]
        r_total = self._conn.execute(
            "SELECT COUNT(*) FROM results"
        ).fetchone()[0]
        m_hash = self._fingerprint("messages", MESSAGE_COLS)
        r_hash = self._fingerprint("results", RESULT_COLS)
        return SnapshotInfo(
            journal_mode=journal_mode,
            message_max_rowid=int(m_max),
            result_max_rowid=int(r_max),
            message_count=int(m_total),
            result_count=int(r_total),
            messages_sha256=m_hash,
            results_sha256=r_hash,
        )

    def _fingerprint(self, table: str, columns: tuple[str, ...]) -> str:
        """Deterministic SHA-256 over all rows in rowid order.

        Rows are encoded as length-prefixed canonical cell values so the
        hash cannot be confused by delimiter collisions; streamed in batches
        so a large warehouse is never materialised in memory.
        """
        digest = hashlib.sha256()
        col_list = ", ".join(columns)
        order_col = "rowid" if table == "messages" else "id"
        last_rowid = 0
        while True:
            rows = self._conn.execute(
                f"SELECT {col_list} FROM {table} "
                f"WHERE {order_col} > ? ORDER BY {order_col} LIMIT ?",
                (last_rowid, _BATCH),
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
            last_rowid = rows[-1][order_col]
        return digest.hexdigest()

    # -- accessors --------------------------------------------------------
    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def count_messages_in_window(
        self, senders: tuple[str, ...], start: str, end: str
    ) -> int:
        placeholders = ",".join("?" for _ in senders)
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM messages "
            f"WHERE sender IN ({placeholders})"
            "  AND received_at >= ? AND received_at < ?",
            (*senders, start, end),
        ).fetchone()
        return int(row[0])

    def stream_window_results(
        self,
        senders: tuple[str, ...],
        test_codes: tuple[str, ...] | None,
        start: str,
        end: str,
        batch_size: int = _BATCH,
    ):
        """Yield result rows belonging to messages in ``[start, end)``.

        Facts are joined to their parent message; only committed messages
        exist in ``messages`` (inbox rejects never create rows), so this
        view can never include rejected or unconfirmed records.  Rows stream
        in stable ``(messages.rowid, results.rowid)`` order.
        """
        sender_ph = ",".join("?" for _ in senders)
        where = (
            " FROM results r JOIN messages m"
            "   ON m.sender = r.sender AND m.control_id = r.control_id"
            f" WHERE m.sender IN ({sender_ph})"
            "   AND m.received_at >= ? AND m.received_at < ?"
        )
        base_params: list = [*senders, start, end]
        if test_codes is not None:
            code_ph = ",".join("?" for _ in test_codes)
            where += f" AND r.test_code IN ({code_ph})"
            base_params.extend(test_codes)
        # keyset pagination on (message rowid, result id) keeps each query
        # bounded, avoids OFFSET scans and fixes the ordering deterministically
        page_sql = (
            "SELECT r.*, m.received_at AS message_received_at,"
            "       m.rowid AS message_rowid" + where
            + " AND (m.rowid > ? OR (m.rowid = ? AND r.id > ?))"
            " ORDER BY m.rowid, r.id LIMIT ?"
        )
        last_msg = 0
        last_row = 0
        while True:
            rows = self._conn.execute(
                page_sql,
                (*base_params, last_msg, last_msg, last_row, batch_size),
            ).fetchall()
            if not rows:
                return
            for row in rows:
                yield row
            last_msg = rows[-1]["message_rowid"]
            last_row = rows[-1]["id"]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._conn.execute("COMMIT")
        except sqlite3.Error:
            self._conn.rollback()
        self._conn.close()

    def __enter__(self) -> "AuditSnapshot":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
