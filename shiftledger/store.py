"""SQLite storage for the handover ledger -- a separate database.

The ledger never opens the receiver database for writing.  Its own database
holds:

* the validated manifest (work orders, receivable items, order mappings);
* immutable *fact snapshots* copied (read-only) from ``messages`` /
  ``results`` of the receiver for one fact batch, so older versions keep
  their exact input even when new facts arrive later;
* durable manual decisions with reasons;
* every published version: per-group attribution, all candidate/potential
  edges with reasons, per-item settlement and per-observation disposition.

Writes happen in bounded batches (``executemany`` + periodic commit inside
one logical transaction); the engine processes work order by work order so
the full candidate graph is never held in memory.
"""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = "1"

INSERT_BATCH = 2000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledgers (
  ledger_id      TEXT PRIMARY KEY,
  shift_id       TEXT NOT NULL,
  shift_start_us INTEGER NOT NULL,
  created_at     TEXT NOT NULL,
  head_version   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS senders (
  ledger_id TEXT NOT NULL,
  sender    TEXT NOT NULL,
  PRIMARY KEY (ledger_id, sender)
);
CREATE TABLE IF NOT EXISTS work_orders (
  ledger_id      TEXT NOT NULL,
  work_order_id  TEXT NOT NULL,
  patient_domain TEXT NOT NULL,
  patient_id     TEXT NOT NULL,
  PRIMARY KEY (ledger_id, work_order_id)
);
CREATE TABLE IF NOT EXISTS items (
  ledger_id      TEXT NOT NULL,
  item_id        TEXT NOT NULL,
  work_order_id  TEXT NOT NULL,
  code_system    TEXT NOT NULL,
  code           TEXT NOT NULL,
  sub_id         TEXT,              -- explicit NULL: matches only NULL sub ids
  due_at_us      INTEGER NOT NULL,
  PRIMARY KEY (ledger_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_items_wo
  ON items (ledger_id, work_order_id);
-- covering index for the exact (system, code, sub_id) edge join
CREATE INDEX IF NOT EXISTS idx_items_match
  ON items (ledger_id, work_order_id, code_system, code, sub_id);
CREATE TABLE IF NOT EXISTS mappings (
  ledger_id      TEXT NOT NULL,
  mapping_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  sender         TEXT NOT NULL,
  number_class   TEXT NOT NULL,
  local_number   TEXT NOT NULL,
  work_order_id  TEXT NOT NULL,
  valid_from_us  INTEGER NOT NULL,
  valid_to_us    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mappings_lookup
  ON mappings (ledger_id, sender, number_class, local_number);

-- ---------------------------------------------------------------------
-- immutable fact snapshots (one batch = one read of the receiver)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_batches (
  batch_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at        TEXT NOT NULL,
  cutoff_us         INTEGER NOT NULL,
  shift_start_us    INTEGER NOT NULL,
  recv_message_max  INTEGER NOT NULL,
  recv_result_max   INTEGER NOT NULL,
  recv_messages_sha TEXT NOT NULL,
  recv_results_sha  TEXT NOT NULL,
  n_groups          INTEGER NOT NULL,
  n_observations    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_groups (
  batch_id        INTEGER NOT NULL,
  group_key       TEXT NOT NULL,
  sender          TEXT NOT NULL,
  control_id      TEXT NOT NULL,
  obr_index       INTEGER NOT NULL,
  received_at_us  INTEGER NOT NULL,
  patient_domain  TEXT,
  patient_id      TEXT,
  PRIMARY KEY (batch_id, group_key)
);
CREATE INDEX IF NOT EXISTS idx_fact_groups_received
  ON fact_groups (batch_id, received_at_us);
CREATE TABLE IF NOT EXISTS fact_group_orders (
  batch_id      INTEGER NOT NULL,
  group_key     TEXT NOT NULL,
  number_class  TEXT NOT NULL,   -- 'placer' | 'filler'
  local_number  TEXT NOT NULL,
  PRIMARY KEY (batch_id, group_key, number_class)
);
CREATE TABLE IF NOT EXISTS fact_observations (
  batch_id     INTEGER NOT NULL,
  obs_key      TEXT NOT NULL,
  group_key    TEXT NOT NULL,
  obx_index    INTEGER NOT NULL,
  code_system  TEXT NOT NULL,
  code         TEXT NOT NULL,
  sub_id       TEXT,
  PRIMARY KEY (batch_id, obs_key)
);
CREATE INDEX IF NOT EXISTS idx_fact_obs_group
  ON fact_observations (batch_id, group_key);

-- ---------------------------------------------------------------------
-- durable manual decisions (never rewritten; withdrawal appends)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS decisions (
  decision_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  ledger_id      TEXT NOT NULL,
  action         TEXT NOT NULL,
  group_key      TEXT,
  work_order_id  TEXT,
  obs_key        TEXT,
  item_id        TEXT,
  reason         TEXT NOT NULL,
  requested_by   TEXT NOT NULL,
  created_at     TEXT NOT NULL,
  base_version   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_ledger ON decisions (ledger_id, decision_id);

-- ---------------------------------------------------------------------
-- published versions
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS versions (
  version_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  ledger_id        TEXT NOT NULL,
  batch_id         INTEGER NOT NULL,
  cutoff_us        INTEGER NOT NULL,
  kind             TEXT NOT NULL,        -- 'initial' | 'decision' | 'extend'
  base_version     INTEGER,
  created_at       TEXT NOT NULL,
  summary          TEXT NOT NULL         -- JSON document
);
CREATE TABLE IF NOT EXISTS version_groups (
  version_id   INTEGER NOT NULL,
  group_key    TEXT NOT NULL,
  status       TEXT NOT NULL,
  work_order_id TEXT,
  detail       TEXT NOT NULL,            -- JSON: per-number mapping detail etc.
  decided      INTEGER NOT NULL DEFAULT 0,  -- 1 manual assignment/withdrawal
  PRIMARY KEY (version_id, group_key)
);
CREATE TABLE IF NOT EXISTS version_edges (
  version_id    INTEGER NOT NULL,
  group_key     TEXT NOT NULL,
  obs_key       TEXT NOT NULL,
  item_id       TEXT NOT NULL,
  edge_kind     TEXT NOT NULL            -- auto | lock | candidate | potential
);
CREATE INDEX IF NOT EXISTS idx_vedges_item ON version_edges (version_id, item_id);
CREATE INDEX IF NOT EXISTS idx_vedges_obs  ON version_edges (version_id, obs_key);
CREATE TABLE IF NOT EXISTS version_items (
  version_id     INTEGER NOT NULL,
  item_id        TEXT NOT NULL,
  work_order_id  TEXT NOT NULL,
  status         TEXT NOT NULL,
  edge_kind      TEXT,                   -- auto | lock when settled
  obs_key        TEXT,
  received_at_us INTEGER,
  timeliness     TEXT,                   -- on_time | late when settled
  block_reasons  TEXT,                   -- JSON list when pending_decision
  PRIMARY KEY (version_id, item_id)
);
CREATE TABLE IF NOT EXISTS version_obs (
  version_id   INTEGER NOT NULL,
  obs_key      TEXT NOT NULL,
  group_key    TEXT NOT NULL,
  disposition  TEXT NOT NULL,
  item_id      TEXT,
  detail       TEXT NOT NULL,            -- JSON, always present (may be {})
  PRIMARY KEY (version_id, obs_key)
);
CREATE INDEX IF NOT EXISTS idx_vobs_group ON version_obs (version_id, group_key);
"""


def open_ledger(path: str) -> sqlite3.Connection:
    """Open (or create) the ledger database with bounded-memory pragmas."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # Keep SQLite's own footprint small; the engine streams instead of caching.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    # Scratch tables can hold hundreds of thousands of edge rows; keep them
    # in temporary *files*, not the process heap, so RSS stays bounded.
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA cache_size=-20000")  # ~20 MiB page cache ceiling
    conn.execute("PRAGMA mmap_size=0")
    conn.executescript(_SCHEMA)
    return conn


def open_receiver_readonly(path: str) -> sqlite3.Connection:
    """Open the receiver database strictly read-only on a pinned snapshot.

    A normal connection with ``query_only`` is used instead of a ``mode=ro``
    URI: the receiver runs in WAL mode, and ``mode=ro`` cannot create the
    ``-wal``/``-shm`` sidecars.  ``query_only`` rejects every write, and the
    explicit ``BEGIN`` pins one WAL snapshot for the whole fact batch.
    """
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    # First read inside this transaction pins the WAL snapshot for the batch.
    conn.execute("BEGIN")
    return conn


class BatchInserter:
    """Accumulate rows up to a bound and flush with ``executemany``.

    The caller owns the transaction; this helper only batches parameter
    tuples so we never hold a Python list of the whole dataset.
    """

    def __init__(self, conn: sqlite3.Connection, sql: str, size: int = INSERT_BATCH):
        self._conn = conn
        self._sql = sql
        self._size = size
        self._buf: list[tuple] = []
        self.total = 0

    def add(self, row: tuple) -> None:
        self._buf.append(row)
        if len(self._buf) >= self._size:
            self.flush()

    def flush(self) -> None:
        if self._buf:
            self._conn.executemany(self._sql, self._buf)
            self.total += len(self._buf)
            self._buf.clear()
