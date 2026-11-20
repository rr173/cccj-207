"""Independent reference check for the result subscription.

Reads the receiver database and the analytics replica directly (stdlib
sqlite3 only -- no product paging or import code) and derives, from the
committed facts and the published positions, exactly what the replica
must contain: every subscribed message in commit order with its result
coordinates and raw values, and nothing else.  Compares that derivation
against the replica contents and the replica cursor.

  python3 -m refcheck.sync_reference --receiver-db RECV --replica-db REPL \
      --sender 'LAB1^HOSP' [--sender ...]

Exit code 0 and ``SYNC REFERENCE CHECK PASSED`` when the replica matches
the independent derivation exactly; 1 with a diff otherwise.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys

RESULT_COLS = ("obr_index", "obx_index", "obs_system", "obs_code",
               "sub_id", "value_type", "value_text", "units")


def _connect_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _results(conn: sqlite3.Connection, table: str, sender: str,
             control_id: str) -> list[dict]:
    rows = conn.execute(
        f"SELECT {', '.join(RESULT_COLS)} FROM {table}"
        " WHERE sender=? AND control_id=? ORDER BY obr_index, obx_index",
        (sender, control_id),
    ).fetchall()
    return [{k: row[k] for k in RESULT_COLS} for row in rows]


def derive_expected(recv_db: str, senders: list[str]) -> list[dict]:
    """Expected replica contents, derived straight from the source."""
    conn = _connect_ro(recv_db)
    try:
        scanned = conn.execute(
            "SELECT sp.seq AS seq, m.sender AS sender,"
            " m.control_id AS control_id, m.received_at AS received_at"
            " FROM sync_positions sp"
            " JOIN messages m ON m.sender=sp.sender"
            "  AND m.control_id=sp.control_id ORDER BY sp.seq"
        ).fetchall()
        wanted = set(senders)
        expected = []
        for row in scanned:
            if row["sender"] not in wanted:
                continue
            expected.append({
                "position": row["seq"],
                "sender": row["sender"],
                "control_id": row["control_id"],
                "received_at": row["received_at"],
                "results": _results(conn, "results", row["sender"],
                                    row["control_id"]),
            })
        return expected
    finally:
        conn.close()


def read_replica(replica_db: str) -> tuple[int, list[dict]]:
    conn = _connect_ro(replica_db)
    try:
        cursor = int(conn.execute(
            "SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
        msgs = conn.execute(
            "SELECT sender, control_id, received_at, position"
            " FROM sub_messages ORDER BY position"
        ).fetchall()
        actual = [{
            "position": m["position"],
            "sender": m["sender"],
            "control_id": m["control_id"],
            "received_at": m["received_at"],
            "results": _results(conn, "sub_results", m["sender"],
                                m["control_id"]),
        } for m in msgs]
        return cursor, actual
    finally:
        conn.close()


def compare(expected: list[dict], actual: list[dict]) -> list[str]:
    problems = []
    if len(expected) != len(actual):
        problems.append(
            f"message count differs: expected {len(expected)}, got {len(actual)}"
        )
    for i, (exp, act) in enumerate(zip(expected, actual)):
        if exp != act:
            problems.append(f"message #{i} differs:\n  expected "
                            f"{json.dumps(exp, sort_keys=True)}\n  actual   "
                            f"{json.dumps(act, sort_keys=True)}")
        if len(problems) > 5:
            problems.append("... (further differences suppressed)")
            break
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--receiver-db", required=True)
    ap.add_argument("--replica-db", required=True)
    ap.add_argument("--sender", action="append", required=True)
    args = ap.parse_args(argv)

    senders = sorted(set(s.strip() for s in args.sender))
    expected_all = derive_expected(args.receiver_db, senders)
    cursor, actual = read_replica(args.replica_db)
    # the replica may legitimately lag the source: compare only the
    # prefix of the expectation that the cursor says has been applied
    expected = [m for m in expected_all if m["position"] <= cursor]
    problems = compare(expected, actual)
    # every landed fact must lie at or below the cursor (the cursor may
    # legitimately be ahead of the last landed message: empty pages over
    # non-subscribed commits still advance it)
    over = [m for m in actual if m["position"] > cursor]
    if over:
        problems.append(
            f"{len(over)} landed messages lie beyond the cursor {cursor}"
        )
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        return 1
    print("SYNC REFERENCE CHECK PASSED "
          f"({len(actual)} messages, cursor {cursor})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
