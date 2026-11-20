"""Offline upgrade: interruption, re-run, and refetch determinism.

* an upgrade killed halfway exposes no positions at all (single
  transaction); the re-run assigns the complete documented order;
* an already-upgraded database is verified, never renumbered;
* re-fetching the same (subscription, start, bound, budget) yields
  byte-identical pages after the receiver keeps writing, after a
  service restart and after SQLite VACUUM;
* the page command never swaps a stale bound for a newer watermark and
  refuses bounds in the future.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from hl7recv.builder import build_message
from hl7recv.store import Store
from hl7recv.sync import publish
from tests import fixtures as fx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUB = ["LAB1^HOSP"]


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hl7recv", *args],
        capture_output=True, text=True, cwd=ROOT,
    )


class UpgradeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "old.db")
        # an "old" database: messages committed before sync existed
        store = Store(self.db)
        for i, payload in enumerate((fx.MSG1, fx.MSG2, fx.MSG_DUP_MERGE), 1):
            store.commit_message(i, build_message(payload), payload)
        store.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _positions(self) -> list[tuple]:
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(
                "SELECT seq, sender, control_id FROM sync_positions ORDER BY seq"
            ).fetchall()
        finally:
            conn.close()

    def test_interrupted_upgrade_exposes_nothing_and_rerun_completes(self):
        # kill the upgrade after the first backfill chunk (drill-only flag)
        proc = _cli("sync", "upgrade", "--db", self.db, "--fault-after-rows", "1")
        self.assertEqual(proc.returncode, 70, proc.stderr)

        conn = sqlite3.connect(self.db)
        try:
            self.assertIsNone(
                conn.execute(
                    "SELECT value FROM meta WHERE key='sync_enabled'"
                ).fetchone(),
                "half an upgrade must not be visible",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM sync_positions").fetchone()[0],
                0,
            )
        finally:
            conn.close()

        # re-run completes the full documented order (rowid ascending)
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        positions = self._positions()
        self.assertEqual(
            positions,
            [(1, "LAB1^HOSP", "MSG00001"), (2, "LAB1^HOSP", "MSG00002"),
             (3, "LAB1^HOSP", "MSG0000G")],
        )

        # a second re-run verifies and never renumbers
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn('"already"', proc.stdout)
        self.assertEqual(self._positions(), positions)

    def test_refetch_byte_identical_across_writes_restart_and_vacuum(self):
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        def fetch() -> bytes:
            return publish.fetch_page(self.db, SUB, start=0, bound=2, budget=5)

        first = fetch()

        # the receiver keeps writing AFTER our bound was fixed
        store = Store(self.db)
        store.commit_message(
            10, build_message(fx.MSG_REPEAT_PID3), fx.MSG_REPEAT_PID3
        )
        store.close()
        self.assertEqual(fetch(), first, "later commits must not leak in")

        # service restart (fresh process) does not change the bytes
        proc = _cli(
            "sync", "page", "--db", self.db, "--sender", SUB[0],
            "--start", "0", "--bound", "2", "--budget", "5",
            "--output", os.path.join(self.tmp.name, "p.json"),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(os.path.join(self.tmp.name, "p.json"), "rb") as fh:
            self.assertEqual(fh.read(), first)

        # VACUUM must not change positions or page bytes
        conn = sqlite3.connect(self.db)
        conn.execute("VACUUM")
        conn.close()
        self.assertEqual(self._positions(),
                         [(1, "LAB1^HOSP", "MSG00001"),
                          (2, "LAB1^HOSP", "MSG00002"),
                          (3, "LAB1^HOSP", "MSG0000G"),
                          (4, "LAB1^HOSP", "MSG0000R")])
        self.assertEqual(fetch(), first, "VACUUM must not change page bytes")

    def test_bound_is_never_swapped_or_exceeded(self):
        publish.upgrade(self.db)
        store = Store(self.db)
        store.commit_message(
            10, build_message(fx.MSG_REPEAT_PID3), fx.MSG_REPEAT_PID3
        )
        store.close()  # watermark is now 4

        # an old bound still means exactly that bound
        page = publish.fetch_page(self.db, SUB, start=0, bound=2, budget=100)
        from hl7recv.sync import protocol

        parsed = protocol.parse_page(page)
        self.assertEqual((parsed["bound"], parsed["end"], parsed["terminal"]),
                         (2, 2, 1))
        self.assertEqual([m["position"] for m in parsed["messages"]], [1, 2])

        # a future bound is rejected, not silently clamped
        with self.assertRaises(publish.BoundError):
            publish.fetch_page(self.db, SUB, start=0, bound=5, budget=1)
        proc = _cli(
            "sync", "page", "--db", self.db, "--sender", SUB[0],
            "--start", "0", "--bound", "5", "--budget", "1",
            "--output", os.path.join(self.tmp.name, "x.json"),
        )
        self.assertEqual(proc.returncode, 7)

    def test_not_enabled_and_empty_subscription_rejected(self):
        with self.assertRaises(publish.SyncNotEnabled):
            publish.watermark(self.db, SUB)
        proc = _cli("sync", "watermark", "--db", self.db, "--sender", SUB[0])
        self.assertEqual(proc.returncode, 6)
        publish.upgrade(self.db)
        with self.assertRaises(ValueError):
            publish.watermark(self.db, [])

    def test_rerun_after_post_upgrade_commits_never_renumbers(self):
        publish.upgrade(self.db)
        store = Store(self.db)
        store.commit_message(
            10, build_message(fx.MSG_REPEAT_PID3), fx.MSG_REPEAT_PID3
        )
        store.close()
        before = self._positions()
        # re-running the upgrade on the live-shape database only verifies
        meta = publish.upgrade(self.db)
        self.assertEqual(meta["status"], "already")
        self.assertEqual(meta["bound"], 4)
        self.assertEqual(self._positions(), before)

    def test_new_commit_and_position_are_atomic(self):
        """A crash before commit leaves neither message nor position."""
        publish.upgrade(self.db)
        before = self._positions()

        class Boom(Exception):
            pass

        class SabotagedStore(Store):
            def _assign_sync_position(self, sender, control_id):
                raise Boom("crash inside the commit transaction")

        store = SabotagedStore(self.db)
        with self.assertRaises(Boom):
            store.commit_message(
                10, build_message(fx.MSG_REPEAT_PID3), fx.MSG_REPEAT_PID3
            )
        store.close()
        # rolled back completely: no message, no position, no gap
        self.assertEqual(self._positions(), before)
        store = Store(self.db)
        self.assertEqual(len(store.list_messages()), 3)
        store.commit_message(
            11, build_message(fx.MSG_REPEAT_PID3), fx.MSG_REPEAT_PID3
        )
        store.close()
        self.assertEqual(self._positions()[-1][0], 4,
                         "the next commit reuses the rolled-back position")


if __name__ == "__main__":
    unittest.main()
