"""Snapshot pinning and concurrent-writer tests.

The audit must observe exactly one logical database view even while the
receiver keeps committing messages.  These tests interleave a real writer
with audit reads and prove:

* a snapshot opened before a commit keeps seeing the old view for its whole
  lifetime (counts, fingerprints, window scans), while a freshly opened
  snapshot sees the new state;
* every report produced under concurrent writes is internally consistent
  with its own snapshot descriptor (no mixing of two database states);
* two reports built on the same snapshot are byte-identical;
* the audit never mutates the receiver's database (inbox states included).
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest

from hl7recv.audit.engine import render_report, run_audit
from hl7recv.audit.policy import load_policy
from hl7recv.audit.snapshot import AuditSnapshot
from hl7recv.builder import build_message
from hl7recv.store import Store
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POLICY_PATH = os.path.join(ROOT, "drills", "audit_policy.example.json")


class SnapshotPinning(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "snap.db")
        self.store = Store(self.db)
        ad.populate(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _commit_extra_message(self, control_id: str, received_at: str) -> None:
        rows = [ad.DrillRow("NM", "WBC", "6.1", units="10*9/L",
                            observed_at="20261110113000")]
        raw = ad.build_oru_message(ad.SENDER_A, control_id, rows)
        parsed = build_message(raw)
        old_clock = self.store._clock
        self.store._clock = lambda: received_at  # noqa: E731
        try:
            attempt = self.store.begin_attempt(7, 1, raw)
            self.assertEqual(
                self.store.commit_message(attempt, parsed, raw), "committed"
            )
            self.store.mark_acked(attempt)
        finally:
            self.store._clock = old_clock

    def test_open_snapshot_is_pinned_across_later_commit(self):
        with AuditSnapshot(self.db) as snap:
            before = snap.snapshot
            self.assertEqual(before.message_count, 8)
            self.assertEqual(before.result_count, 22)
            base_msgs = snap.count_messages_in_window(
                (ad.SENDER_A, ad.SENDER_B),
                ad.CURRENT_START, ad.CURRENT_END,
            )
            self.assertEqual(base_msgs, 3)

            # writer commits a message that lands INSIDE the current window
            self._commit_extra_message(
                "LATE1", "2026-11-10T11:30:00.000+00:00"
            )

            # the pinned snapshot must not observe the new commit
            during = snap.snapshot
            self.assertEqual(during.message_count, 8)
            self.assertEqual(during.results_sha256, before.results_sha256)
            self.assertEqual(
                snap.count_messages_in_window(
                    (ad.SENDER_A, ad.SENDER_B),
                    ad.CURRENT_START, ad.CURRENT_END,
                ),
                base_msgs,
            )
            streamed = list(snap.stream_window_results(
                (ad.SENDER_A, ad.SENDER_B), None,
                ad.CURRENT_START, ad.CURRENT_END,
            ))
            self.assertEqual(len(streamed), 9)  # current-window rows pre-write

        # a new snapshot sees the committed state
        with AuditSnapshot(self.db) as snap2:
            self.assertEqual(snap2.snapshot.message_count, 9)
            self.assertEqual(snap2.snapshot.result_count, 23)
            self.assertEqual(
                snap2.count_messages_in_window(
                    (ad.SENDER_A, ad.SENDER_B),
                    ad.CURRENT_START, ad.CURRENT_END,
                ),
                4,
            )

    def test_snapshot_runs_in_wal_and_is_read_only(self):
        with AuditSnapshot(self.db) as snap:
            self.assertEqual(snap.snapshot.journal_mode, "wal")
            with self.assertRaises(Exception):
                snap.conn.execute(
                    "UPDATE results SET value_text='x' WHERE id=1"
                )

    def test_audit_does_not_mutate_database(self):
        inbox_before = [
            dict(r) for r in self.store.diagnostics()["inbox"]
        ]
        policy = load_policy(POLICY_PATH)
        report, _ = run_audit(self.db, policy)
        inbox_after = [
            dict(r) for r in self.store.diagnostics()["inbox"]
        ]
        self.assertEqual(inbox_before, inbox_after)
        # running the audit again yields the identical snapshot descriptor
        report2, _ = run_audit(self.db, policy)
        self.assertEqual(report["snapshot"], report2["snapshot"])
        self.assertEqual(
            render_report(report), render_report(report2)
        )


class ConcurrentWritersMixedStates(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "mix.db")
        self.store = Store(self.db)
        ad.populate(self.store)
        self.policy = load_policy(POLICY_PATH)
        self.stop = threading.Event()

    def tearDown(self):
        self.stop.set()
        self.store.close()
        self.tmp.cleanup()

    def test_reports_never_mix_two_database_states(self):
        import sqlite3

        reports: list[dict] = []
        errors: list[Exception] = []

        def writer() -> None:
            i = 0
            while not self.stop.is_set():
                i += 1
                cid = f"CONC{i:04d}"
                ts = "2026-11-10T11:20:00.000+00:00"
                rows = [ad.DrillRow("NM", "WBC", f"{i}.0",
                                    observed_at="20261110112000")]
                try:
                    raw = ad.build_oru_message(ad.SENDER_A, cid, rows)
                    parsed = build_message(raw)
                    old = self.store._clock
                    self.store._clock = lambda ts=ts: ts  # noqa: E731
                    try:
                        attempt = self.store.begin_attempt(100 + i, 1, raw)
                        self.store.commit_message(attempt, parsed, raw)
                    finally:
                        self.store._clock = old
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)
                self.stop.wait(0.002)

        def auditor() -> None:
            for _ in range(12):
                if self.stop.is_set():
                    break
                report, _rc = run_audit(self.db, self.policy)
                reports.append(report)

        threads = [threading.Thread(target=writer, daemon=True)]
        threads += [threading.Thread(target=auditor, daemon=True)
                    for _ in range(3)]
        for t in threads:
            t.start()
        self.stop.wait(2.5)
        self.stop.set()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertGreaterEqual(len(reports), 12)

        ro = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            for report in reports:
                snap = report["snapshot"]
                # independent rowid-bounded re-derivation of every window
                # count the report claims; a mixed-state report could not
                # satisfy both boundaries at once.
                for win_name, literals in (
                    ("baseline", report["windows"]["baseline"]),
                    ("current", report["windows"]["current"]),
                ):
                    got = ro.execute(
                        "SELECT COUNT(*) FROM messages"
                        " WHERE rowid <= ? AND sender IN (?, ?)"
                        " AND received_at >= ? AND received_at < ?",
                        (snap["message_max_rowid"], ad.SENDER_A, ad.SENDER_B,
                         literals["start_canonical"],
                         literals["end_canonical"]),
                    ).fetchone()[0]
                    block = report["summary"][
                        f"{win_name}_matching_messages"
                    ]
                    self.assertEqual(
                        got, block,
                        f"{win_name} count inconsistent with snapshot rowid "
                        "boundary -- report mixed database states",
                    )
                # fingerprint/rowid self-consistency
                self.assertEqual(
                    snap["message_count"],
                    ro.execute(
                        "SELECT COUNT(*) FROM messages"
                        " WHERE rowid <= ?",
                        (snap["message_max_rowid"],),
                    ).fetchone()[0],
                )
        finally:
            ro.close()

        # reports sharing a snapshot must be byte-identical; reports from
        # different snapshots must differ in their snapshot block at least
        by_fp: dict[str, str] = {}
        for report in reports:
            fp = report["snapshot"]["messages_sha256"]
            text = render_report(report)
            if fp in by_fp:
                self.assertEqual(by_fp[fp], text)
            else:
                by_fp[fp] = text
        self.assertGreater(len(by_fp), 1, "writer never produced a new state")


if __name__ == "__main__":
    unittest.main()
