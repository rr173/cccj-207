"""Receiver/audit coexistence over a real MLLP connection.

While the audit holds its read-only snapshot, the live receiver must keep
accepting, committing and acknowledging messages, and the audit report
must reflect only the snapshot it pinned.  This complements
``test_audit_snapshot`` by driving writes through the actual TCP server
rather than the Store API directly.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest

from hl7recv.audit.engine import run_audit
from hl7recv.audit.policy import load_policy
from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import send_message
from tests import fixtures as fx
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POLICY_PATH = os.path.join(ROOT, "drills", "audit_policy.example.json")


class AuditWhileServing(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "coexist.db")
        self.store = Store(self.db)
        ad.populate(self.store)
        self.server = MllpServer("127.0.0.1", 0, self.store)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        self.policy = load_policy(POLICY_PATH)

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def test_audit_runs_while_server_commits_new_messages(self):
        # 1. pin a report snapshot
        report_before, _ = run_audit(self.db, self.policy)
        self.assertEqual(report_before["snapshot"]["message_count"], 8)

        # 2. while re-auditing repeatedly, push real MLLP traffic using the
        #    existing product fixtures; every message must get AA
        errors: list[Exception] = []
        reports: list[dict] = []

        def audit_loop() -> None:
            for _ in range(6):
                try:
                    rep, _ = run_audit(self.db, self.policy)
                    reports.append(rep)
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

        t = threading.Thread(target=audit_loop, daemon=True)
        t.start()
        acks = []
        for i, payload in enumerate((fx.MSG1, fx.MSG2, fx.MSG_DUP_MERGE)):
            ack = send_message("127.0.0.1", self.server.port, payload)
            acks.append(ack.code)
        t.join(timeout=20)

        self.assertEqual(errors, [])
        self.assertEqual(acks, ["AA", "AA", "AA"])
        self.assertTrue(reports)

        # 3. every intermediate report is internally consistent with its
        #    own snapshot; none references rows beyond its rowid boundary
        for rep in reports:
            self.assertLessEqual(
                rep["snapshot"]["message_count"], 11,
            )
            self._report_matches_its_snapshot(rep)

        # 4. a fresh audit after all commits sees the final warehouse state,
        #    including the three newly committed messages
        report_after, _ = run_audit(self.db, self.policy)
        self.assertEqual(report_after["snapshot"]["message_count"], 11)
        # byte-stability of the final state
        report_after2, _ = run_audit(self.db, self.policy)
        self.assertEqual(
            json.dumps(report_after, sort_keys=True),
            json.dumps(report_after2, sort_keys=True),
        )

    def _report_matches_its_snapshot(self, report: dict) -> None:
        import sqlite3
        conn = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE rowid <= ?",
                (report["snapshot"]["message_max_rowid"],),
            ).fetchone()[0]
            self.assertEqual(n, report["snapshot"]["message_count"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
