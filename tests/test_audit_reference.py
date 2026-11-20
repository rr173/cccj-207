"""Integration tests for the independent audit reference checker.

The reference implementation imports no product audit code.  These tests
run it as a real subprocess against the product report and additionally
tamper with the report to prove the checker independently detects drift
rather than echoing product outputs.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

from hl7recv.audit.cli import audit_main
from hl7recv.audit.policy import load_policy
from hl7recv.store import Store
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POLICY_PATH = os.path.join(ROOT, "drills", "audit_policy.example.json")


def run_reference(db: str, policy: str, report: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "refcheck.audit_reference",
         "--db", db, "--policy", policy, "--report", report],
        capture_output=True, text=True, cwd=ROOT, timeout=60,
    )


class AuditReferenceCrossCheck(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "drill.db")
        store = Store(self.db)
        ad.populate(store)
        store.close()
        self.report = os.path.join(self.tmp.name, "report.json")
        rc = audit_main([
            "--db", self.db, "--policy", POLICY_PATH,
            "--output", self.report,
        ])
        # the drill is designed to contain failures
        self.assertEqual(rc, 2)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reference_agrees_with_product_on_every_metric(self):
        out = run_reference(self.db, POLICY_PATH, self.report)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("AUDIT REFERENCE CHECK PASSED", out.stdout)

    def test_reference_detects_tampered_count(self):
        with open(self.report, encoding="utf-8") as fh:
            doc = json.load(fh)
        for m in doc["metrics"]:
            if m["metric_id"] == "vol_results":
                m["baseline"]["count"] += 100
        tampered = os.path.join(self.tmp.name, "tampered.json")
        with open(tampered, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        out = run_reference(self.db, POLICY_PATH, tampered)
        self.assertEqual(out.returncode, 1)
        self.assertIn("vol_results", out.stdout)
        self.assertIn("baseline count", out.stdout)

    def test_reference_detects_tampered_severity(self):
        with open(self.report, encoding="utf-8") as fh:
            doc = json.load(fh)
        for m in doc["metrics"]:
            if m["metric_id"] == "card_obx2":
                m["status"] = "ok"
                m["threshold_hits"] = []
        tampered = os.path.join(self.tmp.name, "sev.json")
        with open(tampered, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        out = run_reference(self.db, POLICY_PATH, tampered)
        self.assertEqual(out.returncode, 1)
        self.assertIn("card_obx2", out.stdout)

    def test_reference_detects_tampered_quantile(self):
        with open(self.report, encoding="utf-8") as fh:
            doc = json.load(fh)
        for m in doc["metrics"]:
            if m["metric_id"] == "nm_quantiles":
                m["current"]["quantiles"][0]["value"] = 9999.0
        tampered = os.path.join(self.tmp.name, "q.json")
        with open(tampered, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        out = run_reference(self.db, POLICY_PATH, tampered)
        self.assertEqual(out.returncode, 1)
        self.assertIn("nm_quantiles", out.stdout)

    def test_reference_shares_no_product_audit_code(self):
        with open(
            os.path.join(ROOT, "refcheck", "audit_reference.py"),
            encoding="utf-8",
        ) as fh:
            src = fh.read()
        self.assertNotIn("from hl7recv", src)
        self.assertNotIn("import hl7recv", src)

    def test_reference_recomputes_from_same_snapshot_facts(self):
        # window counts in the report must match an independent plain-SQL
        # count executed with the report's own canonical literals
        import sqlite3
        with open(self.report, encoding="utf-8") as fh:
            doc = json.load(fh)
        conn = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            for name in ("baseline", "current"):
                w = doc["windows"][name]
                n = conn.execute(
                    "SELECT COUNT(*) FROM messages"
                    " WHERE sender IN (?, ?)"
                    " AND received_at >= ? AND received_at < ?",
                    ("LAB1^HOSP", "LAB2^HOSP",
                     w["start_canonical"], w["end_canonical"]),
                ).fetchone()[0]
                self.assertEqual(
                    n, doc["summary"][f"{name}_matching_messages"]
                )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
