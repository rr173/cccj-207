"""Focused unit tests for threshold rules and window edge semantics.

These exercise individual rule families with tiny hand-built databases so
the severity matrix is covered deterministically without relying on the
larger drill policy.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

from hl7recv.audit.cli import audit_main
from hl7recv.audit.engine import (
    EXIT_INCONCLUSIVE,
    EXIT_OK,
    EXIT_WARNING,
    run_audit,
)
from hl7recv.audit.policy import AuditError, parse_policy_text
from hl7recv.audit.timebounds import canonical_bound
from hl7recv.builder import build_message
from hl7recv.store import Store
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WINDOWS = {
    "baseline": {"start": "2026-11-10T10:00:00.000+00:00",
                 "end": "2026-11-10T11:00:00.000+00:00"},
    "current": {"start": "2026-11-10T11:00:00.000+00:00",
                "end": "2026-11-10T12:00:00.000+00:00"},
}


def _commit(store: Store, cid: str, ts: str, rows: list[ad.DrillRow],
            sender: str = ad.SENDER_A) -> None:
    raw = ad.build_oru_message(sender, cid, rows)
    parsed = build_message(raw)
    old = store._clock
    store._clock = lambda ts=ts: ts  # noqa: E731
    try:
        attempt = store.begin_attempt(1, 1, raw)
        assert store.commit_message(attempt, parsed, raw) == "committed"
    finally:
        store._clock = old


def _policy(metrics: list[dict], *, senders=None, extra: dict | None = None):
    doc = {
        "policy_version": 1,
        "selection": {"senders": senders or [ad.SENDER_A]},
        "windows": WINDOWS,
        "metrics": metrics,
    }
    if extra:
        doc.update(extra)
    return parse_policy_text(json.dumps(doc))


class ThresholdRuleCases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "rules.db")
        self.store = Store(self.db)
        # baseline: four well-populated rows; current: two rows
        _commit(self.store, "B1", "2026-11-10T10:05:00.000+00:00", [
            ad.DrillRow("NM", "WBC", "7.5", units="u", observed_at="t"),
            ad.DrillRow("NM", "RBC", "4.2", units="u", observed_at="t"),
            ad.DrillRow("ST", "N1", "abcdefgh", units=None, observed_at="t"),
            ad.DrillRow("ST", "N2", "ijklmnop", units=None, observed_at="t"),
        ])
        _commit(self.store, "C1", "2026-11-10T11:05:00.000+00:00", [
            ad.DrillRow("NM", "WBC", "8.1", units="u", observed_at=None),
            ad.DrillRow("ST", "N1", "q", units=None, observed_at=None),
        ])

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_count_ratio_rule(self):
        pol = _policy([{
            "metric_id": "r", "type": "result_count",
            "rules": [{"rule": "current_to_baseline_ratio_below",
                       "severity": "warning", "parameter": 0.9}],
        }])
        report, rc = run_audit(self.db, pol)
        self.assertEqual(rc, EXIT_WARNING)
        hit = report["metrics"][0]["threshold_hits"][0]
        self.assertEqual(hit["evidence"]["ratio"], 0.5)

    def test_rate_below_is_current_window_only(self):
        pol = _policy([{
            "metric_id": "f", "type": "field_nonnull_rate", "field": "units",
            "rules": [{"rule": "nonnull_rate_below",
                       "severity": "failure", "parameter": 0.6}],
        }])
        report, rc = run_audit(self.db, pol)
        # current units: 1/2 = 0.5 < 0.6 -> failure even though baseline
        # was healthy (2/4)
        self.assertEqual(rc, 2)
        self.assertEqual(
            report["metrics"][0]["threshold_hits"][0]["rule"],
            "nonnull_rate_below",
        )

    def test_rate_drop_rule_uses_window_difference(self):
        pol = _policy([{
            "metric_id": "f", "type": "field_nonnull_rate",
            "field": "observed_at",
            "rules": [{"rule": "nonnull_rate_drop_gte",
                       "severity": "warning", "parameter": 0.4}],
        }])
        report, rc = run_audit(self.db, pol)
        # baseline 4/4 = 1.0 -> current 0/2 = 0.0: drop 1.0 >= 0.4
        self.assertEqual(rc, EXIT_WARNING)
        ev = report["metrics"][0]["threshold_hits"][0]["evidence"]
        self.assertEqual(ev["rate_drop"], 1.0)

    def test_cardinality_ratio_and_drop(self):
        pol = _policy([{
            "metric_id": "c", "type": "cardinality", "dimension": "obx2_type",
            "rules": [
                {"rule": "cardinality_ratio_below", "severity": "warning",
                 "parameter": 0.99},
            ],
        }])
        report, rc = run_audit(self.db, pol)
        # baseline {NM, ST}=2, current {NM, ST}=2 -> ratio 1.0, no fire
        self.assertEqual(rc, EXIT_OK)

    def test_max_length_rule_fires_on_current_only(self):
        pol = _policy([{
            "metric_id": "t", "type": "text_length_distribution",
            "rules": [{"rule": "max_length_gte", "severity": "failure",
                       "parameter": 4}],
        }])
        report, rc = run_audit(self.db, pol)
        # current max length is 1 (< 4): no fire despite baseline length 8
        self.assertEqual(rc, EXIT_OK)

    def test_fraction_over_length_rule(self):
        pol = _policy([{
            "metric_id": "t", "type": "text_length_distribution",
            "rules": [{"rule": "fraction_over_length_gte",
                       "severity": "warning", "parameter": 0.5}],
        }], extra={"limits": {"text_length_buckets": [1, 2, 4]}})
        report, rc = run_audit(self.db, pol)
        # current text rows: one value 'q' (len1); zero rows over top edge 4
        self.assertEqual(rc, EXIT_OK)
        hist = report["metrics"][0]["current"]["histogram"]
        self.assertEqual(hist[-1]["count"], 0)

    def test_min_sample_guard_blocks_other_rules(self):
        pol = _policy([{
            "metric_id": "r", "type": "result_count",
            "rules": [
                {"rule": "min_sample_size", "severity": "warning",
                 "parameter": 100},
                {"rule": "count_drop_gte", "severity": "failure",
                 "parameter": 1},
            ],
        }])
        report, rc = run_audit(self.db, pol)
        self.assertEqual(rc, EXIT_INCONCLUSIVE)
        m = report["metrics"][0]
        self.assertEqual(m["status"], "not_computable")
        self.assertEqual(m["threshold_hits"], [])


class WindowSemantics(unittest.TestCase):
    def test_canonical_bound_accepts_z_and_offsets(self):
        self.assertEqual(
            canonical_bound("2026-11-10T10:00:00Z", which="start"),
            "2026-11-10T10:00:00.000+00:00",
        )
        self.assertEqual(
            canonical_bound("2026-11-10T12:00:00+02:00", which="end"),
            "2026-11-10T10:00:00.000+00:00",
        )

    def test_sub_millisecond_bounds_truncate_not_round(self):
        # 123999 microseconds must truncate to .123, never round up to .124
        self.assertEqual(
            canonical_bound("2026-11-10T10:00:00.123999+00:00", which="x"),
            "2026-11-10T10:00:00.123+00:00",
        )
        self.assertEqual(
            canonical_bound("2026-11-10T10:00:00.123500Z", which="x"),
            "2026-11-10T10:00:00.123+00:00",
        )

    def test_naive_and_rejected_bounds(self):
        with self.assertRaises(AuditError):
            canonical_bound("2026-11-10T10:00:00", which="start")

    def test_start_not_before_end(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = os.path.join(tmp.name, "w.db")
        store = Store(db)
        _commit(store, "B1", "2026-11-10T10:05:00.000+00:00",
                [ad.DrillRow("NM", "WBC", "1", observed_at="t")])
        store.close()
        doc = {
            "policy_version": 1,
            "selection": {"senders": [ad.SENDER_A]},
            "windows": {
                "baseline": {"start": "2026-11-10T11:00:00+00:00",
                             "end": "2026-11-10T10:00:00+00:00"},
                "current": {"start": "2026-11-10T11:00:00+00:00",
                            "end": "2026-11-10T12:00:00+00:00"},
            },
            "metrics": [{"metric_id": "m", "type": "message_count",
                         "rules": []}],
        }
        path = os.path.join(tmp.name, "p.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        with self.assertRaises(AuditError):
            run_audit(db, parse_policy_text(open(path, encoding="utf-8").read()))

    def test_inconclusive_exit_code_via_subprocess(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = os.path.join(tmp.name, "w.db")
        store = Store(db)
        _commit(store, "B1", "2026-11-10T10:05:00.000+00:00",
                [ad.DrillRow("NM", "WBC", "1", observed_at="t")])
        store.close()
        doc = {
            "policy_version": 1,
            "selection": {"senders": [ad.SENDER_A]},
            "windows": {
                "baseline": {"start": "2020-01-01T00:00:00+00:00",
                             "end": "2020-01-02T00:00:00+00:00"},
                "current": {"start": "2020-01-02T00:00:00+00:00",
                            "end": "2020-01-03T00:00:00+00:00"},
            },
            "metrics": [
                {"metric_id": "m", "type": "message_count", "rules": []},
                {"metric_id": "f", "type": "field_nonnull_rate",
                 "field": "units", "rules": []},
            ],
        }
        path = os.path.join(tmp.name, "p.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        proc = subprocess.run(
            [sys.executable, "-m", "hl7recv", "audit",
             "--db", db, "--policy", path],
            capture_output=True, text=True, cwd=ROOT, timeout=30,
        )
        self.assertEqual(proc.returncode, EXIT_INCONCLUSIVE)
        report = json.loads(proc.stdout)
        self.assertEqual(report["report_status"], "not_computable")
        by_id = {m["metric_id"]: m for m in report["metrics"]}
        self.assertEqual(by_id["m"]["status"], "ok")  # count zero is computable
        self.assertEqual(by_id["f"]["status"], "not_computable")
        self.assertEqual(proc.stderr, "")

    def test_stdout_dash_means_stdout(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = os.path.join(tmp.name, "w.db")
        store = Store(db)
        _commit(store, "B1", "2026-11-10T10:05:00.000+00:00",
                [ad.DrillRow("NM", "WBC", "1", observed_at="t")])
        store.close()
        pol_path = os.path.join(ROOT, "drills", "audit_policy.example.json")
        # '-' is an explicit stdout marker: the call returns a normal audit
        # exit code and never attempts to create a file named '-'
        dash_file = os.path.join(os.getcwd(), "-")
        existed = os.path.exists(dash_file)
        rc = audit_main(["--db", db, "--policy", pol_path, "--output", "-"])
        self.assertIn(rc, (1, 2, 3))
        if not existed:
            self.assertFalse(os.path.exists(dash_file))


if __name__ == "__main__":
    unittest.main()
