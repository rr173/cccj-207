"""Hand-derived audit drill over the synthetic two-window dataset.

Every expected number below is derived by hand from the row catalogue in
``drills/audit_drill.py`` -- not read from product code.  Coverage:

* half-open boundary messages (B1 on baseline start, C1 on current start,
  EDGE_OUT on current end, OUT1 before baseline);
* multiple senders, test codes and OBX-2 types in one window;
* NULL vs empty-string optional fields;
* text and numeric values, unparseable NM text;
* low sample size, threshold breaches and metrics with no computable sample;
* rejected inbox records never counted as facts;
* every anomaly carries traceable source coordinates;
* deterministic byte-stable reports.
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
    EXIT_FAILURE,
    EXIT_INCONCLUSIVE,
    EXIT_OK,
    EXIT_USAGE,
    EXIT_WARNING,
    render_report,
    run_audit,
)
from hl7recv.audit.policy import AuditError, load_policy
from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import send_message
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POLICY_PATH = os.path.join(ROOT, "drills", "audit_policy.example.json")


def _load_policy_doc() -> dict:
    with open(POLICY_PATH, encoding="utf-8") as fh:
        return json.load(fh)


class DrillDB:
    def __init__(self, tmpdir: str) -> None:
        self.db = os.path.join(tmpdir, "drill.db")
        self.store = Store(self.db)
        self.catalogue = ad.populate(self.store)

    def close(self) -> None:
        self.store.close()


class HandDerivedDrill(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.drill = DrillDB(self.tmp.name)
        self.policy = load_policy(POLICY_PATH)

    def tearDown(self):
        self.drill.close()
        self.tmp.cleanup()

    def _run(self):
        report, rc = run_audit(self.drill.db, self.policy)
        return report, rc

    def _metric(self, report, mid):
        blocks = {m["metric_id"]: m for m in report["metrics"]}
        return blocks[mid]

    # -- hand-derived catalogue facts ------------------------------------
    def test_window_boundaries_half_open(self):
        report, _ = self._run()
        # baseline: B1(10:00 incl), B2, B3 ; current: C1(11:00 incl), C2, C3
        # OUT1 (09:00) and EDGE_OUT (12:00 == current end) excluded everywhere
        self.assertEqual(report["summary"]["baseline_matching_messages"], 3)
        self.assertEqual(report["summary"]["current_matching_messages"], 3)
        # all eight committed messages exist in the snapshot descriptor
        self.assertEqual(report["snapshot"]["message_count"], 8)
        self.assertEqual(report["snapshot"]["result_count"], 22)

    def test_volume_metrics_hand_derived(self):
        report, rc = self._run()
        m = self._metric(report, "vol_messages")
        self.assertEqual(m["baseline"]["count"], 3)
        self.assertEqual(m["current"]["count"], 3)
        self.assertEqual(m["diff"]["absolute_diff"], 0)
        self.assertEqual(m["status"], "ok")

        r = self._metric(report, "vol_results")
        # B1=7 B2=2 B3=2 -> 11 ; C1=2 C2=4 C3=3 -> 9
        self.assertEqual(r["baseline"]["count"], 11)
        self.assertEqual(r["current"]["count"], 9)
        self.assertEqual(r["diff"]["absolute_diff"], -2)
        self.assertEqual(r["status"], "warning")
        hit = r["threshold_hits"][0]
        self.assertEqual(hit["rule"], "count_drop_gte")
        self.assertEqual(hit["evidence"]["absolute_drop"], 2)

    def test_nonnull_metrics_hand_derived(self):
        report, _ = self._run()
        units = self._metric(report, "nn_units")
        # baseline: only B1 row1 WBC and B2 row1 GLU carry units -> 2/11
        self.assertEqual(units["baseline"]["eligible_rows"], 11)
        self.assertEqual(units["baseline"]["nonnull_rows"], 2)
        self.assertEqual(units["baseline"]["nonnull_rate"],
                         round(2 / 11, 6))
        # current: only C1 WBC has units -> 1/9
        self.assertEqual(units["current"]["nonnull_rows"], 1)
        self.assertEqual(units["current"]["nonnull_rate"], round(1 / 9, 6))
        # min_sample_size=10 guard trips on 9 current rows -> not_computable
        self.assertEqual(units["status"], "not_computable")
        self.assertTrue(units["not_computable_reasons"])
        self.assertEqual(units["threshold_hits"], [])
        # missing samples are bounded and traceable (evidence lives at the
        # metric level, regardless of not_computable status)
        samples = units["evidence"]["missing_value_samples"]
        self.assertLessEqual(samples["returned_count"], 5)
        self.assertEqual(samples["candidate_count"], 8)
        self.assertTrue(samples["truncated"])
        coord = samples["items"][0]
        self.assertEqual(
            sorted(coord),
            sorted(["sender", "control_id", "obr_index", "obx_index",
                    "obr_seg", "obx_seg", "field", "stored_value", "reason"]),
        )

        obs = self._metric(report, "nn_observed_at")
        # baseline rows with observed_at: B1 all 7; B2/B3 none -> 7/11
        self.assertEqual(obs["baseline"]["nonnull_rows"], 7)
        self.assertEqual(obs["baseline"]["nonnull_rate"], round(7 / 11, 6))
        # current: only C1's two rows -> 2/9
        self.assertEqual(obs["current"]["nonnull_rows"], 2)
        self.assertEqual(obs["current"]["nonnull_rate"], round(2 / 9, 6))
        self.assertEqual(obs["status"], "warning")
        self.assertEqual(
            obs["threshold_hits"][0]["rule"], "nonnull_rate_below"
        )

    def test_all_null_column_is_zero_rate_not_missing_metric(self):
        report, _ = self._run()
        visit = self._metric(report, "nn_visit_id")
        # the drill never populates visit_id -> rate 0 in both windows,
        # still computable
        self.assertEqual(visit["baseline"]["nonnull_rate"], 0.0)
        self.assertEqual(visit["current"]["nonnull_rate"], 0.0)
        self.assertEqual(visit["status"], "ok")
        self.assertEqual(visit["not_computable_reasons"], [])

    def test_cardinality_hand_derived(self):
        report, _ = self._run()
        sender = self._metric(report, "card_sender")
        self.assertEqual(sender["baseline"]["cardinality"], 2)
        self.assertEqual(sender["current"]["cardinality"], 2)
        # row counts: baseline A=9 (B1 7 + B3 2), B=2; current A=5, B=4
        base_counts = {v["value"]: v["row_count"]
                       for v in sender["baseline"]["values"]}
        self.assertEqual(base_counts, {"LAB1^HOSP": 9, "LAB2^HOSP": 2})

        obx2 = self._metric(report, "card_obx2")
        # baseline types: NM ST TX FT CWE CE = 6
        self.assertEqual(
            sorted(v["value"] for v in obx2["baseline"]["values"]),
            ["CE", "CWE", "FT", "NM", "ST", "TX"],
        )
        # current types: NM ST TX CWE = 4 (FT and CE disappeared)
        self.assertEqual(
            sorted(v["value"] for v in obx2["current"]["values"]),
            ["CWE", "NM", "ST", "TX"],
        )
        self.assertEqual(obx2["diff"]["absolute_diff"], -2)
        self.assertEqual(obx2["status"], "failure")

    def test_text_length_distribution_hand_derived(self):
        report, _ = self._run()
        t = self._metric(report, "text_lengths")
        # baseline text-like: NOTE len17, MEMO len5, FT1 len2, CMT len29 -> 4
        self.assertEqual(t["baseline"]["eligible_rows"], 4)
        self.assertEqual(t["baseline"]["excluded_rows"], 7)
        self.assertEqual(t["baseline"]["max_length"], 29)
        # current text-like: NOTE x len1, CMT 200, MEMO len4,
        # NOTE 'flagged for review' len19 -> 4
        self.assertEqual(t["current"]["eligible_rows"], 4)
        self.assertEqual(t["current"]["excluded_rows"], 5)
        self.assertEqual(t["current"]["max_length"], 200)
        # min_sample_size=5 guard -> not_computable even though thresholds
        # would otherwise fire
        self.assertEqual(t["status"], "not_computable")
        # but raw histogram/evidence remain available for inspection
        hist = t["current"]["histogram"]
        self.assertEqual(sum(b["count"] for b in hist), 4)
        self.assertEqual(hist[-1]["count"], 1)  # the 200-char value
        long_items = t["evidence"]["long_value_samples"]["items"]
        self.assertEqual(long_items[0]["length"], 200)
        self.assertEqual(long_items[0]["control_id"], "C2")

    def test_numeric_quantiles_hand_derived(self):
        report, _ = self._run()
        q = self._metric(report, "nm_quantiles")
        # baseline NM values: 7.5, 4.2, 130, 5.5, 250 (all parseable)
        self.assertEqual(q["baseline"]["eligible_rows"], 5)
        self.assertEqual(q["baseline"]["parseable_rows"], 5)
        self.assertEqual(q["baseline"]["unparseable_rows"], 0)
        values = [4.2, 5.5, 7.5, 130.0, 250.0]
        # R-7 median is the middle element; R-7 p90 = v3 + 0.6*(v4-v3)
        self.assertEqual(q["baseline"]["quantiles"][0]["value"], 7.5)
        self.assertEqual(
            q["baseline"]["quantiles"][1]["value"],
            round(130 + 0.6 * (250 - 130), 6),
        )
        # current NM: N/A, 18.9, 9.75, pending -> 4 eligible, 2 parseable,
        # 2 unparseable (fraction 0.5)
        self.assertEqual(q["current"]["eligible_rows"], 4)
        self.assertEqual(q["current"]["parseable_rows"], 2)
        self.assertEqual(q["current"]["unparseable_rows"], 2)
        self.assertEqual(q["current"]["unparseable_fraction"], 0.5)
        med = (9.75 + 18.9) / 2
        self.assertEqual(q["current"]["quantiles"][0]["value"], round(med, 6))
        self.assertEqual(q["status"], "failure")
        rules = {h["rule"]: h["severity"] for h in q["threshold_hits"]}
        self.assertEqual(rules["unparseable_fraction_gte"], "failure")
        self.assertEqual(rules["quantile_shift_gte"], "warning")
        # unparseable evidence keeps the ORIGINAL stored text, coordinates
        samples = q["evidence"]["unparseable_samples"]["items"]
        texts = sorted(s["stored_value_text"] for s in samples)
        self.assertEqual(texts, ["N/A", "pending"])
        for s in samples:
            self.assertEqual(s["sender"], "LAB1^HOSP")
            self.assertIn(s["control_id"], ("C1", "C3"))

    def test_overall_status_and_exit_code(self):
        report, rc = self._run()
        self.assertEqual(rc, EXIT_FAILURE)
        self.assertEqual(report["report_status"], "failure")
        self.assertEqual(report["exit_code"], EXIT_FAILURE)
        counts = report["summary"]
        self.assertEqual(counts["metrics_total"], 10)
        self.assertEqual(counts["metrics_failure"], 2)
        self.assertEqual(counts["metrics_warning"], 2)
        self.assertEqual(counts["metrics_not_computable"], 2)
        self.assertEqual(counts["metrics_ok"], 4)

    def test_report_is_byte_stable(self):
        r1, _ = self._run()
        r2, _ = self._run()
        self.assertEqual(render_report(r1), render_report(r2))

    def test_scope_and_filter_documentation_present(self):
        report, _ = self._run()
        self.assertIn("inbox", report["query_filters"]["excluded_tables"])
        self.assertIn("committed", report["query_filters"]["message_status_filter"])
        self.assertIn("[start, end)", report["windows"]["baseline"]["inclusivity"])
        self.assertTrue(report["scope_statement"])

    def test_denominators_explicit_everywhere(self):
        report, _ = self._run()
        for m in report["metrics"]:
            for window in ("baseline", "current"):
                block = m[window]
                self.assertIn("eligible_rows" if m["type"] not in
                              ("message_count", "result_count")
                              else "eligible_result_rows", block)
                if m["type"] not in ("message_count", "result_count"):
                    self.assertIn("excluded_rows", block)


class RejectedInboxExcluded(unittest.TestCase):
    """A rejected message must never enter audited facts."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "rej.db")
        self.store = Store(self.db)
        self.server = MllpServer("127.0.0.1", 0, self.store)
        import threading
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        ad.populate(self.store)

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def test_rejected_message_not_counted(self):
        ack = send_message(
            "127.0.0.1", self.server.port, ad.rejected_payload()
        )
        self.assertEqual(ack.code, "AE")  # rejected at the profile layer
        diag = self.store.diagnostics()
        states = [r["state"] for r in diag["inbox"]]
        self.assertIn("rejected", states)

        policy = load_policy(POLICY_PATH)
        report, _ = run_audit(self.db, policy)
        # still exactly the drill facts; the rejected REJ1 added nothing
        self.assertEqual(report["snapshot"]["message_count"], 8)
        self.assertEqual(report["snapshot"]["result_count"], 22)
        self.assertEqual(report["summary"]["current_matching_messages"], 3)


class EmptyWindowAndNoSampleCases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.drill = DrillDB(self.tmp.name)

    def tearDown(self):
        self.drill.close()
        self.tmp.cleanup()

    def _policy(self, doc):
        path = os.path.join(self.tmp.name, "p.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        return load_policy(path)

    def test_empty_window_is_not_computable_not_zero_anomaly(self):
        doc = json.loads(json.dumps(_load_policy_doc()))
        doc["windows"] = {
            "baseline": {"start": "2020-01-01T00:00:00+00:00",
                         "end": "2020-01-02T00:00:00+00:00"},
            "current": {"start": "2020-01-02T00:00:00+00:00",
                        "end": "2020-01-03T00:00:00+00:00"},
        }
        report, rc = run_audit(self.drill.db, self._policy(doc))
        self.assertEqual(rc, EXIT_INCONCLUSIVE)
        self.assertEqual(report["report_status"], "not_computable")
        for m in report["metrics"]:
            if m["type"] in ("message_count", "result_count"):
                # counts are computable zeroes even in an empty window
                self.assertEqual(m["status"], "ok")
                self.assertEqual(m["baseline"]["count"], 0)
                self.assertEqual(m["current"]["count"], 0)
                self.assertEqual(m["not_computable_reasons"], [])
                self.assertIsNotNone(m["diff"])
            else:
                # shape/sample metrics cannot fabricate statistics
                self.assertEqual(m["status"], "not_computable")
                self.assertTrue(m["not_computable_reasons"])
                self.assertEqual(m["threshold_hits"], [])
                self.assertIsNone(m["diff"])

    def test_metric_with_no_applicable_rows_is_not_computable(self):
        # select only a sender whose windows contain no text-like rows in a
        # fresh narrow policy focused on text/numeric metrics
        doc = {
            "policy_version": 1,
            "selection": {"senders": ["LAB2^HOSP"], "test_codes": ["NOPE"]},
            "windows": {
                "baseline": {"start": "2026-11-10T10:00:00+00:00",
                             "end": "2026-11-10T11:00:00+00:00"},
                "current": {"start": "2026-11-10T11:00:00+00:00",
                            "end": "2026-11-10T12:00:00+00:00"},
            },
            "metrics": [
                {"metric_id": "txt", "type": "text_length_distribution",
                 "rules": []},
                {"metric_id": "num", "type": "numeric_quantile_distribution",
                 "rules": []},
                {"metric_id": "cnt", "type": "result_count", "rules": []},
            ],
        }
        report, rc = run_audit(self.drill.db, self._policy(doc))
        self.assertEqual(rc, EXIT_INCONCLUSIVE)
        by_id = {m["metric_id"]: m for m in report["metrics"]}
        self.assertEqual(by_id["txt"]["status"], "not_computable")
        self.assertEqual(by_id["num"]["status"], "not_computable")
        # a zero result count is a legitimate computable zero, not NC
        self.assertEqual(by_id["cnt"]["status"], "ok")
        self.assertEqual(by_id["cnt"]["current"]["count"], 0)

    def test_sender_filter_isolates_one_sender(self):
        doc = _load_policy_doc()
        doc["selection"]["senders"] = ["LAB2^HOSP"]
        doc["metrics"] = [
            {"metric_id": "msgs", "type": "message_count", "rules": []}
        ]
        report, rc = run_audit(self.drill.db, self._policy(doc))
        self.assertEqual(rc, EXIT_OK)
        m = report["metrics"][0]
        self.assertEqual(m["baseline"]["count"], 1)  # B2 only
        self.assertEqual(m["current"]["count"], 1)   # C2 only


class CliSurface(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.drill = DrillDB(self.tmp.name)

    def tearDown(self):
        self.drill.close()
        self.tmp.cleanup()

    def test_stdout_is_pure_json_and_stderr_clean_on_failure(self):
        proc = subprocess.run(
            [sys.executable, "-m", "hl7recv", "audit",
             "--db", self.drill.db, "--policy", POLICY_PATH],
            capture_output=True, text=True, cwd=ROOT, timeout=30,
        )
        self.assertEqual(proc.returncode, EXIT_FAILURE)
        parsed = json.loads(proc.stdout)  # pure parseable JSON
        self.assertEqual(parsed["exit_code"], EXIT_FAILURE)
        self.assertEqual(proc.stderr, "")

    def test_output_file_keeps_stdout_empty(self):
        out = os.path.join(self.tmp.name, "r.json")
        rc = audit_main([
            "--db", self.drill.db, "--policy", POLICY_PATH, "--output", out,
        ])
        self.assertEqual(rc, EXIT_FAILURE)
        with open(out, encoding="utf-8") as fh:
            on_disk = json.load(fh)
        self.assertEqual(on_disk["report_status"], "failure")

    def test_usage_error_exit_four_json_on_stderr(self):
        proc = subprocess.run(
            [sys.executable, "-m", "hl7recv", "audit",
             "--db", self.drill.db,
             "--policy", os.path.join(self.tmp.name, "missing.json")],
            capture_output=True, text=True, cwd=ROOT, timeout=30,
        )
        self.assertEqual(proc.returncode, EXIT_USAGE)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(json.loads(proc.stderr)["error"],
                         "audit_usage_error")

    def test_naive_timestamp_rejected(self):
        bad = os.path.join(self.tmp.name, "naive.json")
        doc = _load_policy_doc()
        doc["windows"]["baseline"]["start"] = "2026-11-10T10:00:00"
        open(bad, "w", encoding="utf-8").write(json.dumps(doc))
        with self.assertRaises(AuditError):
            run_audit(self.drill.db, load_policy(bad))

    def test_ok_exit_code_when_no_anomalies_and_all_computable(self):
        doc = _load_policy_doc()
        doc["metrics"] = [
            {"metric_id": "msgs", "type": "message_count",
             "rules": [{"rule": "count_drop_gte", "severity": "failure",
                        "parameter": 10}]},
        ]
        path = os.path.join(self.tmp.name, "ok.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        report, rc = run_audit(self.drill.db, load_policy(path))
        self.assertEqual(rc, EXIT_OK)
        self.assertEqual(report["report_status"], "ok")

    def test_warning_only_exit_code_one(self):
        doc = _load_policy_doc()
        doc["metrics"] = [
            {"metric_id": "rows", "type": "result_count",
             "rules": [{"rule": "count_drop_gte", "severity": "warning",
                        "parameter": 2}]},
        ]
        path = os.path.join(self.tmp.name, "warn.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh)
        _, rc = run_audit(self.drill.db, load_policy(path))
        self.assertEqual(rc, EXIT_WARNING)


if __name__ == "__main__":
    unittest.main()
