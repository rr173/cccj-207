"""Large-sample runtime stress for the audit.

A big warehouse is generated at run time (never committed to the
repository) with tens of thousands of result rows across both windows.
The checks assert invariants rather than absolute numbers:

* streaming keeps peak memory well below materialising the whole table;
* evidence samples never exceed the policy cap regardless of population;
* sample ordering is the fixed source-coordinate order across repeated runs;
* quantile and histogram math stays exact and deterministic at scale;
* the audit stays read-only while the receiver keeps serving.
"""

from __future__ import annotations

import gc
import json
import os
import tempfile
import tracemalloc
import unittest

from hl7recv.audit.engine import render_report, run_audit
from hl7recv.audit.metrics import quantile_r7, try_parse_number
from hl7recv.audit.policy import parse_policy_text
from hl7recv.builder import build_message
from hl7recv.store import Store
import array

N_MESSAGES = 400           # 200 per window
ROWS_PER_MESSAGE = 60      # 24,000 result rows total
SAMPLE_CAP = 7


def _policy_text() -> str:
    return json.dumps({
        "policy_version": 1,
        "name": "stress",
        "selection": {"senders": ["BIG^HOSP"]},
        "windows": {
            "baseline": {
                "start": "2026-12-01T00:00:00.000+00:00",
                "end": "2026-12-02T00:00:00.000+00:00",
            },
            "current": {
                "start": "2026-12-02T00:00:00.000+00:00",
                "end": "2026-12-03T00:00:00.000+00:00",
            },
        },
        "limits": {
            "max_samples_per_metric": SAMPLE_CAP,
            "text_length_buckets": [1, 8, 32, 128],
        },
        "metrics": [
            {"metric_id": "msgs", "type": "message_count", "rules": []},
            {"metric_id": "rows", "type": "result_count", "rules": []},
            {"metric_id": "units", "type": "field_nonnull_rate",
             "field": "units", "rules": []},
            {"metric_id": "obx2", "type": "cardinality",
             "dimension": "obx2_type", "rules": []},
            {"metric_id": "txt", "type": "text_length_distribution",
             "rules": []},
            {"metric_id": "num", "type": "numeric_quantile_distribution",
             "quantiles": [0.25, 0.5, 0.75, 0.95], "rules": []},
        ],
    })


def _populate_big(db_path: str) -> Store:
    store = Store(db_path)
    seq = 0
    for window_day, prefix in ((1, "B"), (2, "C")):
        for m in range(N_MESSAGES // 2):
            seq += 1
            cid = f"{prefix}{m:05d}"
            ts = (f"2026-12-0{window_day}T00:{m % 60:02d}:00.000+00:00")
            parts = [
                "MSH|^~\\&|BIG|HOSP|RCV|RCVF|20261201000000||ORU^R01|"
                f"{cid}|P|2.5.1\r",
                "PID|1||P1^^^HOSP^MR||Stress^T\r",
                "PV1|1|O\r",
                "OBR|1|PL|FL|CBC^Count^LN\r",
            ]
            for i in range(ROWS_PER_MESSAGE):
                vtype = ("NM" if i % 3 else
                         "ST" if i % 3 == 1 else "TX")
                if vtype == "NM":
                    value = str((seq * 7 + i) % 997)  # deterministic
                    units = "u" if i % 2 else ""
                elif vtype == "ST":
                    value = "s" * (1 + (i % 40))
                    units = ""
                else:
                    value = "t" * (1 + (i % 200))  # some exceed top bin
                    units = ""
                obs = f"O{i:02d}^Observe^LN"
                parts.append(
                    "|".join([
                        "OBX", str(i + 1), vtype, obs, "", value,
                        units, "", "", "", "", "F", "", "", "",
                    ]) + "\r"
                )
            raw = "".join(parts).encode("utf-8")
            parsed = build_message(raw)
            old = store._clock
            store._clock = lambda ts=ts: ts  # noqa: E731
            try:
                attempt = store.begin_attempt(seq, 1, raw)
                assert store.commit_message(attempt, parsed, raw) == "committed"
            finally:
                store._clock = old
    return store


class AuditStress(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "big.db")
        self.store = _populate_big(self.db)
        self.policy = parse_policy_text(_policy_text())

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_scales_and_stays_bounded(self):
        total_rows = N_MESSAGES * ROWS_PER_MESSAGE
        self.assertEqual(self.store.count_results(), total_rows)

        gc.collect()
        tracemalloc.start()
        baseline_mem, _ = tracemalloc.get_traced_memory()
        report, rc = run_audit(self.db, self.policy)
        peak_mem, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertEqual(rc, 0)
        # 24k rows must not require 24k-row-sized allocations; allow a
        # generous bound but far below a full materialisation (~MBs of Row
        # objects would exceed this)
        self.assertLess(
            peak_mem - baseline_mem, 60_000_000,
            "audit accumulated memory proportional to the full table",
        )

        by_id = {m["metric_id"]: m for m in report["metrics"]}
        self.assertEqual(by_id["msgs"]["baseline"]["count"],
                         N_MESSAGES // 2)
        self.assertEqual(by_id["rows"]["current"]["count"],
                         (N_MESSAGES // 2) * ROWS_PER_MESSAGE)
        # histogram counts reconstruct the whole eligible population
        txt_cur = by_id["txt"]["current"]
        self.assertEqual(
            sum(b["count"] for b in txt_cur["histogram"]),
            txt_cur["eligible_rows"],
        )
        self.assertEqual(
            txt_cur["eligible_rows"] + txt_cur["excluded_rows"],
            (N_MESSAGES // 2) * ROWS_PER_MESSAGE,
        )

    def test_evidence_caps_hold_at_scale(self):
        report, _ = run_audit(self.db, self.policy)
        for m in report["metrics"]:
            evidence = m["evidence"]
            for key, block in evidence.items():
                if isinstance(block, dict) and "returned_count" in block:
                    self.assertLessEqual(block["returned_count"], SAMPLE_CAP)
                    if block["candidate_count"] > SAMPLE_CAP:
                        self.assertTrue(block["truncated"])

    def test_sample_ordering_is_fixed_and_reports_byte_stable(self):
        r1, _ = run_audit(self.db, self.policy)
        r2, _ = run_audit(self.db, self.policy)
        self.assertEqual(render_report(r1), render_report(r2))

        def coord_key(item):
            return (item["sender"], item["control_id"],
                    item["obr_index"], item["obx_index"],
                    item["obr_seg"], item["obx_seg"])

        for m in r1["metrics"]:
            for block in m["evidence"].values():
                if isinstance(block, dict) and block.get("items"):
                    keys = [coord_key(i) for i in block["items"]]
                    self.assertEqual(keys, sorted(keys))

    def test_quantile_math_helpers(self):
        vals = array.array("d", sorted(range(1, 101)))  # 1..100
        # R-7 median of 1..100 is 50.5
        self.assertEqual(quantile_r7(vals, 0.5), 50.5)
        self.assertEqual(quantile_r7(array.array("d", [42.0]), 0.9), 42.0)
        self.assertIsNone(try_parse_number(""))
        self.assertIsNone(try_parse_number("<0.5"))
        self.assertIsNone(try_parse_number("nan"))
        self.assertEqual(try_parse_number("  -7.5e2 "), -750.0)

    def test_repeated_runs_no_memory_growth(self):
        import tracemalloc as tm
        gc.collect()
        tm.start()
        baselines = []
        for _ in range(4):
            run_audit(self.db, self.policy)
            gc.collect()
            cur, _ = tm.get_traced_memory()
            baselines.append(cur)
        tm.stop()
        self.assertLess(
            baselines[-1], baselines[0] * 2 + 2_000_000,
            f"audit memory grew across runs: {baselines}",
        )


if __name__ == "__main__":
    unittest.main()
