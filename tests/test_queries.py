"""Tests for the queries API layer (library interface)."""

from __future__ import annotations

import os
import tempfile
import unittest

from hl7recv import queries
from hl7recv.builder import build_message
from hl7recv.store import Store
from tests import fixtures as fx


class TestQueriesApi(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "q.db"))
        for i, payload in enumerate((fx.MSG1, fx.MSG2), start=1):
            attempt = self.store.begin_attempt(1, i, payload)
            self.store.commit_message(attempt, build_message(payload), payload)
            self.store.mark_acked(attempt)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_list_messages(self):
        msgs = queries.list_messages(self.store)
        self.assertEqual([m["control_id"] for m in msgs], ["MSG00001", "MSG00002"])
        self.assertEqual(msgs[0]["n_results"], 4)
        self.assertEqual(msgs[1]["version"], "2.3.1")

    def test_message_detail(self):
        detail = queries.message_detail(self.store, "LAB1^HOSP", "MSG00001")
        self.assertIsNotNone(detail)
        self.assertEqual(detail["raw_size"], len(fx.MSG1))
        self.assertNotIn("raw", detail)  # blob replaced by size
        self.assertEqual(len(detail["results"]), 4)
        self.assertEqual(len(detail["notes"]), 2)
        self.assertEqual(detail["patients"][0]["identifier"], "P12345")
        # source coordinates present in the API rows
        r0 = detail["results"][0]
        self.assertEqual((r0["obr_seg"], r0["obx_seg"]), (4, 6))

    def test_message_detail_missing(self):
        self.assertIsNone(queries.message_detail(self.store, "X", "Y"))

    def test_results_selectors(self):
        self.assertEqual(
            len(queries.results_by_message(self.store, "LAB1^HOSP", "MSG00001")), 4
        )
        self.assertEqual(
            len(queries.results_by_patient(self.store, "HOSP", "P12345")), 6
        )
        self.assertEqual(len(queries.results_by_order(self.store, "FILLER100")), 4)

    def test_diagnostics_shape(self):
        diag = queries.diagnostics(self.store)
        self.assertEqual([r["state"] for r in diag["inbox"]], ["acked", "acked"])
        self.assertEqual(diag["frame_errors"], [])
        self.assertNotIn("evidence", diag["inbox"][0])


if __name__ == "__main__":
    unittest.main()
