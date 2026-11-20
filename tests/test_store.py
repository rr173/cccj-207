"""Unit tests for the SQLite store: commit semantics, dedup, recovery."""

from __future__ import annotations

import os
import tempfile
import unittest

from hl7recv.builder import build_message
from hl7recv.store import (
    OUT_COMMITTED,
    OUT_CONFLICT,
    OUT_DUPLICATE,
    ST_ACK_LOST,
    ST_COMMITTED,
    ST_CONFLICT,
    ST_DUPLICATE,
    ST_RECOVERED_INCOMPLETE,
    Store,
)
from tests import fixtures as fx


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def commit(self, raw: bytes, conn_id: int = 1, frame_no: int = 1) -> str:
        attempt = self.store.begin_attempt(conn_id, frame_no, raw)
        msg = build_message(raw)
        return self.store.commit_message(attempt, msg, raw)


class TestCommitDedup(StoreTestBase):
    def test_commit_msg1(self):
        out = self.commit(fx.MSG1)
        self.assertEqual(out, OUT_COMMITTED)
        self.assertEqual(self.store.count_results(), 4)
        rows = self.store.results_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            [(r["obr_index"], r["obx_index"]) for r in rows],
            [(1, 1), (1, 2), (2, 1), (2, 2)],
        )

    def test_identical_resend_is_duplicate(self):
        self.commit(fx.MSG1)
        out = self.commit(fx.MSG1, frame_no=2)
        self.assertEqual(out, OUT_DUPLICATE)
        self.assertEqual(self.store.count_results(), 4)
        self.assertEqual(len(self.store.list_messages()), 1)

    def test_conflict_does_not_overwrite(self):
        self.commit(fx.MSG1)
        out = self.commit(fx.MSG1_CONFLICT, frame_no=2)
        self.assertEqual(out, OUT_CONFLICT)
        self.assertEqual(self.store.count_results(), 4)
        row = self.store.results_for_message("LAB1^HOSP", "MSG00001")[0]
        self.assertEqual(row["value_text"], "7.5")  # original value kept

    def test_same_business_fields_different_control_id_ok(self):
        self.commit(fx.MSG1)
        out = self.commit(fx.MSG2)
        self.assertEqual(out, OUT_COMMITTED)
        self.assertEqual(len(self.store.list_messages()), 2)
        # same patient + same order, but two distinct messages
        rows = self.store.results_for_patient("HOSP", "P12345")
        self.assertEqual(len(rows), 6)
        orders = self.store.results_for_order("FILLER100")
        self.assertEqual(len(orders), 4)  # MSG1 OBR1 x2 + MSG2 x2

    def test_sender_scoped_dedup(self):
        # same control id from a different sender is a different message
        other = fx.MSG1.replace(b"LAB1|HOSP", b"LAB2|HOSP")
        self.commit(fx.MSG1)
        out = self.commit(other)
        self.assertEqual(out, OUT_COMMITTED)
        self.assertEqual(len(self.store.list_messages()), 2)

    def test_attempt_states(self):
        self.commit(fx.MSG1)
        self.commit(fx.MSG1, frame_no=2)
        self.commit(fx.MSG1_CONFLICT, frame_no=3)
        diag = self.store.diagnostics()["inbox"]
        states = [r["state"] for r in diag]
        self.assertEqual(states, [ST_COMMITTED, ST_DUPLICATE, ST_CONFLICT])
        conflict = diag[2]
        self.assertIn("identity conflict", conflict["detail"])
        self.assertEqual(bytes(conflict["evidence"]), fx.MSG1_CONFLICT)

    def test_mark_acked(self):
        attempt = self.store.begin_attempt(1, 1, fx.MSG1)
        msg = build_message(fx.MSG1)
        self.store.commit_message(attempt, msg, fx.MSG1)
        self.store.mark_acked(attempt)
        diag = self.store.diagnostics()["inbox"]
        self.assertEqual(diag[0]["state"], "acked")


class TestQueries(StoreTestBase):
    def setUp(self):
        super().setUp()
        self.commit(fx.MSG1)
        self.commit(fx.MSG2)

    def test_results_for_message_coordinates(self):
        rows = self.store.results_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(
            [(r["obr_seg"], r["obx_seg"]) for r in rows],
            [(4, 6), (4, 7), (9, 10), (9, 11)],
        )

    def test_results_for_patient_uses_authority_key(self):
        # OTHER/P999 only exists in MSG_REPEAT_PID3
        self.commit(fx.MSG_REPEAT_PID3)
        rows = self.store.results_for_patient("OTHER", "P999")
        self.assertEqual(len(rows), 1)
        rows = self.store.results_for_patient("HOSP", "P12345")
        self.assertEqual(len(rows), 7)

    def test_results_for_order_matches_placer_or_filler(self):
        self.assertEqual(len(self.store.results_for_order("PLACER200")), 2)
        self.assertEqual(len(self.store.results_for_order("FILLER100")), 4)

    def test_notes_stored_with_scope(self):
        notes = self.store.notes_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(len(notes), 2)
        obr_note = [n for n in notes if n["obx_index"] is None][0]
        self.assertEqual(obr_note["obr_index"], 1)
        self.assertEqual(obr_note["text"], "fasting sample for OBR one")
        self.assertEqual(obr_note["seg"], 5)
        obx_note = [n for n in notes if n["obx_index"] == 2][0]
        self.assertEqual(obx_note["text"], "comment belonging to OBX 2")
        self.assertEqual(obx_note["seg"], 8)

    def test_patients_table(self):
        self.commit(fx.MSG_REPEAT_PID3)
        pats = self.store.patients_for_message("LAB1^HOSP", "MSG0000R")
        self.assertEqual(
            [(p["authority"], p["identifier"]) for p in pats],
            [("HOSP", "P12345"), ("OTHER", "P999")],
        )

    def test_empty_vs_missing_in_db(self):
        self.commit(fx.MSG_EMPTY_VS_MISSING)
        row = self.store.results_for_message("LAB1^HOSP", "MSG0000M")[0]
        self.assertEqual(row["ref_range"], "")  # present-but-empty
        self.assertIsNone(row["observed_at"])  # missing

    def test_iter_results_batches(self):
        batches = list(self.store.iter_results(batch=4))
        self.assertEqual([len(b) for b in batches], [4, 2])

    def test_stable_ordering(self):
        rows = self.store.results_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(
            [r["obs_code"] for r in rows], ["WBC", "WBCM", "GLUCM", "GLU"]
        )


class TestRecovery(unittest.TestCase):
    def test_recovery_reclassifies_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")
            store = Store(db)
            # attempt 1: committed but never acked (crash between commit & ACK)
            a1 = store.begin_attempt(1, 1, fx.MSG1)
            store.commit_message(a1, build_message(fx.MSG1), fx.MSG1)
            # attempt 2: received but never processed (crash mid-parse)
            store.begin_attempt(1, 2, fx.MSG2)
            store.close()

            store2 = Store(db)  # reopen -> recovery
            self.assertEqual(store2.recovered_ack_lost, 1)
            self.assertEqual(store2.recovered_incomplete, 1)
            diag = store2.diagnostics()["inbox"]
            states = [r["state"] for r in diag]
            self.assertEqual(states, [ST_ACK_LOST, ST_RECOVERED_INCOMPLETE])
            # committed message survived the crash
            self.assertEqual(store2.count_results(), 4)
            # resend of the committed message resolves as duplicate
            a3 = store2.begin_attempt(2, 1, fx.MSG1)
            out = store2.commit_message(a3, build_message(fx.MSG1), fx.MSG1)
            self.assertEqual(out, OUT_DUPLICATE)
            self.assertEqual(store2.count_results(), 4)
            store2.close()

    def test_db_reopen_keeps_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")
            store = Store(db)
            store.commit = None  # noqa: keep linters quiet
            a = store.begin_attempt(1, 1, fx.MSG1)
            store.commit_message(a, build_message(fx.MSG1), fx.MSG1)
            store.close()
            store2 = Store(db)
            self.assertEqual(store2.count_results(), 4)
            self.assertEqual(len(store2.list_messages()), 1)
            store2.close()


if __name__ == "__main__":
    unittest.main()
