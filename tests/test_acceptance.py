"""Main acceptance suite.

Drives the receiver over real TCP with the local peer and checks, field by
field, the two handwritten ORU messages plus every required edge case:
identical resend, identity conflict, invalid OBX, unknown version,
fragmented frames, multiple frames per read, bad frame followed by a valid
one, custom separators, escapes, repetitions, empty vs missing, both
profile versions, frame size limit and large multi-OBX messages.
"""

from __future__ import annotations

import os
import tempfile
import threading
import unittest

from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import frame_payload, send_message, send_raw
from tests import fixtures as fx


class ServerCase(unittest.TestCase):
    """In-process server on an ephemeral port with a temp database."""

    max_frame = 1 << 20

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "acc.db")
        self.store = Store(self.db)
        self.server = MllpServer("127.0.0.1", 0, self.store, max_frame=self.max_frame)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.port

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def send(self, payload: bytes, **kw):
        return send_message("127.0.0.1", self.port, payload, **kw)


class TestMainAcceptance(ServerCase):
    def test_msg1_field_by_field(self):
        ack = self.send(fx.MSG1)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(ack.control_id, "MSG00001")

        rows = self.store.results_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(len(rows), 4)

        # --- OBR group 1, OBX 1 (NM) -------------------------------------
        r = rows[0]
        self.assertEqual(r["sender"], "LAB1^HOSP")
        self.assertEqual(r["control_id"], "MSG00001")
        self.assertEqual(r["patient_authority"], "HOSP")
        self.assertEqual(r["patient_id"], "P12345")
        self.assertEqual(r["visit_id"], "V2026-0001")
        self.assertEqual(r["placer_order"], "PLACER100")
        self.assertEqual(r["filler_order"], "FILLER100")
        self.assertEqual(r["test_code"], "CBC")
        self.assertEqual(r["test_system"], "LN")
        self.assertEqual(r["obs_code"], "WBC")
        self.assertEqual(r["obs_system"], "LN")
        self.assertEqual(r["sub_id"], "1")
        self.assertEqual(r["value_type"], "NM")
        self.assertEqual(r["value_text"], "7.5")  # text, not float
        self.assertIsInstance(r["value_text"], str)
        self.assertEqual(r["units"], "10*9/L")
        self.assertEqual(r["ref_range"], "4.0-10.0")
        self.assertEqual(r["abnormal"], "N")
        self.assertEqual(r["rstatus"], "F")
        self.assertEqual(r["observed_at"], "20261101081000")
        self.assertEqual(r["reported_at"], "20261101082900")
        # source coordinates: OBR is segment 4, first OBX segment 6
        self.assertEqual((r["obr_index"], r["obx_index"]), (1, 1))
        self.assertEqual((r["obr_seg"], r["obx_seg"]), (4, 6))

        # --- OBR group 1, OBX 2 (ST) with its NTE -------------------------
        r = rows[1]
        self.assertEqual(r["value_type"], "ST")
        self.assertEqual(r["value_text"], "No abnormal cells seen")
        self.assertEqual((r["obr_seg"], r["obx_seg"]), (4, 7))
        notes = self.store.notes_for_message("LAB1^HOSP", "MSG00001")
        obx_note = [n for n in notes if n["obx_index"] == 2 and n["obr_index"] == 1]
        self.assertEqual(len(obx_note), 1)
        self.assertEqual(obx_note[0]["text"], "comment belonging to OBX 2")
        self.assertEqual(obx_note[0]["seg"], 8)

        # OBR-level NTE (before the first OBX) belongs to the OBR
        obr_note = [n for n in notes if n["obx_index"] is None]
        self.assertEqual(len(obr_note), 1)
        self.assertEqual(obr_note[0]["obr_index"], 1)
        self.assertEqual(obr_note[0]["text"], "fasting sample for OBR one")
        self.assertEqual(obr_note[0]["seg"], 5)

        # --- OBR group 2 ----------------------------------------------------
        r = rows[2]
        self.assertEqual(r["value_type"], "CWE")
        self.assertEqual(r["value_text"], "POCT^Point of Care^HL7OBS")
        self.assertEqual(r["test_code"], "GLUP")
        self.assertEqual((r["obr_index"], r["obx_index"]), (2, 1))
        self.assertEqual((r["obr_seg"], r["obx_seg"]), (9, 10))
        self.assertEqual(r["reported_at"], "20261101082800")

        r = rows[3]
        self.assertEqual(r["value_type"], "NM")
        self.assertEqual(r["value_text"], "5.5")
        self.assertEqual(r["abnormal"], "H")
        self.assertEqual(r["units"], "mmol/L")

    def test_msg2_same_patient_and_order_new_control_id(self):
        self.send(fx.MSG1)
        ack = self.send(fx.MSG2)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(ack.control_id, "MSG00002")
        # both messages present; patient and order queries see both
        self.assertEqual(len(self.store.list_messages()), 2)
        rows = self.store.results_for_patient("HOSP", "P12345")
        self.assertEqual(len(rows), 6)
        orders = self.store.results_for_order("FILLER100")
        self.assertEqual(
            sorted({r["control_id"] for r in orders}), ["MSG00001", "MSG00002"]
        )
        r2 = self.store.results_for_message("LAB1^HOSP", "MSG00002")
        self.assertEqual(r2[0]["value_type"], "CE")
        self.assertEqual(r2[0]["value_text"], "A^Abnormal^HL7OBS")
        self.assertEqual(r2[0]["rstatus"], "P")
        self.assertEqual(r2[1]["value_text"], "4.2")

    def test_identical_resend_no_new_rows(self):
        self.send(fx.MSG1)
        ack = self.send(fx.MSG1)  # byte-identical retransmission
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), 4)
        self.assertEqual(len(self.store.list_messages()), 1)
        states = [r["state"] for r in self.store.diagnostics()["inbox"]]
        self.assertEqual(states, ["acked", "duplicate"])

    def test_conflict_rejected_original_kept(self):
        self.send(fx.MSG1)
        ack = self.send(fx.MSG1_CONFLICT)
        self.assertEqual(ack.code, "AE")
        self.assertEqual(ack.control_id, "MSG00001")
        self.assertIn("conflict", (ack.err or "").lower())
        # original rows untouched
        row = self.store.results_for_message("LAB1^HOSP", "MSG00001")[0]
        self.assertEqual(row["value_text"], "7.5")
        self.assertEqual(self.store.count_results(), 4)
        diag = self.store.diagnostics()["inbox"]
        self.assertEqual(diag[-1]["state"], "conflict")
        self.assertGreater(diag[-1]["evidence"] and len(diag[-1]["evidence"]), 0)

    def test_invalid_obx_rejected_no_rows(self):
        ack = self.send(fx.MSG_INVALID_OBX)
        self.assertEqual(ack.code, "AE")
        self.assertEqual(ack.control_id, "MSG0000B")
        self.assertIn("OBX-11", ack.err or "")
        self.assertEqual(self.store.count_results(), 0)
        self.assertEqual(len(self.store.list_messages()), 0)
        diag = self.store.diagnostics()["inbox"]
        self.assertEqual(diag[0]["state"], "rejected")
        self.assertIn("OBX", diag[0]["detail"])

    def test_unknown_version_rejected(self):
        ack = self.send(fx.MSG_UNKNOWN_VERSION)
        self.assertEqual(ack.code, "AE")
        self.assertIn("9.9", ack.err or "")
        self.assertEqual(self.store.count_results(), 0)

    def test_unsupported_type_rejected(self):
        ack = self.send(fx.MSG_BAD_TYPE)
        self.assertEqual(ack.code, "AE")
        self.assertIn("ADT", ack.err or "")

    def test_undecodable_rejected(self):
        ack = self.send(fx.MSG_UNDECODABLE)
        self.assertEqual(ack.code, "AE")
        self.assertEqual(self.store.count_results(), 0)

    def test_cwe_rejected_for_v231(self):
        ack = self.send(fx.MSG_CWE_IN_231)
        self.assertEqual(ack.code, "AE")
        self.assertIn("CWE", ack.err or "")

    def test_fragmented_frame(self):
        ack = self.send(fx.MSG1, chunk=7, delay=0.005)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), 4)

    def test_byte_by_byte_frame(self):
        ack = self.send(fx.MSG2, chunk=1, delay=0.0)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), 2)

    def test_multiple_frames_one_read(self):
        stream = frame_payload(fx.MSG1) + frame_payload(fx.MSG2)
        acks = send_raw("127.0.0.1", self.port, [stream], expect_acks=2)
        self.assertEqual([a.code for a in acks], ["AA", "AA"])
        self.assertEqual(self.store.count_results(), 6)

    def test_garbage_then_valid_frame(self):
        acks = send_raw(
            "127.0.0.1", self.port,
            [b"garbage-without-vt", frame_payload(fx.MSG1)],
            expect_acks=1,
        )
        self.assertEqual(acks[0].code, "AA")
        self.assertEqual(self.store.count_results(), 4)
        errors = self.store.diagnostics()["frame_errors"]
        self.assertTrue(any("before VT" in e["reason"] for e in errors))

    def test_broken_frame_then_valid_frame(self):
        # FS not followed by CR, then a valid frame on the same connection
        broken = b"\x0bMSH|broken\x1c"  # VT ... FS (no CR)
        acks = send_raw(
            "127.0.0.1", self.port,
            [broken + frame_payload(fx.MSG1)],
            expect_acks=1,
        )
        self.assertEqual(acks[0].code, "AA")
        errors = self.store.diagnostics()["frame_errors"]
        self.assertTrue(any("FS not followed by CR" in e["reason"] for e in errors))
        self.assertEqual(self.store.count_results(), 4)

    def test_unterminated_frame_discarded_at_disconnect(self):
        # send half a frame and disconnect; next connection unaffected
        send_raw("127.0.0.1", self.port, [b"\x0bMSH|partial-no-end"],
                 expect_acks=0)
        ack = self.send(fx.MSG1)
        self.assertEqual(ack.code, "AA")
        errors = self.store.diagnostics()["frame_errors"]
        self.assertTrue(any("unterminated" in e["reason"] for e in errors))
        self.assertEqual(self.store.count_results(), 4)

    def test_custom_separators(self):
        ack = self.send(fx.MSG_CUSTOM_SEP)
        self.assertEqual(ack.code, "AA")
        row = self.store.results_for_message("LAB1^HOSP", "MSG0000S")[0]
        self.assertEqual(row["units"], "10*9/L")  # escaped field separator
        self.assertEqual(row["value_text"], "7.5")

    def test_escape_sequences(self):
        ack = self.send(fx.MSG_ESCAPES)
        self.assertEqual(ack.code, "AA")
        row = self.store.results_for_message("LAB1^HOSP", "MSG0000E")[0]
        self.assertEqual(row["value_text"], "A|B^C~D\\E&F")

    def test_repeating_patient_ids(self):
        ack = self.send(fx.MSG_REPEAT_PID3)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(
            len(self.store.results_for_patient("HOSP", "P12345")), 1
        )
        self.assertEqual(
            len(self.store.results_for_patient("OTHER", "P999")), 1
        )

    def test_duplicate_patient_key_in_pid3_no_double_count(self):
        # same (authority, identifier) twice in PID-3: query must not
        # multiply result rows
        msg = (
            "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000K|P|2.5.1\r"
            "PID|1||P12345^^^HOSP^MR~P12345^^^HOSP^MR||Zhang^San\r"
            "PV1|1|O\r"
            "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
            "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
        ).encode()
        ack = self.send(msg)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(len(self.store.results_for_patient("HOSP", "P12345")), 1)

    def test_empty_vs_missing(self):
        ack = self.send(fx.MSG_EMPTY_VS_MISSING)
        self.assertEqual(ack.code, "AA")
        row = self.store.results_for_message("LAB1^HOSP", "MSG0000M")[0]
        self.assertEqual(row["ref_range"], "")
        self.assertIsNone(row["observed_at"])

    def test_duplicate_result_keys_merged(self):
        ack = self.send(fx.MSG_DUP_MERGE)
        self.assertEqual(ack.code, "AA")
        rows = self.store.results_for_message("LAB1^HOSP", "MSG0000G")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["merged"], 1)

    def test_duplicate_result_keys_conflict_rejected(self):
        ack = self.send(fx.MSG_DUP_CONFLICT)
        self.assertEqual(ack.code, "AE")
        self.assertIn("duplicate", (ack.err or "").lower())
        self.assertEqual(self.store.count_results(), 0)


    def test_rejection_does_not_claim_identity(self):
        # rejected for bad version; the fixed message reuses the same
        # control id and must be accepted (a rejection never reserves the
        # dedup key)
        bad = fx.MSG1.replace(b"|2.5.1\r", b"|9.9\r")
        ack1 = self.send(bad)
        self.assertEqual(ack1.code, "AE")
        ack2 = self.send(fx.MSG1)
        self.assertEqual(ack2.code, "AA")
        self.assertEqual(self.store.count_results(), 4)

    def test_rejected_resend_rejected_again(self):
        self.send(fx.MSG_INVALID_OBX)
        ack = self.send(fx.MSG_INVALID_OBX)
        self.assertEqual(ack.code, "AE")
        self.assertEqual(self.store.count_results(), 0)
        states = [r["state"] for r in self.store.diagnostics()["inbox"]]
        self.assertEqual(states, ["rejected", "rejected"])

    def test_three_component_msh9_accepted(self):
        msg = fx.MSG2.replace(b"ORU^R01|", b"ORU^R01^ORU_R01|")
        ack = self.send(msg)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), 2)

    def test_nte_multiple_repetitions(self):
        msg = (
            "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000N|P|2.5.1\r"
            "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
            "PV1|1|O\r"
            "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
            "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
            "NTE|1||first part~second part\r"
        ).encode()
        ack = self.send(msg)
        self.assertEqual(ack.code, "AA")
        notes = self.store.notes_for_message("LAB1^HOSP", "MSG0000N")
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["text"], "first part~second part")


class TestFrameLimit(ServerCase):
    max_frame = 1024

    def test_oversized_frame_then_valid(self):
        big = fx.make_big_message(200, control_id="MSGTOOBIG")
        self.assertGreater(len(big), 1024)
        acks = send_raw(
            "127.0.0.1", self.port,
            [frame_payload(big) + frame_payload(fx.MSG1)],
            expect_acks=1,
        )
        # oversized frame: no business ACK; the valid frame after it is served
        self.assertEqual(acks[0].code, "AA")
        self.assertEqual(acks[0].control_id, "MSG00001")
        errors = self.store.diagnostics()["frame_errors"]
        self.assertTrue(any("exceeds max_frame" in e["reason"] for e in errors))
        self.assertEqual(self.store.count_results(), 4)


class TestManyObx(ServerCase):
    def test_large_message_batched_read(self):
        msg = fx.make_big_message(5000)
        ack = self.send(msg, chunk=4096)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), 5000)
        # batched streaming read stays correct and ordered
        seen = 0
        for batch in self.store.iter_results(batch=777):
            codes = [r["obs_code"] for r in batch]
            expected = [f"AN{seen + i + 1}" for i in range(len(batch))]
            self.assertEqual(codes, expected)
            seen += len(batch)
        self.assertEqual(seen, 5000)


if __name__ == "__main__":
    unittest.main()
