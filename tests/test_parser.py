"""Unit tests for the hierarchical parser and the profile builder."""

from __future__ import annotations

import unittest

from hl7recv.builder import build_message
from hl7recv.errors import ParseError, ProfileError, StructureError
from hl7recv.hl7 import Delimiters, parse_envelope, unescape
from tests import fixtures as fx


class TestEnvelope(unittest.TestCase):
    def test_declared_delimiters_used(self):
        env = parse_envelope(fx.MSG_CUSTOM_SEP)
        d = env.delimiters
        self.assertEqual((d.field, d.component, d.repetition, d.escape, d.subcomponent),
                         ("*", "!", "@", "#", "$"))
        msh = env.segments[0]
        self.assertEqual(msh.field(9).component(1, 1), "ORU")
        self.assertEqual(msh.field(9).component(1, 2), "R01")
        self.assertEqual(msh.field(10).text(d), "MSG0000S")

    def test_standard_message_shape(self):
        env = parse_envelope(fx.MSG1)
        self.assertEqual(env.segments[0].name, "MSH")
        names = [s.name for s in env.segments]
        self.assertEqual(
            names,
            ["MSH", "PID", "PV1", "OBR", "NTE", "OBX", "OBX", "NTE", "OBR", "OBX", "OBX"],
        )
        # MSH-1 is the separator itself
        self.assertEqual(env.segments[0].field(1).text(env.delimiters), "|")
        self.assertEqual(env.segments[0].field(2).text(env.delimiters), "^~\\&")

    def test_empty_vs_missing(self):
        env = parse_envelope(fx.MSG_EMPTY_VS_MISSING)
        obx = [s for s in env.segments if s.name == "OBX"][0]
        f7 = obx.field(7)
        self.assertTrue(f7.present)
        self.assertEqual(f7.text(env.delimiters), "")
        f14 = obx.field(14)
        self.assertFalse(f14.present)
        self.assertIsNone(f14.text(env.delimiters))

    def test_repetitions_and_components(self):
        env = parse_envelope(fx.MSG_REPEAT_PID3)
        pid3 = env.segments[1].field(3)
        self.assertEqual(pid3.n_reps, 2)
        self.assertEqual(pid3.component(1, 1), "P12345")
        self.assertEqual(pid3.component(1, 4), "HOSP")
        self.assertEqual(pid3.component(2, 1), "P999")
        self.assertEqual(pid3.component(2, 4), "OTHER")

    def test_subcomponents(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X1|P|2.5.1\r"
            "PID|1||P1^^^AUTH^MR||N1\r"
            "PV1|1|O\r"
            "OBR|1|PL|FI|T^Test^LN\r"
            "OBX|1|ST|X^Y^LN|1|v1&v2|||N|||F\r"
        ).encode()
        env = parse_envelope(msg)
        obx5 = env.segments[4].field(5)
        self.assertEqual(obx5.reps[0][0], ["v1", "v2"])
        self.assertEqual(obx5.text(env.delimiters), "v1&v2")

    def test_unescape_all_sequences(self):
        d = Delimiters(field="|", component="^", repetition="~", escape="\\", subcomponent="&")
        self.assertEqual(unescape(r"A\F\B", d), "A|B")
        self.assertEqual(unescape(r"A\S\B", d), "A^B")
        self.assertEqual(unescape(r"A\R\B", d), "A~B")
        self.assertEqual(unescape(r"A\E\B", d), "A\\B")
        self.assertEqual(unescape(r"A\T\B", d), "A&B")
        self.assertEqual(unescape(r"A\X41\B", d), "AAB")
        self.assertEqual(unescape(r"A\H\hi\N\B", d), "AhiB")
        self.assertEqual(unescape("\\Zlocal\\", d), "Zlocal")
        self.assertEqual(unescape("unknown\\Q\\x", d), "unknown\\Q\\x")

    def test_unescape_with_custom_escape_char(self):
        d = Delimiters(field="*", component="!", repetition="@", escape="#", subcomponent="$")
        self.assertEqual(unescape("A#F#B", d), "A*B")
        self.assertEqual(unescape("A#S#B", d), "A!B")

    def test_escaped_separators_do_not_split(self):
        env = parse_envelope(fx.MSG_ESCAPES)
        obx = [s for s in env.segments if s.name == "OBX"][0]
        self.assertEqual(obx.field(5).text(env.delimiters), "A|B^C~D\\E&F")

    def test_undecodable_message(self):
        with self.assertRaises(ParseError) as cm:
            parse_envelope(fx.MSG_UNDECODABLE)
        self.assertIn("UTF-8", str(cm.exception))

    def test_not_starting_with_msh(self):
        with self.assertRaises(ParseError):
            parse_envelope(b"PID|1||x\r")

    def test_crlf_tolerated(self):
        env = parse_envelope(fx.MSG2.replace(b"\r", b"\r\n"))
        self.assertEqual(env.segments[0].name, "MSH")
        self.assertEqual(len([s for s in env.segments if s.name == "OBX"]), 2)


class TestBuilder(unittest.TestCase):
    def test_msg1_structure(self):
        msg = build_message(fx.MSG1)
        self.assertEqual(msg.version, "2.5.1")
        self.assertEqual(msg.sender, "LAB1^HOSP")
        self.assertEqual(msg.control_id, "MSG00001")
        self.assertEqual(msg.patients, [("HOSP", "P12345")])
        self.assertEqual(msg.visit_id, "V2026-0001")
        self.assertEqual(len(msg.groups), 2)

        g1, g2 = msg.groups
        self.assertEqual(g1.test_code, "CBC")
        self.assertEqual(g1.test_system, "LN")
        self.assertEqual(g1.placer_order, "PLACER100")
        self.assertEqual(g1.filler_order, "FILLER100")
        self.assertEqual(g1.reported_at, "20261101082900")
        self.assertEqual(len(g1.ntes), 1)
        self.assertEqual(g1.ntes[0].text, "fasting sample for OBR one")
        self.assertEqual(len(g1.obxs), 2)

        obx1 = g1.obxs[0]
        self.assertEqual(obx1.value_type, "NM")
        self.assertEqual(obx1.value_text, "7.5")  # kept as text, not float
        self.assertEqual(obx1.units, "10*9/L")
        self.assertEqual(obx1.ref_range, "4.0-10.0")
        self.assertEqual(obx1.abnormal, "N")
        self.assertEqual(obx1.status, "F")
        self.assertEqual(obx1.observed_at, "20261101081000")
        self.assertEqual(obx1.sub_id, "1")

        obx2 = g1.obxs[1]
        self.assertEqual(obx2.value_type, "ST")
        self.assertEqual(len(obx2.ntes), 1)
        self.assertEqual(obx2.ntes[0].text, "comment belonging to OBX 2")

        self.assertEqual(g2.obxs[0].value_type, "CWE")
        self.assertEqual(g2.obxs[0].value_text, "POCT^Point of Care^HL7OBS")
        self.assertEqual(g2.obxs[1].abnormal, "H")

    def test_msg2_v231(self):
        msg = build_message(fx.MSG2)
        self.assertEqual(msg.version, "2.3.1")
        self.assertEqual(msg.control_id, "MSG00002")
        self.assertEqual(msg.groups[0].obxs[0].value_type, "CE")
        self.assertEqual(msg.patients, [("HOSP", "P12345")])
        self.assertEqual(msg.groups[0].filler_order, "FILLER100")

    def test_hook_called_once(self):
        calls = []
        build_message(fx.MSG1, hook=lambda: calls.append(1))
        self.assertEqual(len(calls), 1)

    def test_unknown_version_rejected(self):
        with self.assertRaises(ProfileError) as cm:
            build_message(fx.MSG_UNKNOWN_VERSION)
        self.assertIn("9.9", str(cm.exception))
        self.assertIn("field 12", str(cm.exception))

    def test_unsupported_message_type(self):
        with self.assertRaises(ProfileError) as cm:
            build_message(fx.MSG_BAD_TYPE)
        self.assertIn("ADT", str(cm.exception))

    def test_cwe_rejected_in_v231(self):
        with self.assertRaises(ProfileError) as cm:
            build_message(fx.MSG_CWE_IN_231)
        self.assertIn("CWE", str(cm.exception))
        self.assertIn("2.3.1", str(cm.exception))

    def test_invalid_obx_missing_status(self):
        with self.assertRaises(ProfileError) as cm:
            build_message(fx.MSG_INVALID_OBX)
        self.assertIn("OBX-11", str(cm.exception))

    def test_error_location_coordinates(self):
        try:
            build_message(fx.MSG_INVALID_OBX)
        except ProfileError as exc:
            self.assertEqual(exc.segment, "OBX")
            self.assertEqual(exc.field, 11)
            self.assertEqual(exc.seg_index, 5)
        else:
            self.fail("expected ProfileError")

    def test_pid3_missing_authority_located(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X2|P|2.5.1\r"
            "PID|1||P12345^^^~P999^^MR||N\r"
            "PV1|1|O\r"
            "OBR|1|PL|FI|T^Test^LN\r"
            "OBX|1|NM|X^Y^LN|1|1|u|r|N|||F\r"
        ).encode()
        with self.assertRaises(ProfileError) as cm:
            build_message(msg)
        self.assertIn("rep 1", str(cm.exception))
        self.assertIn("component 4", str(cm.exception))

    def test_obx_before_obr_rejected(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X3|P|2.5.1\r"
            "PID|1||P1^^^AU^MR||N\r"
            "PV1|1|O\r"
            "OBX|1|NM|X^Y^LN|1|1|u|r|N|||F\r"
        ).encode()
        with self.assertRaises(StructureError):
            build_message(msg)

    def test_unknown_segment_rejected(self):
        msg = fx.MSG1.replace(b"PV1|1|", b"IN1|1|x\rPV1|1|")
        with self.assertRaises(StructureError) as cm:
            build_message(msg)
        self.assertIn("IN1", str(cm.exception))

    def test_no_obr_group_rejected(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X4|P|2.5.1\r"
            "PID|1||P1^^^AU^MR||N\r"
            "PV1|1|O\r"
        ).encode()
        with self.assertRaises(StructureError):
            build_message(msg)

    def test_duplicate_merge_identical(self):
        msg = build_message(fx.MSG_DUP_MERGE)
        obxs = msg.groups[0].obxs
        self.assertEqual(len(obxs), 1)
        self.assertEqual(obxs[0].merged, 1)
        self.assertEqual(obxs[0].obx_index, 1)  # first occurrence kept

    def test_duplicate_conflict_rejected(self):
        with self.assertRaises(StructureError) as cm:
            build_message(fx.MSG_DUP_CONFLICT)
        self.assertIn("conflicting duplicate", str(cm.exception))

    def test_custom_separators_build(self):
        msg = build_message(fx.MSG_CUSTOM_SEP)
        self.assertEqual(msg.control_id, "MSG0000S")
        self.assertEqual(msg.groups[0].obxs[0].value_text, "7.5")
        self.assertEqual(msg.patients, [("HOSP", "P12345")])

    def test_repeating_pid3_all_keys(self):
        msg = build_message(fx.MSG_REPEAT_PID3)
        self.assertEqual(msg.patients, [("HOSP", "P12345"), ("OTHER", "P999")])

    def test_empty_vs_missing_fields(self):
        msg = build_message(fx.MSG_EMPTY_VS_MISSING)
        obx = msg.groups[0].obxs[0]
        self.assertEqual(obx.ref_range, "")  # present but empty
        self.assertIsNone(obx.observed_at)  # missing (and no OBR-7 fallback)

    def test_orc_order_fallback(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X5|P|2.5.1\r"
            "PID|1||P1^^^AU^MR||N\r"
            "PV1|1|O\r"
            "ORC|RE|ORCPLACER^X|ORCFILLER^X\r"
            "OBR|1|||T^Test^LN\r"
            "OBX|1|NM|X^Y^LN|1|1|u|r|N|||F\r"
        ).encode()
        built = build_message(msg)
        self.assertEqual(built.groups[0].placer_order, "ORCPLACER")
        self.assertEqual(built.groups[0].filler_order, "ORCFILLER")

    def test_obr_overrides_orc(self):
        msg = (
            "MSH|^~\\&|A|B|C|D|20261101000000||ORU^R01|X6|P|2.5.1\r"
            "PID|1||P1^^^AU^MR||N\r"
            "PV1|1|O\r"
            "ORC|RE|ORCPLACER|ORCFILLER\r"
            "OBR|1|OBRPLACER|OBRFILLER|T^Test^LN\r"
            "OBX|1|NM|X^Y^LN|1|1|u|r|N|||F\r"
        ).encode()
        built = build_message(msg)
        self.assertEqual(built.groups[0].placer_order, "OBRPLACER")
        self.assertEqual(built.groups[0].filler_order, "OBRFILLER")


if __name__ == "__main__":
    unittest.main()
