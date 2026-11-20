"""Unit tests for the page wire protocol (canonical form and checks)."""

from __future__ import annotations

import unittest

from hl7recv.sync import protocol


def _page(**over):
    base = dict(
        source_id="src1",
        subscription=["LAB1^HOSP"],
        bound=5,
        start=0,
        end=2,
        budget=3,
        terminal=False,
        messages=[{
            "position": 2,
            "sender": "LAB1^HOSP",
            "control_id": "C1",
            "received_at": "2026-11-01T00:00:00.000+00:00",
            "results": [{
                "obr_index": 1, "obx_index": 1, "obs_system": "LN",
                "obs_code": "WBC", "sub_id": "", "value_type": "NM",
                "value_text": "7.5", "units": None,
            }],
        }],
    )
    base.update(over)
    return base


class CanonicalCase(unittest.TestCase):
    def test_same_content_same_bytes(self):
        a = protocol.build_page(**_page())
        b = protocol.build_page(**_page())
        self.assertEqual(a, b)

    def test_content_change_changes_bytes(self):
        a = protocol.build_page(**_page())
        changed = _page()
        changed["messages"][0]["results"][0]["value_text"] = "7.6"
        b = protocol.build_page(**changed)
        self.assertNotEqual(a, b)

    def test_empty_string_and_null_roundtrip(self):
        page = protocol.parse_page(protocol.build_page(**_page()))
        row = page["messages"][0]["results"][0]
        self.assertEqual(row["sub_id"], "")
        self.assertIsNone(row["units"])

    def test_subscription_normalization(self):
        a = protocol.subscription_id(["LAB2^HOSP", "LAB1^HOSP", "LAB2^HOSP"])
        b = protocol.subscription_id([" LAB1^HOSP ", "LAB2^HOSP"])
        self.assertEqual(a, b, "order, duplicates and padding are normalised")
        c = protocol.subscription_id(["LAB1^HOSP"])
        self.assertNotEqual(a, c)
        with self.assertRaises(ValueError):
            protocol.subscription_id([])
        with self.assertRaises(ValueError):
            protocol.subscription_id(["  "])


class ParseCase(unittest.TestCase):
    def test_roundtrip(self):
        page = protocol.parse_page(protocol.build_page(**_page()))
        self.assertEqual(page["start"], 0)
        self.assertEqual(page["end"], 2)
        self.assertEqual(page["terminal"], 0)
        self.assertEqual(len(page["messages"]), 1)

    def test_truncated_tail(self):
        data = protocol.build_page(**_page())
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(data[:-10])

    def test_digest_covers_payload_and_header(self):
        data = protocol.build_page(**_page())
        for token, repl in ((b"7.5", b"9.9"), (b'"start":0', b'"start":1'),
                            (b'"end":2', b'"end":3')):
            self.assertIn(token, data)
            with self.assertRaises(protocol.PageError, msg=token):
                protocol.parse_page(data.replace(token, repl, 1))

    def test_unknown_and_missing_fields(self):
        import json

        obj = json.loads(protocol.build_page(**_page()))
        obj["extra"] = 1
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(protocol.canonical_bytes(obj))
        del obj["extra"]
        del obj["bound"]
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(protocol.canonical_bytes(obj))

    def test_window_invariants(self):
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(protocol.build_page(**_page(end=9)))
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(
                protocol.build_page(**_page(end=2, terminal=True, bound=5))
            )

    def test_message_positions_must_be_ordered_inside_window(self):
        bad = _page()
        bad["messages"] = [
            dict(bad["messages"][0], position=2),
            dict(bad["messages"][0], position=1),
        ]
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(protocol.build_page(**bad))
        outside = _page()
        outside["messages"] = [dict(outside["messages"][0], position=5)]
        with self.assertRaises(protocol.PageError):
            protocol.parse_page(protocol.build_page(**outside))

    def test_terminal_empty_page_at_bound(self):
        page = protocol.parse_page(protocol.build_page(**_page(
            start=5, end=5, bound=5, terminal=True, messages=[],
        )))
        self.assertEqual(page["messages"], [])
        self.assertEqual(page["terminal"], 1)


if __name__ == "__main__":
    unittest.main()
