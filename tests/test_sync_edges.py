"""Edge cases: zero-width pages, shared control ids, empty OBR groups."""

from __future__ import annotations

import os
import tempfile
import unittest

from hl7recv.builder import build_message
from hl7recv.store import Store
from hl7recv.sync import publish, replica

SUB = ["LAB1^HOSP", "LAB2^HOSP"]

# two senders reusing the SAME control id: independent messages
A1 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|SHARED1|P|2.5.1\r"
    "PID|1||P1^^^HOSP^MR||A^B\r"
    "PV1|1|O\r"
    "OBR|1|PL|FI|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
).encode("utf-8")
A2 = (
    "MSH|^~\\&|LAB2|HOSP|RCVAPP|RCVFAC|20261101083100||ORU^R01|SHARED1|P|2.5.1\r"
    "PID|1||P2^^^HOSP^MR||C^D\r"
    "PV1|1|O\r"
    "OBR|1|PL|FI|LYTE^Lyte Panel^LN\r"
    "OBX|1|NM|K^Potassium^LN|1|4.1|mmol/L|3.5-5.1|N|||F\r"
).encode("utf-8")
# an OBR group with zero OBX (legal per profile): message with no results
EMPTY_GROUP = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083200||ORU^R01|NOOBX1|P|2.5.1\r"
    "PID|1||P1^^^HOSP^MR||A^B\r"
    "PV1|1|O\r"
    "OBR|1|PL|FI|PANEL^Empty Panel^LN\r"
).encode("utf-8")


class EdgeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "recv.db")
        self.rep = os.path.join(self.tmp.name, "replica.db")
        store = Store(self.db)
        for i, payload in enumerate((A1, A2, EMPTY_GROUP), 1):
            store.commit_message(i, build_message(payload), payload)
        store.close()
        self.meta = publish.upgrade(self.db)
        replica.init_replica(self.rep, source_id=self.meta["source_id"],
                             senders=SUB)

    def tearDown(self):
        self.tmp.cleanup()

    def _catch_up(self, bound: int, budget: int = 10) -> None:
        cursor = replica.get_position(self.rep)["cursor"]
        while cursor < bound:
            data = publish.fetch_page(self.db, SUB, start=cursor,
                                      bound=bound, budget=budget)
            out = replica.import_page(self.rep, data)
            self.assertEqual(out["status"], "applied")
            cursor = out["cursor"]

    def test_same_control_id_different_senders_are_independent(self):
        self._catch_up(3)
        facts = list(replica.iter_facts(self.rep))
        shared = [f for f in facts if f["control_id"] == "SHARED1"]
        self.assertEqual(len(shared), 2)
        self.assertEqual({f["sender"] for f in shared},
                         {"LAB1^HOSP", "LAB2^HOSP"})
        by_sender = {f["sender"]: f for f in shared}
        self.assertEqual(by_sender["LAB1^HOSP"]["results"][0]["obs_code"],
                         "WBC")
        self.assertEqual(by_sender["LAB2^HOSP"]["results"][0]["obs_code"],
                         "K")

    def test_message_with_zero_results_still_lands(self):
        self._catch_up(3)
        facts = {f["control_id"]: f for f in replica.iter_facts(self.rep)}
        self.assertIn("NOOBX1", facts)
        self.assertEqual(facts["NOOBX1"]["results"], [])

    def test_zero_width_page_applies_and_replays(self):
        self._catch_up(3)
        # watermark has not moved: a page over (3,3] is a legal empty
        # terminal page; applying it must not fail, and replaying it must
        # be a clean no-op (not a primary-key crash)
        data = publish.fetch_page(self.db, SUB, start=3, bound=3, budget=1)
        out = replica.import_page(self.rep, data)
        self.assertEqual((out["status"], out["cursor"]), ("applied", 3))
        again = replica.import_page(self.rep, data)
        self.assertEqual((again["status"], again["cursor"]), ("duplicate", 3))
        self.assertEqual(replica.get_position(self.rep)["cursor"], 3)
        self.assertEqual(len(list(replica.iter_facts(self.rep))), 3)


if __name__ == "__main__":
    unittest.main()
