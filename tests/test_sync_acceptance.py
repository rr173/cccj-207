"""Acceptance: catch up one segment, then the next (two-round story).

Round 1 commits three messages in order: M1 (subscribed LAB1, multi
OBR/OBX), M2 (UNSUBSCRIBED sender), M3 (subscribed LAB2, empty-string vs
NULL facts).  The round-1 watermark is fixed, then two more subscribed
messages are committed whose ``received_at`` is respectively EQUAL to and
EARLIER than messages inside the round-1 bound -- proving positions
follow commit order, not business time.

Pages are fetched with a scan budget of ONE message per page so the
all-non-matching page (M2) must still advance the cursor.  Every page's
facts are checked against hand-derived expectations; after each round an
independent reference (refcheck.sync_reference, no product paging/import
code) re-derives the expected replica contents from the source database.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest

from hl7recv.builder import build_message
from hl7recv.store import Store
from hl7recv.sync import publish, replica

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SUBSCRIPTION = ["LAB1^HOSP", "LAB2^HOSP"]

# --- hand-written synthetic messages (all expectations derived by hand) ----
# M1: subscribed, two OBR groups / four OBX (multi-coordinate message).
M1 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|SYNC0001|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
    "OBX|2|ST|WBCM^Morphology Comment^LN|1|No abnormal cells seen|||N|||F\r"
    "OBR|2|PLACER200|FILLER200|GLUP^Glucose Panel^LN\r"
    "OBX|1|CWE|GLUCM^Glucose Method^LN|1|POCT^Point of Care^HL7OBS|||N|||F\r"
    "OBX|2|NM|GLU^Glucose^LN|1|5.5|mmol/L|3.9-6.1|H|||F\r"
).encode("utf-8")

# M2: NOT subscribed (sender MIC1^HOSP).
M2 = (
    "MSH|^~\\&|MIC1|HOSP|RCVAPP|RCVFAC|20261101084000||ORU^R01|SYNC0002|P|2.5.1\r"
    "PID|1||P222^^^HOSP^MR||Li^Si\r"
    "PV1|1|O\r"
    "OBR|1|PL1|FI1|MICP^Micro Panel^LN\r"
    "OBX|1|ST|GRAM^Gram Stain^LN|1|Negative|||N|||F\r"
).encode("utf-8")

# M3: subscribed; OBX-4 present-but-empty -> sub_id is the EMPTY STRING;
# OBX-6 present-but-empty -> units is NULL (facts pass through as-is).
M3 = (
    "MSH|^~\\&|LAB2|HOSP|RCVAPP|RCVFAC|20261101085000||ORU^R01|SYNC0003|P|2.5.1\r"
    "PID|1||P333^^^HOSP^MR||Wang^Wu\r"
    "PV1|1|O\r"
    "OBR|1|PL2|FI2|LYTE^Lyte Panel^LN\r"
    "OBX|1|NM|K^Potassium^LN||4.1|mmol/L|3.5-5.1|N|||F\r"
    "OBX|2|ST|FLAG^Flag Text^LN|2|review note|||N|||F\r"
).encode("utf-8")

# M4: committed after the round-1 watermark was fixed; received_at EQUALS M3's.
M4 = (
    "MSH|^~\\&|LAB2|HOSP|RCVAPP|RCVFAC|20261101090000||ORU^R01|SYNC0004|P|2.5.1\r"
    "PID|1||P333^^^HOSP^MR||Wang^Wu\r"
    "PV1|1|O\r"
    "OBR|1|PL3|FI3|LYTE^Lyte Panel^LN\r"
    "OBX|1|NM|NA^Sodium^LN|1|140|mmol/L|135-145|N|||F\r"
).encode("utf-8")

# M5: committed after M4; received_at EARLIER than M1's.
M5 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101091000||ORU^R01|SYNC0005|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PL4|FI4|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|HGB^Hemoglobin^LN|1|13.2|g/dL|12.0-16.0|N|||F\r"
).encode("utf-8")

T1 = "2026-11-01T08:30:00.000+00:00"
T2 = "2026-11-01T08:31:00.000+00:00"
T3 = "2026-11-01T08:32:00.000+00:00"

# hand-derived expected facts (never built from product-generated pages)
EXP_M1_RESULTS = [
    {"obr_index": 1, "obx_index": 1, "obs_system": "LN", "obs_code": "WBC",
     "sub_id": "1", "value_type": "NM", "value_text": "7.5", "units": "10*9/L"},
    {"obr_index": 1, "obx_index": 2, "obs_system": "LN", "obs_code": "WBCM",
     "sub_id": "1", "value_type": "ST",
     "value_text": "No abnormal cells seen", "units": None},
    {"obr_index": 2, "obx_index": 1, "obs_system": "LN", "obs_code": "GLUCM",
     "sub_id": "1", "value_type": "CWE",
     "value_text": "POCT^Point of Care^HL7OBS", "units": None},
    {"obr_index": 2, "obx_index": 2, "obs_system": "LN", "obs_code": "GLU",
     "sub_id": "1", "value_type": "NM", "value_text": "5.5",
     "units": "mmol/L"},
]
EXP_M3_RESULTS = [
    {"obr_index": 1, "obx_index": 1, "obs_system": "LN", "obs_code": "K",
     "sub_id": "", "value_type": "NM", "value_text": "4.1",
     "units": "mmol/L"},
    {"obr_index": 1, "obx_index": 2, "obs_system": "LN", "obs_code": "FLAG",
     "sub_id": "2", "value_type": "ST", "value_text": "review note",
     "units": None},
]
EXP_M4_RESULTS = [
    {"obr_index": 1, "obx_index": 1, "obs_system": "LN", "obs_code": "NA",
     "sub_id": "1", "value_type": "NM", "value_text": "140",
     "units": "mmol/L"},
]
EXP_M5_RESULTS = [
    {"obr_index": 1, "obx_index": 1, "obs_system": "LN", "obs_code": "HGB",
     "sub_id": "1", "value_type": "NM", "value_text": "13.2",
     "units": "g/dL"},
]


def _refcheck(recv_db: str, replica_db: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "refcheck.sync_reference",
         "--receiver-db", recv_db, "--replica-db", replica_db]
        + [arg for s in SUBSCRIPTION for arg in ("--sender", s)],
        capture_output=True, text=True, cwd=ROOT)
    assert proc.returncode == 0, f"reference check failed:\n{proc.stderr}"
    assert "SYNC REFERENCE CHECK PASSED" in proc.stdout


class TwoRoundCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.recv_db = os.path.join(self.tmp.name, "recv.db")
        self.replica_db = os.path.join(self.tmp.name, "replica.db")
        self.clock = [T1]
        self.store = Store(self.recv_db, clock=lambda: self.clock[0])

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _commit(self, payload: bytes, attempt: int) -> str:
        msg = build_message(payload)
        return self.store.commit_message(attempt, msg, payload)

    def _fetch(self, start: int, bound: int, budget: int = 1) -> dict:
        data = publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=start, bound=bound, budget=budget
        )
        from hl7recv.sync import protocol

        return protocol.parse_page(data)

    def test_catch_up_segment_then_next(self):
        # ---- round 1: three commits, the middle one not subscribed --------
        self.clock[0] = T1
        self.assertEqual(self._commit(M1, 1), "committed")
        self.clock[0] = T2
        self.assertEqual(self._commit(M2, 2), "committed")
        self.clock[0] = T3
        self.assertEqual(self._commit(M3, 3), "committed")

        # offline upgrade publishes the three historical messages
        meta = publish.upgrade(self.recv_db)
        self.assertEqual(meta["status"], "upgraded")
        self.assertEqual(meta["bound"], 3)

        wm1 = publish.watermark(self.recv_db, SUBSCRIPTION)
        self.assertEqual(wm1["bound"], 3)
        replica.init_replica(
            self.replica_db, source_id=wm1["source_id"], senders=SUBSCRIPTION
        )
        self.assertEqual(replica.get_position(self.replica_db)["cursor"], 0)

        # page 1: (0,1] -> M1 with all four results
        p1 = self._fetch(0, wm1["bound"])
        self.assertEqual((p1["start"], p1["end"], p1["terminal"]), (0, 1, 0))
        self.assertEqual([m["position"] for m in p1["messages"]], [1])
        self.assertEqual(p1["messages"][0]["sender"], "LAB1^HOSP")
        self.assertEqual(p1["messages"][0]["control_id"], "SYNC0001")
        self.assertEqual(p1["messages"][0]["received_at"], T1)
        self.assertEqual(p1["messages"][0]["results"], EXP_M1_RESULTS)
        r = replica.import_page(self.replica_db, publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=0, bound=wm1["bound"], budget=1))
        self.assertEqual((r["status"], r["cursor"]), ("applied", 1))

        # page 2: (1,2] -> M2 not subscribed: EMPTY payload, still advances
        p2 = self._fetch(1, wm1["bound"])
        self.assertEqual((p2["start"], p2["end"], p2["terminal"]), (1, 2, 0))
        self.assertEqual(p2["messages"], [])
        r = replica.import_page(self.replica_db, publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=1, bound=wm1["bound"], budget=1))
        self.assertEqual((r["status"], r["cursor"]), ("applied", 2))
        self.assertEqual(replica.get_position(self.replica_db)["cursor"], 2)

        # page 3: (2,3] -> M3, terminal at the bound
        p3 = self._fetch(2, wm1["bound"])
        self.assertEqual((p3["start"], p3["end"], p3["terminal"]), (2, 3, 1))
        self.assertEqual([m["position"] for m in p3["messages"]], [3])
        self.assertEqual(p3["messages"][0]["sender"], "LAB2^HOSP")
        self.assertEqual(p3["messages"][0]["results"], EXP_M3_RESULTS)
        # empty string vs NULL survived the trip exactly
        self.assertEqual(p3["messages"][0]["results"][0]["sub_id"], "")
        self.assertIsNone(p3["messages"][0]["results"][1]["units"])
        r = replica.import_page(self.replica_db, publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=2, bound=wm1["bound"], budget=1))
        self.assertEqual((r["status"], r["cursor"]), ("applied", 3))

        facts = list(replica.iter_facts(self.replica_db))
        self.assertEqual([f["control_id"] for f in facts],
                         ["SYNC0001", "SYNC0003"])
        _refcheck(self.recv_db, self.replica_db)

        # ---- watermark for round 1 is now fixed; late commits arrive ------
        self.clock[0] = T3  # EQUAL to M3's received_at (inside the bound)
        self.assertEqual(self._commit(M4, 4), "committed")
        self.clock[0] = "2026-11-01T08:00:00.000+00:00"  # EARLIER than M1
        self.assertEqual(self._commit(M5, 5), "committed")

        # the fixed round-1 bound must still reproduce the same pages
        p1_again = self._fetch(0, wm1["bound"])
        self.assertEqual(p1_again["digest"], p1["digest"])

        # ---- round 2: catch up from the applied position ------------------
        wm2 = publish.watermark(self.recv_db, SUBSCRIPTION)
        self.assertEqual(wm2["bound"], 5)
        cursor = replica.get_position(self.replica_db)["cursor"]
        self.assertEqual(cursor, 3)

        p4 = self._fetch(cursor, wm2["bound"])
        self.assertEqual((p4["start"], p4["end"], p4["terminal"]), (3, 4, 0))
        self.assertEqual([m["control_id"] for m in p4["messages"]], ["SYNC0004"])
        self.assertEqual(p4["messages"][0]["received_at"], T3)
        self.assertEqual(p4["messages"][0]["results"], EXP_M4_RESULTS)
        replica.import_page(self.replica_db, publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=cursor, bound=wm2["bound"],
            budget=1))

        p5 = self._fetch(4, wm2["bound"])
        self.assertEqual((p5["start"], p5["end"], p5["terminal"]), (4, 5, 1))
        self.assertEqual([m["control_id"] for m in p5["messages"]], ["SYNC0005"])
        self.assertEqual(p5["messages"][0]["received_at"],
                         "2026-11-01T08:00:00.000+00:00")
        self.assertEqual(p5["messages"][0]["results"], EXP_M5_RESULTS)
        replica.import_page(self.replica_db, publish.fetch_page(
            self.recv_db, SUBSCRIPTION, start=4, bound=wm2["bound"], budget=1))

        # the two late commits each appear exactly once, in commit order
        facts = list(replica.iter_facts(self.replica_db))
        self.assertEqual(
            [(f["position"], f["control_id"]) for f in facts],
            [(1, "SYNC0001"), (3, "SYNC0003"), (4, "SYNC0004"), (5, "SYNC0005")],
        )
        ids = [f["control_id"] for f in facts]
        self.assertEqual(ids.count("SYNC0004"), 1)
        self.assertEqual(ids.count("SYNC0005"), 1)
        self.assertEqual(replica.get_position(self.replica_db)["cursor"], 5)
        _refcheck(self.recv_db, self.replica_db)

    def test_duplicate_and_conflict_consume_no_position(self):
        self.clock[0] = T1
        self._commit(M1, 1)
        publish.upgrade(self.recv_db)
        # identical resend -> duplicate; conflicting bytes -> conflict
        self.assertEqual(self._commit(M1, 2), "duplicate")
        conflict = M1.replace(b"|7.5|", b"|7.6|")
        self.assertEqual(self._commit(conflict, 3), "conflict")
        status = self.store.sync_status()
        self.assertEqual(status["bound"], 1, "no new positions may appear")
        # a genuinely new message still gets the very next position
        self.clock[0] = T2
        self._commit(M2, 4)
        self.assertEqual(self.store.sync_status()["bound"], 2)


if __name__ == "__main__":
    unittest.main()
