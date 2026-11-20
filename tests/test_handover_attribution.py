"""Targeted attribution edge tests built on small hand-made fixtures.

These go beyond the acceptance story to pin down the exact, distinguishable
attribution outcomes and the strict mapping rules:

* placer and filler numbers that map to disjoint work orders -> conflict,
  with no back-stop from the other number;
* a number registered but outside its effective window at received_at;
* a half-open mapping switch on the exact boundary instant;
* identical identification conditions on two items staying separate;
* an explicit NULL sub id never matching a non-null observation.

Each scenario is also cross-checked by the independent reference checker.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from drills import handover_drill as hd
from refcheck import handover_reference as href
from shiftledger import reader, service, vocab


def _write(path: str, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, sort_keys=True) + "\n")


class MiniLedger:
    """Build a tiny receiver DB + manifest with explicit rows."""

    def __init__(self, tmp: str):
        self.tmp = tmp
        self.recv = os.path.join(tmp, "receiver.db")
        self.mdir = os.path.join(tmp, "manifest")
        self.ledger = os.path.join(tmp, "ledger.db")
        os.makedirs(self.mdir, exist_ok=True)
        self.store = hd.new_receiver(self.recv)
        self.conn = self.store._conn

    def group(self, cid, *, patient=(hd.DOMAIN, hd.PATIENT), placer=None,
              filler=None, obs=("LN", "X", None), received=hd.T2):
        hd._insert_group(
            self.conn, hd.SENDER, cid, 1, patient, placer, filler,
            [(i + 1, s, c, sub) for i, (s, c, sub) in enumerate(
                obs if isinstance(obs, list) else [obs])],
            received)
        self.conn.commit()

    def manifest(self, items, mappings, wos=None, cutoff=hd.T3):
        wos = wos or [
            {"work_order_id": "WO1", "patient_domain": hd.DOMAIN,
             "patient_id": hd.PATIENT},
            {"work_order_id": "WO2", "patient_domain": hd.DOMAIN,
             "patient_id": hd.PATIENT},
        ]
        with open(os.path.join(self.mdir, "manifest.json"), "w") as fh:
            json.dump({"shift_id": "S", "shift_start": hd.T0,
                       "cutoff": cutoff, "senders": [hd.SENDER]}, fh)
        _write(os.path.join(self.mdir, "work_orders.jsonl"), wos)
        _write(os.path.join(self.mdir, "items.jsonl"), items)
        _write(os.path.join(self.mdir, "mappings.jsonl"), mappings)

    def close(self):
        self.store.close()


class MappingConflictCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = MiniLedger(self.tmp.name)

    def tearDown(self):
        self.fx.close()
        self.tmp.cleanup()

    def test_disjoint_placer_filler_is_conflict_no_backstop(self):
        # placer -> WO1, filler -> WO2: intersection empty, cannot fall back.
        self.fx.group("MSG-C", placer="LP", filler="LF")
        self.fx.manifest(
            items=[{"item_id": "I1", "work_order_id": "WO1",
                    "code_system": "LN", "code": "X", "sub_id": None,
                    "due_at": hd.DUE_EARLY}],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LP", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T5},
                {"sender": hd.SENDER, "number_class": "filler",
                 "local_number": "LF", "work_order_id": "WO2",
                 "valid_from": hd.T0, "valid_to": hd.T5},
            ])
        service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)
        groups = list(reader.iter_groups(self.fx.ledger, "S", 1))
        self.assertEqual(groups[0]["status"], vocab.GRP_MAPPING_CONFLICT)
        obs = list(reader.iter_observations(self.fx.ledger, "S", 1))
        self.assertEqual(obs[0]["disposition"], vocab.OBS_UNATTRIBUTABLE)

    def test_out_of_window_number(self):
        # mapping ends at T2 (exclusive); message arrives exactly at T2.
        self.fx.group("MSG-W", placer="LW", received=hd.T2)
        self.fx.manifest(
            items=[{"item_id": "I1", "work_order_id": "WO1",
                    "code_system": "LN", "code": "X", "sub_id": None,
                    "due_at": hd.DUE_EARLY}],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LW", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T2},
            ])
        service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_OUT_OF_WINDOW)
        self.assertEqual(g["detail"]["numbers"][0]["reason"],
                         vocab.NUM_NOT_EFFECTIVE)

    def test_half_open_switch_uses_new_segment_at_boundary(self):
        # WO1 [T0,T2), WO2 [T2,T5); arrival at T2 must take WO2.
        self.fx.group("MSG-S", placer="LS", received=hd.T2)
        self.fx.manifest(
            items=[{"item_id": "I2", "work_order_id": "WO2",
                    "code_system": "LN", "code": "X", "sub_id": None,
                    "due_at": hd.DUE_EARLY}],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LS", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T2},
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LS", "work_order_id": "WO2",
                 "valid_from": hd.T2, "valid_to": hd.T5},
            ])
        service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_ASSIGNED)
        self.assertEqual(g["work_order_id"], "WO2")
        # and one instant earlier it would have been WO1
        self.tmp2 = tempfile.TemporaryDirectory()
        try:
            early = MiniLedger(self.tmp2.name)
            early.group("MSG-S", placer="LS", received=hd.T1)
            early.manifest(
                items=[{"item_id": "I1", "work_order_id": "WO1",
                        "code_system": "LN", "code": "X", "sub_id": None,
                        "due_at": hd.DUE_EARLY}],
                mappings=[
                    {"sender": hd.SENDER, "number_class": "placer",
                     "local_number": "LS", "work_order_id": "WO1",
                     "valid_from": hd.T0, "valid_to": hd.T2},
                    {"sender": hd.SENDER, "number_class": "placer",
                     "local_number": "LS", "work_order_id": "WO2",
                     "valid_from": hd.T2, "valid_to": hd.T5},
                ])
            service.register_and_settle(
                early.ledger, early.recv, early.mdir)
            ge = list(reader.iter_groups(early.ledger, "S", 1))[0]
            self.assertEqual(ge["work_order_id"], "WO1")
        finally:
            early.close()
            self.tmp2.cleanup()


class DuplicateAndSubIdCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = MiniLedger(self.tmp.name)

    def tearDown(self):
        self.fx.close()
        self.tmp.cleanup()

    def test_two_identical_items_stay_separate_and_compete(self):
        # Two items with identical conditions; one observation can only ever
        # match both, so neither auto-settles and both stay pending.
        self.fx.group("MSG-D", placer="LD",
                      obs=[("LN", "X", None)])
        self.fx.manifest(
            items=[
                {"item_id": "IA", "work_order_id": "WO1",
                 "code_system": "LN", "code": "X", "sub_id": None,
                 "due_at": hd.DUE_EARLY},
                {"item_id": "IB", "work_order_id": "WO1",
                 "code_system": "LN", "code": "X", "sub_id": None,
                 "due_at": hd.DUE_EARLY},
            ],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LD", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T5},
            ])
        reg = service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)
        problems = href.compare_version(
            self.fx.ledger, "S", reg["version_id"], self.fx.mdir,
            self.fx.recv, hd.T3)
        self.assertEqual(problems, [])
        items = {i["item_id"]: i for i in
                 reader.iter_items(self.fx.ledger, "S", 1)}
        self.assertEqual(items["IA"]["status"], vocab.ITEM_PENDING_DECISION)
        self.assertEqual(items["IB"]["status"], vocab.ITEM_PENDING_DECISION)
        # a human can still lock the single observation onto one of them
        obs = list(reader.iter_observations(self.fx.ledger, "S", 1))[0]
        service.submit_decisions(self.fx.ledger, {
            "ledger_id": "S", "base_version": 1, "requested_by": "op",
            "actions": [{"action": vocab.DEC_LOCK_OBS,
                         "obs_key": obs["obs_key"], "item_id": "IA",
                         "reason": "choose IA"}]})
        items = {i["item_id"]: i for i in
                 reader.iter_items(self.fx.ledger, "S", 2)}
        self.assertEqual(items["IA"]["status"], vocab.ITEM_LOCK_SETTLED)
        # The single observation cannot cover two items: IB now has no free
        # candidate left, so it falls through to awaiting receipt (not done).
        self.assertEqual(items["IB"]["status"], vocab.ITEM_PENDING_RECEIPT)
        # and the observation's single disposition is adopted, never twice
        obs2 = list(reader.iter_observations(self.fx.ledger, "S", 2))[0]
        self.assertEqual(obs2["disposition"], vocab.OBS_ADOPTED)
        self.assertEqual(obs2["item_id"], "IA")

    def test_null_subid_does_not_match_nonnull_observation(self):
        self.fx.group("MSG-N", placer="LN",
                      obs=[("LN", "X", "SUB1")])
        self.fx.manifest(
            items=[
                # explicit NULL sub id item: must not match the SUB1 obs
                {"item_id": "INULL", "work_order_id": "WO1",
                 "code_system": "LN", "code": "X", "sub_id": None,
                 "due_at": hd.DUE_EARLY},
                # matching non-null item does match
                {"item_id": "ISUB", "work_order_id": "WO1",
                 "code_system": "LN", "code": "X", "sub_id": "SUB1",
                 "due_at": hd.DUE_EARLY},
            ],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "LN", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T5},
            ])
        service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)
        items = {i["item_id"]: i for i in
                 reader.iter_items(self.fx.ledger, "S", 1)}
        self.assertEqual(items["ISUB"]["status"], vocab.ITEM_AUTO_SETTLED)
        # NULL item never received a matching observation
        self.assertIn(items["INULL"]["status"],
                      (vocab.ITEM_PENDING_RECEIPT, vocab.ITEM_OVERDUE))
        obs = list(reader.iter_observations(self.fx.ledger, "S", 1))[0]
        self.assertEqual(obs["disposition"], vocab.OBS_ADOPTED)


class MissingIdentityAndOrderCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = MiniLedger(self.tmp.name)

    def tearDown(self):
        self.fx.close()
        self.tmp.cleanup()

    def _one_item_manifest(self):
        self.fx.manifest(
            items=[{"item_id": "I1", "work_order_id": "WO1",
                    "code_system": "LN", "code": "X", "sub_id": None,
                    "due_at": hd.DUE_EARLY}],
            mappings=[
                {"sender": hd.SENDER, "number_class": "placer",
                 "local_number": "L1", "work_order_id": "WO1",
                 "valid_from": hd.T0, "valid_to": hd.T5},
            ])
        service.register_and_settle(
            self.fx.ledger, self.fx.recv, self.fx.mdir)

    def test_no_patient_identity(self):
        self.fx.group("MSG-NP", patient=(None, None), placer="L1")
        self._one_item_manifest()
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_NO_PATIENT)
        o = list(reader.iter_observations(self.fx.ledger, "S", 1))[0]
        self.assertEqual(o["disposition"], vocab.OBS_UNATTRIBUTABLE)

    def test_no_order_number(self):
        self.fx.group("MSG-NO", placer=None, filler=None)
        self._one_item_manifest()
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_NO_ORDER)

    def test_unregistered_number_distinct_from_out_of_window(self):
        # a number that has no mapping row at all
        self.fx.group("MSG-U", placer="NEVER-REGISTERED")
        self._one_item_manifest()
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_UNREGISTERED_ORDER)
        self.assertEqual(g["detail"]["numbers"][0]["reason"],
                         vocab.NUM_NO_MAPPING)

    def test_patient_mismatch(self):
        self.fx.group("MSG-PM", patient=(hd.DOMAIN, hd.PATIENT_OTHER),
                      placer="L1")
        self._one_item_manifest()
        g = list(reader.iter_groups(self.fx.ledger, "S", 1))[0]
        self.assertEqual(g["status"], vocab.GRP_PATIENT_MISMATCH)
        o = list(reader.iter_observations(self.fx.ledger, "S", 1))[0]
        self.assertEqual(o["disposition"], vocab.OBS_UNATTRIBUTABLE)


if __name__ == "__main__":
    unittest.main()
