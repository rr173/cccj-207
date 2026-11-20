"""End-to-end handover ledger acceptance, enumerated item by item.

The scenario mirrors the operations story: three handover entries, two of
them (WO1/WO2) waiting on the same X and a third (WO3) with no matching
return; a shared number points at the first two and a dedicated number only
at the first.  Every expectation below is hand-derived and additionally
cross-checked by the independent reference checker, which does not import
the reconciliation engine.  We assert individual rows, not only totals.
"""

from __future__ import annotations

import os
import tempfile
import unittest

from drills import handover_drill as hd
from refcheck import handover_reference as href
from shiftledger import reader, service, vocab
from shiftledger.errors import (
    DecisionRejectedError,
    ManifestError,
    StaleVersionError,
)


class HandoverAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = hd.build_acceptance_fixture(self.tmp.name)
        # Every test starts from a freshly registered v1; tests that need a
        # different cut-off simply re-register.
        self._register()

    def tearDown(self):
        self.tmp.cleanup()

    def _register(self, cutoff=hd.T3):
        # Idempotent within one test: drop any ledger from an earlier call.
        for suffix in ("", "-wal", "-shm"):
            p = self.fx["ledger_db"] + suffix
            if os.path.exists(p):
                os.remove(p)
        return service.register_and_settle(
            self.fx["ledger_db"], self.fx["receiver_db"],
            self.fx["manifest_dir"], cutoff=cutoff)

    def _items(self, version=None):
        return {it["item_id"]: it for it in reader.iter_items(
            self.fx["ledger_db"], "SHIFT-1", version)}

    def _groups(self, version=None):
        return {g["group_key"]: g for g in reader.iter_groups(
            self.fx["ledger_db"], "SHIFT-1", version)}

    def _obs(self, version=None):
        return {o["obs_key"]: o for o in reader.iter_observations(
            self.fx["ledger_db"], "SHIFT-1", version)}

    # -- registration: no automatic close while the shared group is open --
    def test_initial_version_blocks_both_x_items(self):
        reg = self._register()
        self.assertEqual(reg["version_id"], 1)
        items = self._items(1)
        # Neither X may auto-settle: the shared-number X is an ambiguous
        # group that could satisfy either, blocking both WO1-X and WO2-X.
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_PENDING_DECISION)
        self.assertEqual(items["WO2-X"]["status"], vocab.ITEM_PENDING_DECISION)
        # The dedicated X is a candidate but stays unassigned.
        obs = self._obs(1)
        self.assertEqual(
            obs[self.fx["keys"]["one_x_obs"]]["disposition"],
            vocab.OBS_CANDIDATE_UNASSIGNED)
        # The shared group is ambiguous across exactly WO1 and WO2.
        groups = self._groups(1)
        shared = groups[self.fx["keys"]["shared_group"]]
        self.assertEqual(shared["status"], vocab.GRP_AMBIGUOUS)
        self.assertEqual(
            sorted(shared["detail"]["candidate_work_orders"]), ["WO1", "WO2"])
        # Shared X observation is group_pending.
        self.assertEqual(
            obs[self.fx["keys"]["shared_x_obs"]]["disposition"],
            vocab.OBS_GROUP_PENDING)

    def test_third_work_order_pending_vs_overdue(self):
        items = self._items(1)
        # cutoff T3 (11:00) <= due 12:00 -> still awaiting receipt
        self.assertEqual(items["WO3-Z-OPEN"]["status"],
                         vocab.ITEM_PENDING_RECEIPT)
        # cutoff 11:00 > due 09:30 -> overdue, no candidate ever arrived
        self.assertEqual(items["WO3-Z-LATE"]["status"], vocab.ITEM_OVERDUE)

    def test_two_item_group_settles_both_items(self):
        # MSG-TWO is uniquely WO1 and carries Y and Z(S1); both items are
        # unique 1:1, so they auto-settle even though they share one group.
        items = self._items(1)
        self.assertEqual(items["WO1-Y"]["status"], vocab.ITEM_AUTO_SETTLED)
        self.assertEqual(items["WO1-ZS1"]["status"], vocab.ITEM_AUTO_SETTLED)
        self.assertEqual(items["WO1-Y"]["timeliness"], vocab.SETTLED_ON_TIME)

    def test_equal_subid_competition_stays_pending(self):
        # Two observations with the identical (Y, sub DUP) match one item:
        # no unique 1:1, so the item waits for adjudication; both obs show
        # candidate_unassigned.
        items = self._items(1)
        self.assertEqual(items["WO2-YDUP"]["status"],
                         vocab.ITEM_PENDING_DECISION)
        obs = self._obs(1)
        comp_obs = [o for o, d in obs.items()
                    if d["disposition"] == vocab.OBS_CANDIDATE_UNASSIGNED
                    and "MSG-COMP" in o]
        self.assertEqual(len(comp_obs), 2)

    def test_unregistered_and_mismatch_and_segment_switch(self):
        groups = self._groups(1)
        unreg = next(g for k, g in groups.items() if "MSG-UNREG" in k)
        self.assertEqual(unreg["status"], vocab.GRP_UNREGISTERED_ORDER)
        mismatch = next(g for k, g in groups.items() if "MSG-MISMATCH" in k)
        self.assertEqual(mismatch["status"], vocab.GRP_PATIENT_MISMATCH)
        # L-SEG switches exactly at T2; the T2 message takes the new segment
        seg = groups[self.fx["keys"]["seg_group"]]
        self.assertEqual(seg["status"], vocab.GRP_ASSIGNED)
        self.assertEqual(seg["work_order_id"], "WO2")
        # the unattributable observations are labelled distinctly
        obs = self._obs(1)
        bad = [o for o, d in obs.items()
               if d["disposition"] == vocab.OBS_UNATTRIBUTABLE]
        self.assertTrue(any("MSG-UNREG" in o for o in bad))
        self.assertTrue(any("MSG-MISMATCH" in o for o in bad))

    # -- adjudication: assign the shared group to WO2, then verify sources --
    def test_assign_shared_group_then_distinct_settlement_sources(self):
        self._register()
        req = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-zhang",
            "actions": [{
                "action": vocab.DEC_ASSIGN_GROUP,
                "group_key": self.fx["keys"]["shared_group"],
                "work_order_id": "WO2",
                "reason": "shared instrument number confirmed for WO2",
            }],
        }
        out = service.submit_decisions(self.fx["ledger_db"], req)
        self.assertEqual(out["version_id"], 2)
        items = self._items(2)
        # WO2's X is settled by the shared-group observation (manual path).
        self.assertEqual(items["WO2-X"]["status"], vocab.ITEM_AUTO_SETTLED)
        self.assertEqual(items["WO2-X"]["obs_key"],
                         self.fx["keys"]["shared_x_obs"])
        # WO1's X is now uniquely served by the dedicated on-time observation.
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_AUTO_SETTLED)
        self.assertEqual(items["WO1-X"]["obs_key"],
                         self.fx["keys"]["one_x_obs"])
        self.assertEqual(items["WO1-X"]["timeliness"], vocab.SETTLED_ON_TIME)
        # decisions are durable with reason and author
        log = reader.list_decisions(self.fx["ledger_db"], "SHIFT-1")
        self.assertEqual(log[0]["requested_by"], "op-zhang")
        self.assertIn("confirmed", log[0]["reason"])

    def test_stale_version_rejected_and_two_concurrent_decisions(self):
        self._register()
        req1 = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-a",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "pick WO2"}],
        }
        service.submit_decisions(self.fx["ledger_db"], req1)
        # A second, different decision based on the same old v1 must fail.
        req2 = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-b",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO1", "reason": "pick WO1 instead"}],
        }
        with self.assertRaises(StaleVersionError):
            service.submit_decisions(self.fx["ledger_db"], req2)
        # head stays at v2; the stale choice never landed
        items = self._items(2)
        self.assertEqual(items["WO2-X"]["obs_key"],
                         self.fx["keys"]["shared_x_obs"])

    def test_illegal_assignment_rejected_whole(self):
        self._register()
        # the dedicated group is uniquely WO1; forcing it to WO2 is illegal
        req = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-li",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["one_group"],
                         "work_order_id": "WO2",
                         "reason": "attempt illegal edge"}],
        }
        with self.assertRaises(DecisionRejectedError):
            service.submit_decisions(self.fx["ledger_db"], req)
        # no new version was published
        versions = reader.list_versions(self.fx["ledger_db"], "SHIFT-1")
        self.assertEqual(len(versions), 1)

    def test_withdraw_group_assignment_returns_to_ambiguous(self):
        self._register()
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-z",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r1"}]})
        out = service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 2,
            "requested_by": "op-z",
            "actions": [{"action": vocab.DEC_WITHDRAW_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "reason": "new evidence, reopen"}]})
        self.assertEqual(out["version_id"], 3)
        items = self._items(3)
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_PENDING_DECISION)
        self.assertEqual(items["WO2-X"]["status"], vocab.ITEM_PENDING_DECISION)

    # -- extend cutoff: late dedicated X reopens WO1, then lock settles it --
    def test_extend_then_lock_late_observation(self):
        self._register()
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-z",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r1"}]})
        late_obs = hd.add_late_x_for_wo1(self.fx["receiver_db"])
        # Extend to T5: the late X (T4) is a second candidate for WO1, so the
        # previous auto write-off must turn back into pending_decision.
        ext = service.extend_cutoff(
            self.fx["ledger_db"], self.fx["receiver_db"],
            ledger_id="SHIFT-1", base_version=2, new_cutoff=hd.T5,
            requested_by="op-z")
        self.assertEqual(ext["version_id"], 3)
        items = self._items(3)
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_PENDING_DECISION)
        # old version keeps its original source evidence
        old = self._items(2)
        self.assertEqual(old["WO1-X"]["obs_key"],
                         self.fx["keys"]["one_x_obs"])
        # lock the late observation onto WO1-X -> late write-off
        out = service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 3,
            "requested_by": "op-z",
            "actions": [{"action": vocab.DEC_LOCK_OBS,
                         "obs_key": late_obs, "item_id": "WO1-X",
                         "reason": "accept the late dedicated result"}]})
        self.assertEqual(out["version_id"], 4)
        items = self._items(4)
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_LOCK_SETTLED)
        self.assertEqual(items["WO1-X"]["obs_key"], late_obs)
        self.assertEqual(items["WO1-X"]["timeliness"], vocab.SETTLED_LATE)
        # the earlier on-time X is now "all matching items occupied"
        obs = self._obs(4)
        self.assertEqual(
            obs[self.fx["keys"]["one_x_obs"]]["disposition"],
            vocab.OBS_ITEMS_OCCUPIED)

    def test_lock_to_nonmatching_item_rejected(self):
        self._register()
        # the dedicated X observation cannot be locked to a Y item
        req = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-q",
            "actions": [{"action": vocab.DEC_LOCK_OBS,
                         "obs_key": self.fx["keys"]["one_x_obs"],
                         "item_id": "WO1-Y", "reason": "wrong edge"}],
        }
        with self.assertRaises(DecisionRejectedError):
            service.submit_decisions(self.fx["ledger_db"], req)

    # -- independent enumeration must agree on every row of v1 and v2 -----
    def test_reference_checker_matches_every_row(self):
        reg = self._register()
        problems = href.compare_version(
            self.fx["ledger_db"], "SHIFT-1", reg["version_id"],
            self.fx["manifest_dir"], self.fx["receiver_db"], hd.T3)
        self.assertEqual(problems, [])
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-z",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r1"}]})
        problems = href.compare_version(
            self.fx["ledger_db"], "SHIFT-1", 2,
            self.fx["manifest_dir"], self.fx["receiver_db"], hd.T3,
            active_assign={self.fx["keys"]["shared_group"]: "WO2"})
        self.assertEqual(problems, [])


class ManifestValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = hd.build_acceptance_fixture(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_naive_timestamp_rejected(self):
        mdir = os.path.join(self.tmp.name, "bad-manifest")
        doc = {"shift_id": "S", "shift_start": "2026-11-20T08:00:00",
               "cutoff": hd.T3, "senders": [hd.SENDER]}
        hd.write_manifest(mdir, doc,
                          [{"work_order_id": "WO1", "patient_domain": hd.DOMAIN,
                            "patient_id": hd.PATIENT}],
                          [{"item_id": "I1", "work_order_id": "WO1",
                            "code_system": hd.LOINC, "code": "X",
                            "sub_id": None, "due_at": hd.DUE_EARLY}], [])
        with self.assertRaises(ManifestError):
            service.register_and_settle(
                os.path.join(self.tmp.name, "l.db"),
                self.fx["receiver_db"], mdir)

    def test_unknown_work_order_reference_rejected(self):
        mdir = os.path.join(self.tmp.name, "bad-ref")
        doc = {"shift_id": "S", "shift_start": hd.T0, "cutoff": hd.T3,
               "senders": [hd.SENDER]}
        hd.write_manifest(mdir, doc,
                          [{"work_order_id": "WO1", "patient_domain": hd.DOMAIN,
                            "patient_id": hd.PATIENT}],
                          [{"item_id": "I1", "work_order_id": "WO-GHOST",
                            "code_system": hd.LOINC, "code": "X",
                            "sub_id": None, "due_at": hd.DUE_EARLY}], [])
        with self.assertRaises(ManifestError):
            service.register_and_settle(
                os.path.join(self.tmp.name, "l.db"),
                self.fx["receiver_db"], mdir)

    def test_cutoff_not_after_start_rejected(self):
        with self.assertRaises(ManifestError):
            self._register_with_cutoff(hd.T0)

    def _register_with_cutoff(self, cutoff):
        return service.register_and_settle(
            os.path.join(self.tmp.name, "l.db"),
            self.fx["receiver_db"], self.fx["manifest_dir"], cutoff=cutoff)


if __name__ == "__main__":
    unittest.main()
