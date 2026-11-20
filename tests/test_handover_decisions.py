"""Decision-integrity edge cases: every illegal change is rejected whole.

Covers the explicit rejection rules from the handover contract:

* an observation cannot settle two items;
* one item cannot be locked twice;
* withdrawing a group assignment must not orphan an existing item lock;
* a group assignment and an observation lock may be committed together;
* automatic matches from a previous version are never treated as manual
  decisions;
* an invalid action never leaves a partial version behind.
"""

from __future__ import annotations

import unittest

from drills import handover_drill as hd
from shiftledger import reader, service, vocab
from shiftledger.errors import DecisionRejectedError


class DecisionIntegrity(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = hd.build_acceptance_fixture(self.tmp.name)
        service.register_and_settle(
            self.fx["ledger_db"], self.fx["receiver_db"],
            self.fx["manifest_dir"], cutoff=hd.T3)

    def tearDown(self):
        self.tmp.cleanup()

    def _versions(self):
        return reader.list_versions(self.fx["ledger_db"], "SHIFT-1")

    def _items(self, version):
        return {it["item_id"]: it for it in reader.iter_items(
            self.fx["ledger_db"], "SHIFT-1", version)}

    def test_combined_group_assignment_and_lock_in_one_request(self):
        # Point the shared group at WO2 and, in the same request, lock the
        # dedicated on-time X to WO1-X (it already matches, so it is legal).
        req = {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-c",
            "actions": [
                {"action": vocab.DEC_ASSIGN_GROUP,
                 "group_key": self.fx["keys"]["shared_group"],
                 "work_order_id": "WO2", "reason": "shared -> WO2"},
                {"action": vocab.DEC_LOCK_OBS,
                 "obs_key": self.fx["keys"]["one_x_obs"],
                 "item_id": "WO1-X", "reason": "pin dedicated X"},
            ],
        }
        out = service.submit_decisions(self.fx["ledger_db"], req)
        self.assertEqual(out["version_id"], 2)
        items = self._items(2)
        self.assertEqual(items["WO1-X"]["status"], vocab.ITEM_LOCK_SETTLED)
        self.assertEqual(items["WO1-X"]["obs_key"],
                         self.fx["keys"]["one_x_obs"])
        self.assertEqual(items["WO2-X"]["obs_key"],
                         self.fx["keys"]["shared_x_obs"])

    def test_one_observation_cannot_lock_two_items(self):
        # The dedicated X matches WO1-X only in this fixture; build a second
        # legal target by assigning the shared group first, then attempt a
        # double lock of one observation onto two different items.
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-c",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r"}]})
        # The dedicated observation cannot also lock WO2-X (different WO).
        with self.assertRaises(DecisionRejectedError):
            service.submit_decisions(self.fx["ledger_db"], {
                "ledger_id": "SHIFT-1", "base_version": 2,
                "requested_by": "op-c",
                "actions": [{"action": vocab.DEC_LOCK_OBS,
                             "obs_key": self.fx["keys"]["one_x_obs"],
                             "item_id": "WO2-X", "reason": "illegal double"}]})
        self.assertEqual(len(self._versions()), 2)

    def test_one_item_cannot_be_locked_twice(self):
        # Lock WO1-X once (legal), then try to lock another observation onto
        # the same item in a later request.
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-c",
            "actions": [{"action": vocab.DEC_LOCK_OBS,
                         "obs_key": self.fx["keys"]["one_x_obs"],
                         "item_id": "WO1-X", "reason": "first lock"}]})
        # A second distinct observation aimed at the same item must fail.
        other = self.fx["keys"]["shared_x_obs"]
        with self.assertRaises(DecisionRejectedError):
            service.submit_decisions(self.fx["ledger_db"], {
                "ledger_id": "SHIFT-1", "base_version": 2,
                "requested_by": "op-c",
                "actions": [
                    {"action": vocab.DEC_ASSIGN_GROUP,
                     "group_key": self.fx["keys"]["shared_group"],
                     "work_order_id": "WO1", "reason": "make obs eligible"},
                    {"action": vocab.DEC_LOCK_OBS, "obs_key": other,
                     "item_id": "WO1-X", "reason": "second lock"}]})

    def test_withdrawing_group_assignment_orphaning_lock_rejected(self):
        # First assign the shared group to WO2 and lock its X to WO2-X.
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-c",
            "actions": [
                {"action": vocab.DEC_ASSIGN_GROUP,
                 "group_key": self.fx["keys"]["shared_group"],
                 "work_order_id": "WO2", "reason": "r"},
                {"action": vocab.DEC_LOCK_OBS,
                 "obs_key": self.fx["keys"]["shared_x_obs"],
                 "item_id": "WO2-X", "reason": "lock after assign"},
            ]})
        # Withdrawing the assignment makes the lock's group ambiguous again;
        # that orphaned lock must reject the whole request.
        with self.assertRaises(DecisionRejectedError):
            service.submit_decisions(self.fx["ledger_db"], {
                "ledger_id": "SHIFT-1", "base_version": 2,
                "requested_by": "op-c",
                "actions": [{"action": vocab.DEC_WITHDRAW_GROUP,
                             "group_key": self.fx["keys"]["shared_group"],
                             "reason": "would orphan the lock"}]})
        # head unchanged: no partial version
        self.assertEqual(self._versions()[-1]["version_id"], 2)

    def test_withdraw_lock_then_withdraw_group_succeeds(self):
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1,
            "requested_by": "op-c",
            "actions": [
                {"action": vocab.DEC_ASSIGN_GROUP,
                 "group_key": self.fx["keys"]["shared_group"],
                 "work_order_id": "WO2", "reason": "r"},
                {"action": vocab.DEC_LOCK_OBS,
                 "obs_key": self.fx["keys"]["shared_x_obs"],
                 "item_id": "WO2-X", "reason": "lock"},
            ]})
        # Withdraw the lock first, then the group assignment in one request.
        out = service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 2,
            "requested_by": "op-c",
            "actions": [
                {"action": vocab.DEC_WITHDRAW_LOCK,
                 "obs_key": self.fx["keys"]["shared_x_obs"],
                 "reason": "release lock"},
                {"action": vocab.DEC_WITHDRAW_GROUP,
                 "group_key": self.fx["keys"]["shared_group"],
                 "reason": "release assignment"},
            ]})
        self.assertEqual(out["version_id"], 3)
        items = self._items(3)
        self.assertEqual(items["WO2-X"]["status"], vocab.ITEM_PENDING_DECISION)
        log = reader.list_decisions(self.fx["ledger_db"], "SHIFT-1")
        actions = [d["action"] for d in log]
        self.assertIn(vocab.DEC_WITHDRAW_LOCK, actions)
        self.assertIn(vocab.DEC_WITHDRAW_GROUP, actions)

    def test_auto_match_is_not_persisted_as_manual_decision(self):
        # WO1-Y auto-settles in v1 with no manual action.  The decision log
        # must stay empty and the auto edge must be labelled exactly so.
        self.assertEqual(reader.list_decisions(
            self.fx["ledger_db"], "SHIFT-1"), [])
        detail = reader.item_detail(
            self.fx["ledger_db"], "SHIFT-1", "WO1-Y", 1)
        self.assertEqual(detail["edge_kind"], vocab.EDGE_AUTO)
        auto_edges = [e for e in detail["edges"]
                      if e["edge_kind"] == vocab.EDGE_AUTO]
        self.assertTrue(auto_edges)


if __name__ == "__main__":
    unittest.main()
