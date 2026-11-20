"""Cut-off extension and fact-batch reuse semantics.

These pin the versioning/input contract:

* a normal decision re-settles on the *same* fact batch, so facts that
  arrived after the cut-off never leak in; only an explicit extend reads a
  new batch;
* an extend accepts withdrawals of manual decisions in the same request and
  records them on the append-only log;
* a stale base_version is rejected on extend as on decide;
* old versions keep their original batch and per-item evidence.
"""

from __future__ import annotations

import tempfile
import unittest

from drills import handover_drill as hd
from shiftledger import reader, service, vocab
from shiftledger.errors import StaleVersionError


class ExtendAndBatchReuse(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = hd.build_acceptance_fixture(self.tmp.name)
        service.register_and_settle(
            self.fx["ledger_db"], self.fx["receiver_db"],
            self.fx["manifest_dir"], cutoff=hd.T3)

    def tearDown(self):
        self.tmp.cleanup()

    def _batch_of(self, version):
        return next(v["batch_id"] for v in
                    reader.list_versions(self.fx["ledger_db"], "SHIFT-1")
                    if v["version_id"] == version)

    def test_decisions_reuse_same_batch_until_explicit_extend(self):
        # A late observation arrives after the v1 cut-off but is NOT yet
        # visible: a plain decision stays on batch 1.
        late_obs = hd.add_late_x_for_wo1(self.fx["receiver_db"])
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1, "requested_by": "op",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r"}]})
        self.assertEqual(self._batch_of(2), self._batch_of(1))
        # the late observation still does not exist in version 2's view
        obs2 = {o["obs_key"] for o in
                reader.iter_observations(self.fx["ledger_db"], "SHIFT-1", 2)}
        self.assertNotIn(late_obs, obs2)

        # Only the explicit extend reads a fresh batch containing the late obs.
        ext = service.extend_cutoff(
            self.fx["ledger_db"], self.fx["receiver_db"],
            ledger_id="SHIFT-1", base_version=2, new_cutoff=hd.T5,
            requested_by="op")
        self.assertEqual(ext["version_id"], 3)
        self.assertNotEqual(self._batch_of(3), self._batch_of(2))
        obs3 = {o["obs_key"] for o in
                reader.iter_observations(self.fx["ledger_db"], "SHIFT-1", 3)}
        self.assertIn(late_obs, obs3)

        # A follow-up decision reuses the extend's new batch (no further leak).
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 3, "requested_by": "op",
            "actions": [{"action": vocab.DEC_LOCK_OBS, "obs_key": late_obs,
                         "item_id": "WO1-X", "reason": "late lock"}]})
        self.assertEqual(self._batch_of(4), self._batch_of(3))

    def test_extend_accepts_withdrawal_in_same_request(self):
        # Build a lock first, then extend while withdrawing that lock in the
        # same request; the withdrawal is durably recorded and no lock remains.
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1, "requested_by": "op",
            "actions": [
                {"action": vocab.DEC_ASSIGN_GROUP,
                 "group_key": self.fx["keys"]["shared_group"],
                 "work_order_id": "WO2", "reason": "assign"},
                {"action": vocab.DEC_LOCK_OBS,
                 "obs_key": self.fx["keys"]["shared_x_obs"],
                 "item_id": "WO2-X", "reason": "lock"},
            ]})
        ext = service.extend_cutoff(
            self.fx["ledger_db"], self.fx["receiver_db"],
            ledger_id="SHIFT-1", base_version=2, new_cutoff=hd.T5,
            requested_by="op",
            withdrawals=[{"action": vocab.DEC_WITHDRAW_LOCK,
                          "obs_key": self.fx["keys"]["shared_x_obs"],
                          "reason": "release before extend"}])
        self.assertEqual(ext["version_id"], 3)
        log = reader.list_decisions(self.fx["ledger_db"], "SHIFT-1")
        self.assertIn(vocab.DEC_WITHDRAW_LOCK, [d["action"] for d in log])
        # WO2-X is no longer lock-settled in the extended version
        item = next(i for i in reader.iter_items(
            self.fx["ledger_db"], "SHIFT-1", 3) if i["item_id"] == "WO2-X")
        self.assertNotEqual(item["status"], vocab.ITEM_LOCK_SETTLED)

    def test_extend_rejects_stale_base_version(self):
        service.submit_decisions(self.fx["ledger_db"], {
            "ledger_id": "SHIFT-1", "base_version": 1, "requested_by": "op",
            "actions": [{"action": vocab.DEC_ASSIGN_GROUP,
                         "group_key": self.fx["keys"]["shared_group"],
                         "work_order_id": "WO2", "reason": "r"}]})
        # head is now v2; an extend based on v1 must be rejected
        with self.assertRaises(StaleVersionError):
            service.extend_cutoff(
                self.fx["ledger_db"], self.fx["receiver_db"],
                ledger_id="SHIFT-1", base_version=1, new_cutoff=hd.T5,
                requested_by="op")
        # and no new version was created
        versions = reader.list_versions(self.fx["ledger_db"], "SHIFT-1")
        self.assertEqual(versions[-1]["version_id"], 2)

    def test_extend_cannot_move_cutoff_backwards(self):
        from shiftledger.errors import ManifestError
        with self.assertRaises(ManifestError):
            service.extend_cutoff(
                self.fx["ledger_db"], self.fx["receiver_db"],
                ledger_id="SHIFT-1", base_version=1, new_cutoff=hd.T1,
                requested_by="op")


if __name__ == "__main__":
    unittest.main()
