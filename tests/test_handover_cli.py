"""End-to-end CLI tests for the handover ledger.

Drives the real command line (the ``bin/shiftledger`` wrapper, under
isolated Python) over a temporary synthetic fixture and asserts on the JSON
documents and process exit codes, including the stale-version exit code 5.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRAPPER = os.path.join(ROOT, "bin", "shiftledger")


class CliFlow(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        sys.path.insert(0, ROOT)
        from drills import handover_drill as hd  # noqa: E402
        self.hd = hd
        self.fx = hd.build_acceptance_fixture(self.tmp.name)
        self.ledger = os.path.join(self.tmp.name, "cli.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, *args) -> tuple[int, dict | list, str]:
        proc = subprocess.run(
            [sys.executable, "-I", WRAPPER, *args],
            capture_output=True, text=True,
        )
        stream = proc.stdout if proc.returncode == 0 else proc.stderr
        payload = json.loads(stream) if stream.strip() else None
        return proc.returncode, payload, proc.stderr

    def test_register_view_decide_stale_extend(self):
        rc, out, _ = self._run(
            "register", "--ledger-db", self.ledger,
            "--receiver-db", self.fx["receiver_db"],
            "--manifest-dir", self.fx["manifest_dir"])
        self.assertEqual(rc, 0)
        self.assertEqual(out["version_id"], 1)

        rc, overview, _ = self._run(
            "overview", "--ledger-db", self.ledger, "--ledger-id", "SHIFT-1")
        self.assertEqual(rc, 0)
        self.assertEqual(overview["version_id"], 1)
        self.assertEqual(
            overview["summary"]["item_counts"]["overdue"], 1)

        rc, items, _ = self._run(
            "items", "--ledger-db", self.ledger, "--ledger-id", "SHIFT-1",
            "--status", "pending_decision")
        self.assertEqual(rc, 0)
        ids = {it["item_id"] for it in items}
        self.assertIn("WO1-X", ids)
        self.assertIn("WO2-X", ids)

        # decision via a real request file containing the US separator byte
        gk = self.fx["keys"]["shared_group"]
        req_path = os.path.join(self.tmp.name, "dec.json")
        with open(req_path, "w", encoding="utf-8") as fh:
            json.dump({
                "ledger_id": "SHIFT-1", "base_version": 1,
                "requested_by": "op-cli",
                "actions": [{"action": "assign_group", "group_key": gk,
                             "work_order_id": "WO2", "reason": "cli decide"}]},
                fh)
        rc, out, _ = self._run(
            "decide", "--ledger-db", self.ledger, "--request", req_path)
        self.assertEqual(rc, 0)
        self.assertEqual(out["version_id"], 2)

        # a concurrent, different decision on the same old base is stale
        stale_path = os.path.join(self.tmp.name, "stale.json")
        with open(stale_path, "w", encoding="utf-8") as fh:
            json.dump({
                "ledger_id": "SHIFT-1", "base_version": 1,
                "requested_by": "op-other",
                "actions": [{"action": "assign_group", "group_key": gk,
                             "work_order_id": "WO1", "reason": "race"}]},
                fh)
        rc, err, _ = self._run(
            "decide", "--ledger-db", self.ledger, "--request", stale_path)
        self.assertEqual(rc, 5)
        self.assertEqual(err["error"], "stale_version")

        # extend cutoff after a late observation arrives
        late_obs = self.hd.add_late_x_for_wo1(self.fx["receiver_db"])
        ext_path = os.path.join(self.tmp.name, "ext.json")
        with open(ext_path, "w", encoding="utf-8") as fh:
            json.dump({
                "ledger_id": "SHIFT-1", "base_version": 2,
                "requested_by": "op-cli", "new_cutoff": self.hd.T5}, fh)
        rc, out, _ = self._run(
            "extend", "--ledger-db", self.ledger,
            "--receiver-db", self.fx["receiver_db"], "--request", ext_path)
        self.assertEqual(rc, 0)
        self.assertEqual(out["version_id"], 3)

        # old version still shows its original source
        rc, old_items, _ = self._run(
            "items", "--ledger-db", self.ledger, "--ledger-id", "SHIFT-1",
            "--version", "2")
        w1 = next(i for i in old_items if i["item_id"] == "WO1-X")
        self.assertEqual(w1["obs_key"], self.fx["keys"]["one_x_obs"])

        # decision log carries stable records with reasons
        rc, decisions, _ = self._run(
            "decisions", "--ledger-db", self.ledger, "--ledger-id", "SHIFT-1")
        self.assertEqual(rc, 0)
        self.assertTrue(any(d["reason"] == "cli decide" for d in decisions))

    def test_invalid_manifest_exit_usage(self):
        bad_dir = os.path.join(self.tmp.name, "bad")
        os.makedirs(bad_dir, exist_ok=True)
        with open(os.path.join(bad_dir, "manifest.json"), "w") as fh:
            json.dump({"shift_id": "S",
                       "shift_start": "2026-11-20T08:00:00",
                       "cutoff": self.hd.T3, "senders": [self.hd.SENDER]}, fh)
        for name in ("work_orders.jsonl", "items.jsonl", "mappings.jsonl"):
            open(os.path.join(bad_dir, name), "w").close()
        rc, err, _ = self._run(
            "register", "--ledger-db", os.path.join(self.tmp.name, "x.db"),
            "--receiver-db", self.fx["receiver_db"], "--manifest-dir", bad_dir)
        self.assertEqual(rc, 4)
        self.assertEqual(err["error"], "invalid_manifest")


if __name__ == "__main__":
    unittest.main()
