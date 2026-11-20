"""CLI surface tests: serve banner, query commands, peer CLI, raise faults."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest

from peer.client import send_message
from tests import fixtures as fx
from tests.test_recovery import ROOT, start_server, wait_exit


def run_cli(args: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hl7recv", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=30, **kw,
    )


def run_peer(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "peer", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=30,
    )


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cli.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_serve_query_roundtrip(self):
        proc, host, port = start_server(self.db)
        f1 = os.path.join(self.tmp.name, "m1.hl7")
        with open(f1, "wb") as fh:
            fh.write(fx.MSG1)
        rc = run_peer(["--port", str(port), "--file", f1, "--expect", "AA"])
        self.assertEqual(rc.returncode, 0, rc.stderr)

        out = run_cli(["query", "--db", self.db, "messages"])
        self.assertEqual(out.returncode, 0, out.stderr)
        messages = json.loads(out.stdout)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["control_id"], "MSG00001")
        self.assertEqual(messages[0]["n_results"], 4)

        out = run_cli(
            ["query", "--db", self.db, "results", "--message", "LAB1^HOSP:MSG00001"]
        )
        rows = json.loads(out.stdout)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["obs_code"], "WBC")

        out = run_cli(["query", "--db", self.db, "results", "--patient", "HOSP:P12345"])
        self.assertEqual(len(json.loads(out.stdout)), 4)

        out = run_cli(["query", "--db", self.db, "results", "--order", "FILLER100"])
        self.assertEqual(len(json.loads(out.stdout)), 2)

        out = run_cli(["query", "--db", self.db, "diagnostics"])
        diag = json.loads(out.stdout)
        self.assertEqual([r["state"] for r in diag["inbox"]], ["acked"])

        proc.send_signal(signal.SIGTERM)
        self.assertEqual(wait_exit(proc), 0)

    def test_query_results_requires_filter(self):
        proc, host, port = start_server(self.db)
        out = run_cli(["query", "--db", self.db, "results"])
        self.assertNotEqual(out.returncode, 0)
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    def test_peer_expect_none_on_reject(self):
        proc, host, port = start_server(self.db)
        f1 = os.path.join(self.tmp.name, "bad.hl7")
        with open(f1, "wb") as fh:
            fh.write(fx.MSG_INVALID_OBX)
        rc = run_peer(["--port", str(port), "--file", f1, "--expect", "AE"])
        self.assertEqual(rc.returncode, 0, rc.stderr)
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    def test_raise_fault_means_no_success_ack_and_no_rows(self):
        # simulated internal failure at the commit boundary
        proc, host, port = start_server(self.db, ["before_commit:raise:1"])
        ack = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertNotEqual(ack.code, "AA", "no success ACK on internal failure")
        out = run_cli(["query", "--db", self.db, "messages"])
        self.assertEqual(json.loads(out.stdout), [])
        # service keeps working: a later frame is processed normally
        ack2 = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertEqual(ack2.code, "AA")
        out = run_cli(["query", "--db", self.db, "messages"])
        self.assertEqual(len(json.loads(out.stdout)), 1)
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    def test_raise_fault_after_commit_keeps_committed_truth(self):
        # commit succeeded but the ACK path failed: the attempt must stay
        # 'committed' (not be rewritten as rejected) and a resend resolves
        # against the committed identity without duplicating rows
        proc, host, port = start_server(self.db, ["after_commit_before_ack:raise:1"])
        ack = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertNotEqual(ack.code, "AA")
        out = run_cli(["query", "--db", self.db, "messages"])
        self.assertEqual(len(json.loads(out.stdout)), 1)  # commit happened
        out = run_cli(["query", "--db", self.db, "diagnostics"])
        states = [r["state"] for r in json.loads(out.stdout)["inbox"]]
        self.assertEqual(states, ["committed"])
        ack2 = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertEqual(ack2.code, "AA")
        out = run_cli(["query", "--db", self.db, "messages"])
        self.assertEqual(len(json.loads(out.stdout)), 1)
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    def test_serve_bad_fault_spec_rejected(self):
        out = run_cli(
            ["serve", "--db", self.db, "--port", "0",
             "--fault-inject", "nonsense:exit:1"]
        )
        self.assertNotEqual(out.returncode, 0)


if __name__ == "__main__":
    unittest.main()
