"""Process-exit drills at the commit/ACK boundary.

Each drill runs the receiver as a real subprocess with a deterministic
fault-injection point, kills it mid-flight, restarts it on the same
database and derives the only legal end state from whether the client
observed AA:

* crash BEFORE commit            -> client sees no AA; after restart and
                                    resend the message appears exactly once.
* crash AFTER commit, BEFORE ACK -> client sees no AA, but the message was
                                    committed; a resend resolves against the
                                    committed identity (AA, no duplicates).
* crash AFTER ACK                -> client saw AA; the message must be
                                    complete; a resend is a duplicate.
* crash DURING parse             -> nothing committed; the durable inbox
                                    attempt is recovered as incomplete.

Conflicts never overwrite: verified across a restart as well.
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from hl7recv.store import Store
from peer.client import send_message
from tests import fixtures as fx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def start_server(db: str, faults: list[str] | None = None, port: int = 0):
    """Start the receiver subprocess; return (proc, host, port)."""
    cmd = [
        sys.executable, "-m", "hl7recv", "serve",
        "--db", db, "--host", "127.0.0.1", "--port", str(port),
    ]
    for f in faults or []:
        cmd += ["--fault-inject", f]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=ROOT, text=True
    )
    ready, _, _ = select.select([proc.stdout], [], [], 10.0)
    if not ready:
        proc.kill()
        raise RuntimeError(f"server did not start: {proc.stderr.read()}")
    line = proc.stdout.readline().strip()
    if not line.startswith("LISTENING"):
        proc.kill()
        raise RuntimeError(f"unexpected server banner: {line!r}")
    _, host, port_s = line.split()
    return proc, host, int(port_s)


def wait_exit(proc: subprocess.Popen, timeout: float = 10.0) -> int:
    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise AssertionError("server process did not exit in time")
    for pipe in (proc.stdout, proc.stderr):
        try:
            if pipe:
                pipe.close()
        except OSError:
            pass
    return rc


class DrillCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "drill.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _send_expect_crash(self, host, port, payload):
        """Send one message; the server crashes at the injected point."""
        ack = send_message("127.0.0.1", port, payload, timeout=5.0)
        return ack

    # -- drill 1: crash before commit ---------------------------------------
    def test_crash_before_commit(self):
        proc, host, port = start_server(self.db, ["before_commit:exit:1"])
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertIsNone(ack.code, "no ACK may be observed when the commit never happened")
        self.assertEqual(wait_exit(proc), 70)

        # nothing committed
        store = Store(self.db)
        self.assertEqual(store.count_results(), 0)
        self.assertEqual(len(store.list_messages()), 0)
        store.close()

        # restart clean, resend: message becomes visible exactly once
        proc, host, port = start_server(self.db)
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA")
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(wait_exit(proc), 0)
        store = Store(self.db)
        self.assertEqual(store.count_results(), 4)
        self.assertEqual(len(store.list_messages()), 1)
        store.close()

    # -- drill 2: crash after commit, before ACK ------------------------------
    def test_crash_after_commit_before_ack(self):
        proc, host, port = start_server(self.db, ["after_commit_before_ack:exit:1"])
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertIsNone(ack.code, "ACK lost: client cannot know the commit succeeded")
        self.assertEqual(wait_exit(proc), 70)

        # restart: the committed message survived; attempt shows ack_lost
        proc, host, port = start_server(self.db)
        store = Store(self.db)
        self.assertEqual(store.count_results(), 4)
        states = [r["state"] for r in store.diagnostics()["inbox"]]
        self.assertIn("ack_lost", states)
        store.close()

        # client resends the identical frame -> AA via committed identity,
        # no duplicate insert
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA")
        store2 = Store(self.db)
        self.assertEqual(store2.count_results(), 4)
        self.assertEqual(len(store2.list_messages()), 1)
        states = [r["state"] for r in store2.diagnostics()["inbox"]]
        self.assertIn("duplicate", states)
        store2.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- drill 3: crash after ACK sent ----------------------------------------
    def test_crash_after_ack(self):
        proc, host, port = start_server(self.db, ["after_ack:exit:1"])
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA", "client observed AA before the crash")
        self.assertEqual(wait_exit(proc), 70)

        # AA observed -> the message must be complete after restart
        proc, host, port = start_server(self.db)
        store = Store(self.db)
        self.assertEqual(store.count_results(), 4)
        rows = store.results_for_message("LAB1^HOSP", "MSG00001")
        self.assertEqual(len(rows), 4)
        store.close()
        # resend -> duplicate, still exactly one message
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA")
        store2 = Store(self.db)
        self.assertEqual(store2.count_results(), 4)
        self.assertEqual(len(store2.list_messages()), 1)
        store2.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- drill 4: crash during parse -------------------------------------------
    def test_crash_during_parse_recovers_incomplete(self):
        proc, host, port = start_server(self.db, ["during_parse:exit:1"])
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertIsNone(ack.code)
        self.assertEqual(wait_exit(proc), 70)

        proc, host, port = start_server(self.db)
        store = Store(self.db)
        self.assertEqual(store.count_results(), 0)
        states = [r["state"] for r in store.diagnostics()["inbox"]]
        self.assertEqual(states, ["recovered_incomplete"])
        store.close()
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA")
        store2 = Store(self.db)
        self.assertEqual(store2.count_results(), 4)
        store2.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- drill 4b: crash right after a complete frame arrived -----------------
    def test_crash_after_frame_received(self):
        proc, host, port = start_server(self.db, ["after_frame:exit:1"])
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertIsNone(ack.code)
        self.assertEqual(wait_exit(proc), 70)
        # nothing was even attempted persistently -> clean resend works
        proc, host, port = start_server(self.db)
        store = Store(self.db)
        self.assertEqual(store.count_results(), 0)
        store.close()
        ack = self._send_expect_crash(host, port, fx.MSG1)
        self.assertEqual(ack.code, "AA")
        store2 = Store(self.db)
        self.assertEqual(store2.count_results(), 4)
        store2.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- drill 5: unconfirmed message visible exactly once after resend --------
    def test_unconfirmed_then_resend_exactly_once(self):
        # two different messages, both lost at after_commit_before_ack
        proc, host, port = start_server(
            self.db, ["after_commit_before_ack:exit:1"]
        )
        ack1 = self._send_expect_crash(host, port, fx.MSG1)
        self.assertIsNone(ack1.code)
        wait_exit(proc)
        # restart and immediately send MSG2 with the same fault (fires once)
        proc, host, port = start_server(
            self.db, ["after_commit_before_ack:exit:1"]
        )
        ack2 = self._send_expect_crash(host, port, fx.MSG2)
        self.assertIsNone(ack2.code)
        wait_exit(proc)

        # final restart, no faults: resend both originals
        proc, host, port = start_server(self.db)
        for payload in (fx.MSG1, fx.MSG2):
            ack = self._send_expect_crash(host, port, payload)
            self.assertEqual(ack.code, "AA")
        store = Store(self.db)
        self.assertEqual(len(store.list_messages()), 2)
        self.assertEqual(store.count_results(), 6)
        store.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- drill 6: conflict never overwrites, even across a crash ---------------
    def test_conflict_never_overwrites_across_restart(self):
        proc, host, port = start_server(self.db)
        self.assertEqual(self._send_expect_crash(host, port, fx.MSG1).code, "AA")
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

        proc, host, port = start_server(self.db)
        ack = self._send_expect_crash(host, port, fx.MSG1_CONFLICT)
        self.assertEqual(ack.code, "AE")
        store = Store(self.db)
        row = store.results_for_message("LAB1^HOSP", "MSG00001")[0]
        self.assertEqual(row["value_text"], "7.5")
        store.close()
        proc.send_signal(signal.SIGTERM)
        wait_exit(proc)

    # -- graceful stop ----------------------------------------------------------
    def test_graceful_sigterm_stop(self):
        proc, host, port = start_server(self.db)
        self.assertEqual(self._send_expect_crash(host, port, fx.MSG1).code, "AA")
        proc.send_signal(signal.SIGTERM)
        rc = wait_exit(proc)
        self.assertEqual(rc, 0)
        # database is consistent and queryable after the graceful stop
        store = Store(self.db)
        self.assertEqual(store.count_results(), 4)
        store.close()


if __name__ == "__main__":
    unittest.main()
