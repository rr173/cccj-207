"""CLI round trip and online coexistence with a running receiver.

The receiver keeps accepting and committing messages while pages are
fetched -- sync never pauses it.  Also covers the full command surface:
init idempotency/mismatch, position and facts output, and a whole
initial-sync-plus-catch-up conversation driven only through the CLI.
"""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from hl7recv.sync import publish
from peer.client import send_message
from tests import fixtures as fx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUB = ["LAB1^HOSP"]


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hl7recv", *args],
        capture_output=True, text=True, cwd=ROOT,
    )


def _start_server(db: str):
    cmd = [sys.executable, "-m", "hl7recv", "serve", "--db", db,
           "--host", "127.0.0.1", "--port", "0"]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=ROOT, text=True
    )
    ready, _, _ = select.select([proc.stdout], [], [], 10.0)
    if not ready:
        proc.kill()
        raise RuntimeError(f"server did not start: {proc.stderr.read()}")
    line = proc.stdout.readline().strip()
    _, host, port_s = line.split()
    return proc, host, int(port_s)


class CliRoundTripCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "recv.db")
        self.rep = os.path.join(self.tmp.name, "replica.db")
        self.pages = os.path.join(self.tmp.name, "pages")
        os.makedirs(self.pages)

    def tearDown(self):
        self.tmp.cleanup()

    def _page(self, start: int, bound: int, budget: int, name: str) -> str:
        out = os.path.join(self.pages, name)
        proc = _cli("sync", "page", "--db", self.db, "--sender", SUB[0],
                    "--start", str(start), "--bound", str(bound),
                    "--budget", str(budget), "--output", out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return out

    def test_full_cli_conversation_and_online_coexistence(self):
        # receiver online from the start; sync never pauses it
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        source_id = json.loads(proc.stdout)["source_id"]
        server, host, port = _start_server(self.db)
        try:
            # round 1: two messages arrive over MLLP while we watch
            self.assertEqual(
                send_message("127.0.0.1", port, fx.MSG1, timeout=5.0).code, "AA"
            )
            self.assertEqual(
                send_message("127.0.0.1", port, fx.MSG2, timeout=5.0).code, "AA"
            )

            wm = _cli("sync", "watermark", "--db", self.db, "--sender", SUB[0])
            self.assertEqual(wm.returncode, 0, wm.stderr)
            bound1 = json.loads(wm.stdout)["bound"]
            self.assertEqual(bound1, 2)

            init = _cli("replica", "init", "--db", self.rep,
                        "--source-id", source_id, "--sender", SUB[0])
            self.assertEqual(init.returncode, 0, init.stderr)
            # init is idempotent for the same identity ...
            again = _cli("replica", "init", "--db", self.rep,
                         "--source-id", source_id, "--sender", SUB[0])
            self.assertEqual(again.returncode, 0, again.stderr)
            self.assertIn('"already"', again.stdout)
            # ... but a different subscription cannot take over the replica
            bad = _cli("replica", "init", "--db", self.rep,
                       "--source-id", source_id, "--sender", "MIC1^HOSP")
            self.assertEqual(bad.returncode, 2)

            # initial sync: two budget-1 pages while the server is running
            p1 = self._page(0, bound1, 1, "p1.json")
            p2 = self._page(1, bound1, 1, "p2.json")
            for page in (p1, p2):
                imp = _cli("replica", "import", "--db", self.rep, "--page", page)
                self.assertEqual(imp.returncode, 0, imp.stderr)
                self.assertIn('"applied"', imp.stdout)

            pos = _cli("replica", "position", "--db", self.rep)
            self.assertEqual(json.loads(pos.stdout)["cursor"], 2)

            # round 2: more commits WHILE we keep fetching; a resend too
            self.assertEqual(
                send_message("127.0.0.1", port, fx.MSG1, timeout=5.0).code,
                "AA",  # duplicate resend: no new position
            )
            self.assertEqual(
                send_message("127.0.0.1", port, fx.MSG_DUP_MERGE,
                             timeout=5.0).code, "AA",
            )
            wm2 = _cli("sync", "watermark", "--db", self.db,
                       "--sender", SUB[0])
            bound2 = json.loads(wm2.stdout)["bound"]
            self.assertEqual(bound2, 3, "duplicate must not move the bound")

            p3 = self._page(2, bound2, 10, "p3.json")
            imp = _cli("replica", "import", "--db", self.rep, "--page", p3)
            self.assertEqual(imp.returncode, 0, imp.stderr)
            pos = _cli("replica", "position", "--db", self.rep)
            self.assertEqual(json.loads(pos.stdout)["cursor"], 3)

            # facts CLI: the landed facts are consumable as JSON
            facts = _cli("replica", "facts", "--db", self.rep)
            self.assertEqual(facts.returncode, 0, facts.stderr)
            landed = json.loads(facts.stdout)
            self.assertEqual([m["control_id"] for m in landed],
                             ["MSG00001", "MSG00002", "MSG0000G"])
            self.assertEqual(landed[0]["results"][0]["value_text"], "7.5")

            # independent reference agrees, driven purely by its own reads
            ref = subprocess.run(
                [sys.executable, "-m", "refcheck.sync_reference",
                 "--receiver-db", self.db, "--replica-db", self.rep,
                 "--sender", SUB[0]],
                capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(ref.returncode, 0, ref.stderr)
        finally:
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=10)

    def test_fetch_is_consistent_under_concurrent_commits(self):
        """Pages fetched during a commit storm stay well-formed tiles."""
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        server, host, port = _start_server(self.db)
        try:
            stop = threading.Event()
            payloads = [fx.MSG1, fx.MSG2, fx.MSG_DUP_MERGE, fx.MSG_REPEAT_PID3,
                        fx.MSG_EMPTY_VS_MISSING]

            def storm():
                i = 0
                while not stop.is_set():
                    # unique control id per send so commits keep coming
                    payload = payloads[i % len(payloads)].replace(
                        b"MSG000", b"MSG%03d" % (i % 900 + 100), 1)
                    try:
                        send_message("127.0.0.1", port, payload, timeout=5.0)
                    except OSError:
                        pass
                    i += 1

            t = threading.Thread(target=storm, daemon=True)
            t.start()
            time.sleep(0.3)  # let some commits land

            cursor = 0
            pages = 0
            while True:
                bound = publish.watermark(self.db, SUB)["bound"]
                if bound >= 25 and cursor >= 25:
                    break
                if cursor >= bound:
                    time.sleep(0.01)
                    continue
                data = publish.fetch_page(self.db, SUB, start=cursor,
                                          bound=bound, budget=3)
                from hl7recv.sync import protocol

                page = protocol.parse_page(data)
                self.assertEqual(page["start"], cursor)
                self.assertGreater(page["end"], cursor)
                self.assertLessEqual(page["end"], bound)
                cursor = page["end"]
                pages += 1
            stop.set()
            t.join(timeout=5)
            self.assertGreater(pages, 5)
            self.assertGreaterEqual(cursor, 25)
        finally:
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
