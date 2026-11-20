"""The independent reference checker validates the fact store.

Positive: a database populated through the real server matches the
reference derivation for every fixture message.  Negative: tampering
with a stored value makes the reference check fail.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import send_message
from tests import fixtures as fx
from tests.test_recovery import ROOT

CHECKED = (
    fx.MSG1,
    fx.MSG2,
    fx.MSG_CUSTOM_SEP,
    fx.MSG_ESCAPES,
    fx.MSG_REPEAT_PID3,
    fx.MSG_EMPTY_VS_MISSING,
)


class TestReferenceChecker(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ref.db")
        self.store = Store(self.db)
        self.server = MllpServer("127.0.0.1", 0, self.store)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.files = []
        for i, payload in enumerate(CHECKED):
            ack = send_message("127.0.0.1", self.server.port, payload)
            assert ack.code == "AA", f"fixture {i} was not accepted: {ack.err}"
            path = os.path.join(self.tmp.name, f"msg{i}.hl7")
            with open(path, "wb") as fh:
                fh.write(payload)
            self.files.append(path)

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def run_refcheck(self) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "refcheck.reference", "--db", self.db, *self.files],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )

    def test_reference_check_passes(self):
        out = self.run_refcheck()
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("REFERENCE CHECK PASSED", out.stdout)
        self.assertEqual(out.stdout.count("OK  "), len(CHECKED))

    def test_reference_check_detects_tampering(self):
        self.store.close()  # release before external modification
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE results SET value_text='TAMPERED' WHERE rowid=1")
        conn.commit()
        conn.close()
        out = self.run_refcheck()
        self.assertEqual(out.returncode, 1)
        self.assertIn("TAMPERED", out.stdout)
        # reopen for tearDown symmetry
        self.store = Store(self.db)

    def test_reference_check_detects_missing_message(self):
        extra = os.path.join(self.tmp.name, "extra.hl7")
        with open(extra, "wb") as fh:
            fh.write(fx.MSG_DUP_MERGE)  # never sent to the server
        out = subprocess.run(
            [sys.executable, "-m", "refcheck.reference", "--db", self.db, extra],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(out.returncode, 1)
        self.assertIn("not found", out.stdout)


if __name__ == "__main__":
    unittest.main()
