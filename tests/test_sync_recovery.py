"""Process-exit drills for the subscription pipeline.

Receive side (real server subprocesses, deterministic fault points):
* crash after commit, before ACK  -> the committed message still gets a
  position and is catch-up-able (a missing ACK changes nothing);
* crash before commit             -> neither message nor position exists
  and the next commit reuses the position (no skippable hole).

Import side (real importer subprocesses, drill-only fault flag):
* exit before the import commit   -> readers see the old whole-page state
  (cursor and facts unchanged);
* exit after the import commit    -> the whole page is visible;
* replaying a fully applied page is a no-op; corrupted pages, stale
  lookalikes, gaps and identity mismatches are rejected without moving
  the cursor; two racing importers cannot both advance the same cursor.

Every drill opens the database afterwards and checks rows and cursor --
never just the exit code.
"""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import unittest

from hl7recv.sync import publish, replica
from peer.client import send_message
from tests import fixtures as fx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUB = ["LAB1^HOSP"]


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hl7recv", *args],
        capture_output=True, text=True, cwd=ROOT,
    )


def _start_server(db: str, faults: list[str] | None = None):
    cmd = [sys.executable, "-m", "hl7recv", "serve", "--db", db,
           "--host", "127.0.0.1", "--port", "0"]
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
    _, host, port_s = line.split()
    return proc, host, int(port_s)


def _wait_exit(proc: subprocess.Popen, timeout: float = 10.0) -> int:
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


def _stop(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    _wait_exit(proc)


class ReceivePublishCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "recv.db")
        self.rep = os.path.join(self.tmp.name, "replica.db")
        # upgraded, still empty receiver database
        proc = _cli("sync", "upgrade", "--db", self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.source_id = json.loads(proc.stdout)["source_id"]

    def tearDown(self):
        self.tmp.cleanup()

    def _wm_bound(self) -> int:
        return publish.watermark(self.db, SUB)["bound"]

    def test_committed_but_ack_lost_is_still_caught_up(self):
        proc, host, port = _start_server(
            self.db, ["after_commit_before_ack:exit:1"]
        )
        ack = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertIsNone(ack.code, "client cannot have seen an ACK")
        self.assertEqual(_wait_exit(proc), 70)

        # restart: the commit (and its position) survived the lost ACK
        proc, host, port = _start_server(self.db)
        self.assertEqual(self._wm_bound(), 1)
        page = publish.fetch_page(self.db, SUB, start=0, bound=1, budget=10)
        replica.init_replica(self.rep, source_id=self.source_id, senders=SUB)
        out = replica.import_page(self.rep, page)
        self.assertEqual((out["status"], out["cursor"]), ("applied", 1))
        facts = list(replica.iter_facts(self.rep))
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["control_id"], "MSG00001")
        self.assertEqual(len(facts[0]["results"]), 4)

        # a normal commit afterwards gets the very next position (dense)
        ack = send_message("127.0.0.1", port, fx.MSG2, timeout=5.0)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self._wm_bound(), 2)
        _stop(proc)

    def test_crash_before_commit_leaves_no_hole(self):
        proc, host, port = _start_server(self.db, ["before_commit:exit:1"])
        ack = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertIsNone(ack.code)
        self.assertEqual(_wait_exit(proc), 70)

        proc, host, port = _start_server(self.db)
        self.assertEqual(self._wm_bound(), 0, "no position may be published")
        ack = send_message("127.0.0.1", port, fx.MSG1, timeout=5.0)
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self._wm_bound(), 1,
                         "the retried commit takes position 1, not 2")
        _stop(proc)


class ImportDrillCase(unittest.TestCase):
    """Import-side breakpoints and rejection drills (subprocess CLI)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "recv.db")
        self.rep = os.path.join(self.tmp.name, "replica.db")
        from hl7recv.builder import build_message
        from hl7recv.store import Store

        store = Store(self.db)
        store.commit_message(1, build_message(fx.MSG1), fx.MSG1)
        store.commit_message(2, build_message(fx.MSG2), fx.MSG2)
        store.close()
        self.meta = publish.upgrade(self.db)
        self.pages = []
        for i, (start, bound) in enumerate(((0, 1), (1, 2))):
            path = os.path.join(self.tmp.name, f"p{i}.json")
            with open(path, "wb") as fh:
                fh.write(publish.fetch_page(
                    self.db, SUB, start=start, bound=2, budget=1))
            self.pages.append(path)
        proc = _cli("replica", "init", "--db", self.rep,
                    "--source-id", self.meta["source_id"], "--sender", SUB[0])
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def tearDown(self):
        self.tmp.cleanup()

    def _cursor(self) -> int:
        return replica.get_position(self.rep)["cursor"]

    def _facts(self):
        return list(replica.iter_facts(self.rep))

    def _import(self, page_path: str, *extra: str) -> subprocess.CompletedProcess:
        return _cli("replica", "import", "--db", self.rep,
                    "--page", page_path, *extra)

    # -- breakpoints ---------------------------------------------------------
    def test_exit_before_import_commit_leaves_old_state(self):
        proc = self._import(self.pages[0],
                            "--fault-inject", "import_before_commit")
        self.assertEqual(proc.returncode, 70)
        self.assertEqual(self._cursor(), 0)
        self.assertEqual(self._facts(), [])
        # the page is still applicable afterwards
        proc = self._import(self.pages[0])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["status"], "applied")
        self.assertEqual(self._cursor(), 1)
        self.assertEqual(len(self._facts()), 1)

    def test_exit_after_import_commit_is_fully_visible(self):
        proc = self._import(self.pages[0],
                            "--fault-inject", "import_after_commit")
        self.assertEqual(proc.returncode, 70)
        self.assertEqual(self._cursor(), 1)
        facts = self._facts()
        self.assertEqual(len(facts), 1)
        self.assertEqual(len(facts[0]["results"]), 4)

    # -- replay / stale --------------------------------------------------------
    def test_replay_of_applied_page_is_noop(self):
        self.assertEqual(self._import(self.pages[0]).returncode, 0)
        before = self._facts()
        proc = self._import(self.pages[0])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["status"], "duplicate")
        self.assertEqual(self._cursor(), 1)
        self.assertEqual(self._facts(), before)

    def test_stale_lookalike_is_not_a_replay(self):
        # same (start, end] progress but fetched with a different budget ->
        # different header bytes -> must not be accepted as a replay
        self.assertEqual(self._import(self.pages[0]).returncode, 0)
        lookalike = os.path.join(self.tmp.name, "lookalike.json")
        with open(lookalike, "wb") as fh:
            fh.write(publish.fetch_page(self.db, SUB, start=0, bound=1,
                                        budget=99))
        proc = self._import(lookalike)
        self.assertEqual(proc.returncode, 4, proc.stderr)
        self.assertEqual(self._cursor(), 1)

    # -- corruption ------------------------------------------------------------
    def test_truncated_tail_rejected(self):
        with open(self.pages[0], "rb") as fh:
            data = fh.read()
        cut = os.path.join(self.tmp.name, "cut.json")
        with open(cut, "wb") as fh:
            fh.write(data[:-25])
        proc = self._import(cut)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(self._cursor(), 0)
        self.assertEqual(self._facts(), [])

    def test_payload_tamper_rejected(self):
        with open(self.pages[0], "rb") as fh:
            data = fh.read()
        self.assertIn(b"7.5", data)
        tampered = os.path.join(self.tmp.name, "tampered.json")
        with open(tampered, "wb") as fh:
            fh.write(data.replace(b"7.5", b"9.9", 1))
        proc = self._import(tampered)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(self._cursor(), 0)

    def test_header_tamper_rejected(self):
        with open(self.pages[0], "rb") as fh:
            data = fh.read()
        self.assertIn(b'"budget":1', data)
        tampered = os.path.join(self.tmp.name, "hdr.json")
        with open(tampered, "wb") as fh:
            fh.write(data.replace(b'"budget":1', b'"budget":2', 1))
        proc = self._import(tampered)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(self._cursor(), 0)

    # -- window / identity -------------------------------------------------------
    def test_gap_rejected(self):
        proc = self._import(self.pages[1])  # starts at 1, cursor is 0
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(self._cursor(), 0)
        self.assertEqual(self._facts(), [])

    def test_foreign_source_rejected(self):
        other = os.path.join(self.tmp.name, "other.db")
        from hl7recv.builder import build_message
        from hl7recv.store import Store

        store = Store(other)
        store.commit_message(1, build_message(fx.MSG1), fx.MSG1)
        store.close()
        publish.upgrade(other)
        foreign = os.path.join(self.tmp.name, "foreign.json")
        with open(foreign, "wb") as fh:
            fh.write(publish.fetch_page(other, SUB, start=0, bound=1, budget=1))
        proc = self._import(foreign)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(self._cursor(), 0)

    def test_foreign_subscription_rejected(self):
        page = os.path.join(self.tmp.name, "othersub.json")
        with open(page, "wb") as fh:
            fh.write(publish.fetch_page(
                self.db, ["LAB1^HOSP", "MIC1^HOSP"], start=0, bound=1,
                budget=1))
        proc = self._import(page)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(self._cursor(), 0)

    # -- racing importers ---------------------------------------------------------
    def test_competing_pages_only_one_advances(self):
        big = os.path.join(self.tmp.name, "big.json")
        with open(big, "wb") as fh:
            fh.write(publish.fetch_page(self.db, SUB, start=0, bound=2,
                                        budget=2))
        # page A covers (0,1], page B covers (0,2]; both start at cursor 0
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "hl7recv", "replica", "import",
                 "--db", self.rep, "--page", path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, cwd=ROOT,
            )
            for path in (self.pages[0], big)
        ]
        results = [p.communicate(timeout=60) for p in procs]
        rcs = [p.returncode for p in procs]
        self.assertEqual(sorted(rcs)[0], 0, f"one must win: {results}")
        self.assertEqual(rcs.count(0), 1, f"exactly one may advance: {rcs}")
        winner = rcs.index(0)
        if winner == 0:  # small page won -> cursor 1, one message
            self.assertEqual(self._cursor(), 1)
            self.assertEqual(len(self._facts()), 1)
        else:            # big page won -> cursor 2, two messages
            self.assertEqual(self._cursor(), 2)
            self.assertEqual(len(self._facts()), 2)


if __name__ == "__main__":
    unittest.main()
