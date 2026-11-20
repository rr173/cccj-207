"""Concurrency and graceful-shutdown tests."""

from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
import unittest

from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import frame_payload, send_message
from tests import fixtures as fx


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "conc.db")
        self.store = Store(self.db)
        self.server = MllpServer("127.0.0.1", 0, self.store)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.port

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def test_concurrent_senders_exactly_once(self):
        # many threads, some sending the same message concurrently
        payloads = [fx.MSG1, fx.MSG2, fx.MSG1, fx.MSG2, fx.MSG1,
                    fx.MSG_CUSTOM_SEP, fx.MSG_ESCAPES, fx.MSG1]
        results: list[str | None] = [None] * len(payloads)

        def worker(i: int, payload: bytes) -> None:
            ack = send_message("127.0.0.1", self.port, payload, timeout=15)
            results[i] = ack.code

        threads = [
            threading.Thread(target=worker, args=(i, p))
            for i, p in enumerate(payloads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(results, ["AA"] * len(payloads))
        # MSG1 x4 -> 1 copy; MSG2 x2 -> 1 copy; custom + escapes -> 1 each
        self.assertEqual(len(self.store.list_messages()), 4)
        self.assertEqual(self.store.count_results(), 4 + 2 + 1 + 1)
        states = [r["state"] for r in self.store.diagnostics()["inbox"]]
        self.assertEqual(states.count("duplicate"), 4)

    def test_graceful_shutdown_with_idle_connection(self):
        # an idle client holding the connection open must not block shutdown
        idle = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            ack = send_message("127.0.0.1", self.port, fx.MSG1)
            self.assertEqual(ack.code, "AA")
            t0 = time.monotonic()
            self.server.shutdown()
            self.thread.join(timeout=10)
            self.assertLess(time.monotonic() - t0, 8.0)
            self.assertFalse(self.thread.is_alive())
        finally:
            idle.close()
        self.assertEqual(self.store.count_results(), 4)

    def test_frames_interleaved_across_connections(self):
        # two connections, each sends a different message in fragments;
        # per-connection parser state keeps them apart
        conn1 = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        conn2 = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        f1 = frame_payload(fx.MSG1)
        f2 = frame_payload(fx.MSG2)
        try:
            conn1.sendall(f1[:10])
            conn2.sendall(f2[:10])
            conn1.sendall(f1[10:])
            conn2.sendall(f2[10:])
            conn1.settimeout(10)
            conn2.settimeout(10)
            from peer.client import read_ack
            self.assertEqual(read_ack(conn1).code, "AA")
            self.assertEqual(read_ack(conn2).code, "AA")
        finally:
            conn1.close()
            conn2.close()
        self.assertEqual(self.store.count_results(), 6)


if __name__ == "__main__":
    unittest.main()
