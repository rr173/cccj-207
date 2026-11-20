"""Stress and memory checks.

Stress input is generated at runtime (never committed to git).  The checks
assert *invariants* rather than absolute numbers:

* the receiver survives large multi-OBX messages delivered in fragments;
* repeated large messages do not accumulate memory (no per-message leak);
* result iteration is batched: walking 20k rows keeps memory bounded by
  the batch, not by the table.
"""

from __future__ import annotations

import gc
import os
import tempfile
import threading
import tracemalloc
import unittest

from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import send_message
from tests import fixtures as fx

N_OBX = 20_000
TIMEOUT = 60.0


class StressCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "stress.db")
        self.store = Store(self.db)
        self.server = MllpServer(
            "127.0.0.1", 0, self.store, max_frame=64 << 20
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.store.close()
        self.tmp.cleanup()

    def test_large_message_end_to_end(self):
        msg = fx.make_big_message(N_OBX)
        ack = send_message(
            "127.0.0.1", self.server.port, msg, chunk=65536, timeout=TIMEOUT
        )
        self.assertEqual(ack.code, "AA")
        self.assertEqual(self.store.count_results(), N_OBX)

    def test_repeated_large_messages_no_memory_growth(self):
        msgs = [
            fx.make_big_message(3000, control_id=f"MSGBIG{i}") for i in range(3)
        ]
        tracemalloc.start()
        baselines = []
        for msg in msgs:
            ack = send_message(
                "127.0.0.1", self.server.port, msg, chunk=65536, timeout=TIMEOUT
            )
            self.assertEqual(ack.code, "AA")
            gc.collect()
            current, _peak = tracemalloc.get_traced_memory()
            baselines.append(current)
        tracemalloc.stop()
        self.assertEqual(self.store.count_results(), 9000)
        # no per-message accumulation: the last baseline must be close to
        # the first (allowing allocator noise)
        self.assertLess(
            baselines[-1],
            baselines[0] * 2 + 5_000_000,
            f"memory grew across messages: {baselines}",
        )

    def test_batched_iteration_bounded_memory(self):
        msg = fx.make_big_message(N_OBX)
        ack = send_message(
            "127.0.0.1", self.server.port, msg, chunk=65536, timeout=TIMEOUT
        )
        self.assertEqual(ack.code, "AA")

        gc.collect()
        tracemalloc.start()
        before, _ = tracemalloc.get_traced_memory()
        count = 0
        peak_during = before
        for batch in self.store.iter_results(batch=500):
            count += len(batch)
            current, _ = tracemalloc.get_traced_memory()
            peak_during = max(peak_during, current)
            batch.clear()  # do not accumulate
        tracemalloc.stop()
        self.assertEqual(count, N_OBX)
        # walking 20k rows in batches of 500 must stay far below the size
        # of the full result set materialised at once
        self.assertLess(
            peak_during - before,
            30_000_000,
            "batched iteration accumulated memory",
        )


if __name__ == "__main__":
    unittest.main()
