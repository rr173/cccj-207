"""Long-history scale proof for the result subscription.

Generates a 200k-message committed history at run time (override with
SYNC_SCALE_MESSAGES; nothing is committed to the repository), runs the
real upgrade + paged catch-up + near-tail tiny-page drill in a
subprocess, and asserts:

* the replica catches up to the exact published bound and holds exactly
  the subscribed facts (independently counted in SQL by the drill);
* the page scan is a bounded range scan (query-plan evidence) and a
  tail page is as cheap as a head page -- no rescan from position 0;
* peak RSS of the whole drill stays under the 256 MiB ceiling.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIMIT_BYTES = 256 * 1024 * 1024

N_MESSAGES = int(os.environ.get("SYNC_SCALE_MESSAGES", "200000"))

_DRILL_CHILD = r'''
import json, sys
sys.path.insert(0, %r)
from drills.sync_scale_drill import run_scale_drill
print(json.dumps(run_scale_drill(sys.argv[1], int(sys.argv[2]))))
''' % ROOT


class SyncScaleCase(unittest.TestCase):
    def test_long_history_catchup_bounded_memory(self):
        with tempfile.TemporaryDirectory() as workdir:
            proc = subprocess.run(
                [sys.executable, "-I", "-c", _DRILL_CHILD, workdir,
                 str(N_MESSAGES)],
                capture_output=True, text=True, timeout=900,
            )
            if proc.returncode != 0:
                raise AssertionError(f"drill failed:\n{proc.stderr[-3000:]}")
            stats = json.loads(proc.stdout.strip().splitlines()[-1])

            self.assertEqual(stats["messages"], N_MESSAGES)
            self.assertEqual(stats["bound"], N_MESSAGES + 3)
            self.assertEqual(stats["cursor"], N_MESSAGES + 3)
            # 2 of 4 senders are subscribed; the 3 tail commits add 2
            self.assertEqual(
                stats["replica_messages"], N_MESSAGES // 2 + 2
            )

            # scan-boundary evidence: bounded range scan on the durable
            # position, not a rescan from the subscription start
            self.assertIn("SEARCH sp USING INTEGER PRIMARY KEY",
                          stats["scan_plan"])
            self.assertIn("rowid>? AND rowid<?", stats["scan_plan"])
            head = stats["seconds"]["head_page_fetch"]
            tail = stats["seconds"]["tail_page_fetch"]
            self.assertLess(head, 1.0)
            self.assertLess(tail, 1.0)

            peak = stats["peak_rss_bytes"]
            self.assertLess(
                peak, LIMIT_BYTES,
                f"peak RSS {peak/1024/1024:.1f} MiB exceeded 256 MiB",
            )


if __name__ == "__main__":
    unittest.main()
