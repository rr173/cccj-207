"""Bounded-memory proof at the stated scale.

The scenario is generated at run time (200k receivable items, 500k
observations; override with LEDGER_SCALE_ITEMS / LEDGER_SCALE_OBS) and the
two heavy phases run in **separate subprocesses** so each one's peak RSS is
measured independently via ``resource.ru_maxrss`` (KiB on Linux):

1. fixture generation (writes the receiver DB + manifest);
2. the ledger register+settle plus paged consumption of the whole version.

Both peaks must stay under the 256 MiB acceptance ceiling.  The ledger
streams every scan and keeps candidate/edge sets in on-disk SQLite temp
tables, so neither process materialises the full input, candidate graph or
version.  A second test checks the conservation invariants and that paged
consumption never holds the whole version.
"""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIMIT_BYTES = 256 * 1024 * 1024

N_ITEMS = int(os.environ.get("LEDGER_SCALE_ITEMS", "200000"))
N_OBS = int(os.environ.get("LEDGER_SCALE_OBS", "500000"))

_BUILD_CHILD = r'''
import json, resource, sys
sys.path.insert(0, %r)
from drills import handover_drill as hd
hd.build_scale_fixture(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
print(json.dumps({"peak_rss_bytes":
    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}))
''' % ROOT

_SETTLE_CHILD = r'''
import json, resource, sys
sys.path.insert(0, %r)
from shiftledger import reader, service
wd = sys.argv[1]
reg = service.register_and_settle(
    wd + "/ledger.db", wd + "/receiver.db", wd + "/manifest")
seen_items = seen_obs = seen_groups = 0
for _ in reader.iter_items(wd + "/ledger.db", "SHIFT-SCALE", 1, page=200):
    seen_items += 1
for _ in reader.iter_observations(wd + "/ledger.db", "SHIFT-SCALE", 1, page=200):
    seen_obs += 1
for _ in reader.iter_groups(wd + "/ledger.db", "SHIFT-SCALE", 1, page=200):
    seen_groups += 1
print(json.dumps({
    "version": reg["version_id"], "seen_items": seen_items,
    "seen_observations": seen_obs, "seen_groups": seen_groups,
    "peak_rss_bytes":
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}))
''' % ROOT


def _run(child: str, *args: str) -> dict:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", child, *args],
        capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        raise AssertionError(f"child failed:\n{proc.stderr[-3000:]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


class ScaleMemoryCase(unittest.TestCase):
    def test_both_phases_peak_rss_under_256mib(self):
        with tempfile.TemporaryDirectory() as workdir:
            build = _run(_BUILD_CHILD, workdir, str(N_ITEMS), str(N_OBS))
            settle = _run(_SETTLE_CHILD, workdir)

            self.assertEqual(settle["seen_items"], N_ITEMS)
            self.assertEqual(settle["seen_observations"], N_OBS)
            self.assertEqual(settle["seen_groups"], N_OBS)
            self.assertGreater(settle["version"], 0)

            for label, result in (("build", build), ("settle", settle)):
                peak = result["peak_rss_bytes"]
                self.assertLess(
                    peak, LIMIT_BYTES,
                    f"{label} peak RSS {peak/1024/1024:.1f} MiB"
                    " exceeded 256 MiB")

    def test_conservation_and_disposition_coverage(self):
        n_items, n_obs = min(N_ITEMS, 20000), min(N_OBS, 50000)
        with tempfile.TemporaryDirectory() as workdir:
            _run(_BUILD_CHILD, workdir, str(n_items), str(n_obs))
            _run(_SETTLE_CHILD, workdir)
            sys.path.insert(0, ROOT)
            from shiftledger import reader  # noqa: E402
            overview = reader.version_overview(
                workdir + "/ledger.db", "SHIFT-SCALE", 1)
            ic = overview["summary"]["item_counts"]
            oc = overview["summary"]["observation_counts"]
            # every item and observation is accounted for exactly once
            self.assertEqual(sum(ic.values()), n_items)
            self.assertEqual(sum(oc.values()), n_obs)
            # adopted observations == settled items (no double write-off)
            self.assertEqual(
                ic["auto_settled"] + ic["lock_settled"], oc["adopted"])
            self.assertEqual(
                overview["fact_batch"]["n_observations"], n_obs)


if __name__ == "__main__":
    unittest.main()
