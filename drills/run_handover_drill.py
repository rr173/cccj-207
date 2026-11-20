"""Runnable handover reconciliation drill over synthetic data.

Generates the three-work-order acceptance fixture at run time, then drives
the full scriptable lifecycle -- register, inspect pending/overdue items and
observation dispositions, adjudicate the ambiguous shared group, extend the
cut-off after a late observation arrives, and finally lock the late result
to show a late write-off.  The independent reference checker re-derives every
row of the first two versions without importing the reconciliation engine.

Usage::

    python3 -I drills/run_handover_drill.py --workdir /tmp/handover-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from drills import handover_drill as hd  # noqa: E402
from refcheck import handover_reference as href  # noqa: E402
from shiftledger import reader, service, vocab  # noqa: E402


def _heading(text: str) -> None:
    print("\n=== " + text + " ===")


def run(workdir: str) -> int:
    os.makedirs(workdir, exist_ok=True)
    fx = hd.build_acceptance_fixture(workdir)
    ledger_db = os.path.join(workdir, "drill-ledger.db")

    _heading("register and settle v1")
    reg = service.register_and_settle(
        ledger_db, fx["receiver_db"], fx["manifest_dir"], cutoff=hd.T3)
    print(json.dumps(reg, sort_keys=True))

    _heading("v1 item statuses (hand-derived expectations)")
    for it in reader.iter_items(ledger_db, "SHIFT-1", 1):
        print(f"  {it['item_id']:12s} {it['status']:17s}"
              f" {it.get('timeliness') or ''}")

    _heading("ambiguous groups and observation dispositions")
    for g in reader.iter_groups(
            ledger_db, "SHIFT-1", 1, status=vocab.GRP_AMBIGUOUS):
        print("  ambiguous:", g["group_key"],
              g["detail"]["candidate_work_orders"])
    for o in reader.iter_observations(ledger_db, "SHIFT-1", 1):
        print(f"  {o['disposition']:24s} {o['obs_key']}")

    _heading("independent reference enumeration of v1 (must be empty)")
    problems = href.compare_version(
        ledger_db, "SHIFT-1", reg["version_id"], fx["manifest_dir"],
        fx["receiver_db"], hd.T3)
    print("  mismatches:", problems)

    _heading("adjudicate: point the shared group at WO2")
    out = service.submit_decisions(ledger_db, {
        "ledger_id": "SHIFT-1", "base_version": 1, "requested_by": "op-zhang",
        "actions": [{
            "action": vocab.DEC_ASSIGN_GROUP,
            "group_key": fx["keys"]["shared_group"],
            "work_order_id": "WO2",
            "reason": "shared instrument number confirmed for WO2"}]})
    print("  published version", out["version_id"])
    for iid in ("WO1-X", "WO2-X"):
        detail = reader.item_detail(ledger_db, "SHIFT-1", iid, 2)
        print(f"  {iid}: {detail['status']} via {detail['obs_key']}")

    _heading("independent reference enumeration of v2 (must be empty)")
    problems = href.compare_version(
        ledger_db, "SHIFT-1", 2, fx["manifest_dir"], fx["receiver_db"],
        hd.T3, active_assign={fx["keys"]["shared_group"]: "WO2"})
    print("  mismatches:", problems)

    _heading("late dedicated X arrives; extend cut-off and re-settle")
    late_obs = hd.add_late_x_for_wo1(fx["receiver_db"])
    ext = service.extend_cutoff(
        ledger_db, fx["receiver_db"], ledger_id="SHIFT-1",
        base_version=2, new_cutoff=hd.T5, requested_by="op-zhang")
    print("  published version", ext["version_id"])
    reopened = next(i for i in reader.iter_items(ledger_db, "SHIFT-1", 3)
                    if i["item_id"] == "WO1-X")
    print("  WO1-X reopened to:", reopened["status"])

    _heading("lock the late observation -> late write-off; old v2 unchanged")
    service.submit_decisions(ledger_db, {
        "ledger_id": "SHIFT-1", "base_version": 3, "requested_by": "op-zhang",
        "actions": [{"action": vocab.DEC_LOCK_OBS, "obs_key": late_obs,
                     "item_id": "WO1-X",
                     "reason": "accept the late dedicated result"}]})
    final = reader.item_detail(ledger_db, "SHIFT-1", "WO1-X", 4)
    print("  v4 WO1-X:", final["status"], final["timeliness"],
          "via", final["obs_key"])
    old = reader.item_detail(ledger_db, "SHIFT-1", "WO1-X", 2)
    print("  v2 WO1-X still via:", old["obs_key"])

    _heading("version history")
    for v in reader.list_versions(ledger_db, "SHIFT-1"):
        print(" ", v["version_id"], v["kind"], v["cutoff"],
              v["summary"]["item_counts"])
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workdir", required=True)
    args = ap.parse_args(argv)
    return run(args.workdir)


if __name__ == "__main__":
    raise SystemExit(main())
