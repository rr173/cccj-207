"""Runnable synthetic audit drill (one command, fully reproducible).

Steps performed, all against runtime-generated synthetic data in a fresh
temporary (or caller-specified) directory:

1. build the two-window warehouse through the real Store commit path,
   including boundary-timestamp messages and a rejected inbox record;
2. run the product audit CLI and capture its deterministic JSON report;
3. run the independent reference checker against that report;
4. print the report path, exit code and reference verdict.

Usage::

    python -m drills.run_audit_drill                 # temp dir, auto-cleaned
    python -m drills.run_audit_drill --workdir DIR   # keeps db/report/policy
    python -m drills.run_audit_drill --no-reject     # skip the rejected record

The drill exits 0 only when the product audit runs and the independent
reference check agrees.  It never touches a real instrument or hospital
system.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

from hl7recv.audit.cli import audit_main
from hl7recv.server import MllpServer
from hl7recv.store import Store
from peer.client import send_message
from drills import audit_drill as ad

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POLICY_TEMPLATE = os.path.join(ROOT, "drills", "audit_policy.example.json")


def _start_server(store: Store) -> tuple[MllpServer, "object"]:
    import threading
    server = MllpServer("127.0.0.1", 0, store)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="drills.run_audit_drill")
    ap.add_argument("--workdir", help="directory for db/policy/report; "
                                      "a temp dir is used and removed otherwise")
    ap.add_argument("--no-reject", action="store_true",
                    help="do not send the rejected inbox record")
    args = ap.parse_args(argv)

    created = args.workdir is None
    workdir = args.workdir or tempfile.mkdtemp(prefix="audit-drill-")
    os.makedirs(workdir, exist_ok=True)
    db = os.path.join(workdir, "drill.db")
    policy_dst = os.path.join(workdir, "policy.json")
    report = os.path.join(workdir, "report.json")
    shutil.copyfile(POLICY_TEMPLATE, policy_dst)

    store = Store(db)
    catalogue = ad.populate(store)
    rejected = 0
    if not args.no_reject:
        server, thread = _start_server(store)
        try:
            ack = send_message(
                "127.0.0.1", server.port, ad.rejected_payload()
            )
            rejected = 1 if ack.code == "AE" else 0
            if not rejected:  # pragma: no cover - drill invariant
                print("drill setup failed: rejected payload was accepted",
                      file=sys.stderr)
                return 2
        finally:
            server.shutdown()
            thread.join(timeout=5)
    store.close()

    rc_audit = audit_main([
        "--db", db, "--policy", policy_dst, "--output", report,
    ])
    with open(report, encoding="utf-8") as fh:
        product = json.load(fh)

    ref = subprocess.run(
        [sys.executable, "-m", "refcheck.audit_reference",
         "--db", db, "--policy", policy_dst, "--report", report],
        capture_output=True, text=True, cwd=ROOT, timeout=60,
    )
    print("=" * 72)
    print("synthetic audit drill summary")
    print(f"  workdir              : {workdir}")
    print(f"  committed messages   : "
          f"{product['snapshot']['message_count']} (rows "
          f"{product['snapshot']['result_count']})")
    print(f"  rejected inbox rows  : {rejected} (never counted as facts)")
    print(f"  rows per control id  : "
          f"{ {k: len(v) for k, v in sorted(catalogue.items())} }")
    print(f"  audit exit code      : {rc_audit} "
          f"({product['report_status']})")
    print("  metric statuses      :")
    for m in product["metrics"]:
        hits = ",".join(h["rule"] for h in m["threshold_hits"]) or "-"
        print(f"    {m['metric_id']:<14} {m['status']:<15} hits={hits}")
    print(f"  report               : {report}")
    print("  reference check      :")
    for line in ref.stdout.strip().splitlines():
        print(f"    {line}")
    print("=" * 72)

    if created:
        shutil.rmtree(workdir, ignore_errors=True)
        print("(temporary workdir removed)")
    if ref.returncode != 0:
        print(ref.stderr, file=sys.stderr)
        return 1
    print("DRILL OK: product report and independent reference agree")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
