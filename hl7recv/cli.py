"""Command line interface.

  python -m hl7recv serve --db PATH [--host H] [--port P] [--max-frame N]
                          [--fault-inject point:action:n]...
  python -m hl7recv query --db PATH messages
  python -m hl7recv query --db PATH results [--message SENDER:CONTROL_ID]
                                            [--patient AUTHORITY:ID]
                                            [--order ORDER_ID]
  python -m hl7recv query --db PATH diagnostics

``serve`` prints ``LISTENING <host> <port>`` on stdout once bound so
wrappers (drills) can synchronise.  SIGTERM/SIGINT trigger a graceful
stop.  All query output is JSON (stable key order) for tooling.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from . import queries
from .audit.cli import audit_main
from .faults import POINTS, FaultInjector
from .mllp import DEFAULT_MAX_FRAME
from .server import MllpServer
from .store import Store


def _cmd_serve(args: argparse.Namespace) -> int:
    try:
        faults = FaultInjector.from_strings(args.fault_inject)
    except ValueError as exc:
        print(f"invalid --fault-inject: {exc}", file=sys.stderr)
        return 2
    store = Store(args.db)
    server = MllpServer(
        args.host, args.port, store, max_frame=args.max_frame, faults=faults
    )
    print(f"LISTENING {server.host} {server.port}", flush=True)

    stopped = threading.Event()

    def _stop_handler(signum, frame):  # noqa: ARG001
        stopped.set()
        server.shutdown()

    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)
    try:
        server.serve_forever()
    finally:
        store.close()
    return 0


def _cmd_query(args: argparse.Namespace) -> int:
    store = Store(args.db, recover=False)  # queries never mutate recovery state
    try:
        if args.what == "messages":
            out = queries.list_messages(store)
        elif args.what == "results":
            if args.message:
                sender, sep, cid = args.message.partition(":")
                if not sep:
                    raise SystemExit("--message must look like SENDER:CONTROL_ID")
                out = queries.results_by_message(store, sender, cid)
            elif args.patient:
                auth, sep, ident = args.patient.partition(":")
                if not sep:
                    raise SystemExit("--patient must look like AUTHORITY:ID")
                out = queries.results_by_patient(store, auth, ident)
            elif args.order:
                out = queries.results_by_order(store, args.order)
            else:
                raise SystemExit(
                    "results query needs --message, --patient or --order"
                )
        elif args.what == "diagnostics":
            out = queries.diagnostics(store)
        else:  # pragma: no cover - argparse enforces choices
            raise SystemExit(f"unknown query {args.what}")
    finally:
        store.close()
    json.dump(out, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) >= 1 and argv[0] == "audit":
        # delegate everything after the subcommand word to the audit parser
        return audit_main(argv[1:])

    ap = argparse.ArgumentParser(prog="hl7recv", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("serve", help="run the MLLP receiver")
    sp.add_argument("--db", required=True, help="SQLite database path")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=2575)
    sp.add_argument("--max-frame", type=int, default=DEFAULT_MAX_FRAME)
    sp.add_argument(
        "--fault-inject",
        action="append",
        default=[],
        metavar="POINT:ACTION:N",
        help=f"deterministic fault injection; points: {', '.join(POINTS)}; "
        "actions: exit, raise",
    )
    sp.set_defaults(func=_cmd_serve)

    qp = sub.add_parser("query", help="query the fact store")
    qp.add_argument("--db", required=True)
    qp.add_argument("what", choices=["messages", "results", "diagnostics"])
    qp.add_argument("--message", help="SENDER:CONTROL_ID")
    qp.add_argument("--patient", help="ASSIGNING_AUTHORITY:IDENTIFIER")
    qp.add_argument("--order", help="placer or filler order id")
    qp.set_defaults(func=_cmd_query)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
