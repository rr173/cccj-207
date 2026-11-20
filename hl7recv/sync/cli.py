"""CLI for the result subscription (docs/SYNC.md).

  python -m hl7recv sync upgrade --db DB [--source-id HEX]
  python -m hl7recv sync watermark --db DB --sender S [--sender S ...]
  python -m hl7recv sync page --db DB --sender S ... --start N --bound M \
      --budget K --output FILE
  python -m hl7recv replica init --db R --source-id HEX --sender S ...
  python -m hl7recv replica import --db R --page FILE
  python -m hl7recv replica position --db R
  python -m hl7recv replica facts --db R

Exit codes: 0 success (import reports "applied" or "duplicate");
1 corrupt page; 2 source/subscription mismatch; 3 cursor gap/straddle;
4 stale page conflict; 5 usage error; 6 sync not enabled on the source;
7 invalid page window.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import protocol, publish, replica

EXIT_CORRUPT = 1
EXIT_MISMATCH = 2
EXIT_GAP = 3
EXIT_STALE = 4
EXIT_USAGE = 5
EXIT_NOT_ENABLED = 6
EXIT_WINDOW = 7

# drill-only crash simulation, mirrors hl7recv.faults.EXIT_CODE
_DRILL_EXIT = 70


def _out(obj: dict) -> None:
    json.dump(obj, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def _cmd_sync_upgrade(args: argparse.Namespace) -> int:
    hook = None
    if args.fault_after_rows is not None:
        def hook(assigned: int) -> None:  # noqa: F811
            if assigned >= args.fault_after_rows:
                os._exit(_DRILL_EXIT)  # simulated mid-upgrade crash (drill)
    result = publish.upgrade(
        args.db, source_id=args.source_id, fault_hook=hook
    )
    _out(result)
    return 0


def _cmd_sync_watermark(args: argparse.Namespace) -> int:
    try:
        _out(publish.watermark(args.db, args.sender))
        return 0
    except publish.SyncNotEnabled as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_NOT_ENABLED
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_USAGE


def _cmd_sync_page(args: argparse.Namespace) -> int:
    try:
        data = publish.fetch_page(
            args.db, args.sender,
            start=args.start, bound=args.bound, budget=args.budget,
        )
    except publish.SyncNotEnabled as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_NOT_ENABLED
    except publish.BoundError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_WINDOW
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_USAGE
    with open(args.output, "wb") as fh:
        fh.write(data)
    page = protocol.parse_page(data)
    _out({"status": "written", "output": args.output, "start": page["start"],
          "end": page["end"], "terminal": page["terminal"],
          "messages": len(page["messages"]), "bytes": len(data)})
    return 0


def _cmd_replica_init(args: argparse.Namespace) -> int:
    try:
        _out(replica.init_replica(args.db, source_id=args.source_id,
                                  senders=args.sender))
        return 0
    except (replica.ReplicaError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_MISMATCH


def _cmd_replica_import(args: argparse.Namespace) -> int:
    hook = None
    if args.fault_inject is not None:
        def hook(point: str) -> None:  # noqa: F811
            if point == args.fault_inject:
                os._exit(_DRILL_EXIT)  # simulated mid-import crash (drill)
    try:
        with open(args.page, "rb") as fh:
            data = fh.read()
    except OSError as exc:
        print(f"cannot read page file: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        _out(replica.import_page(args.db, data, fault_hook=hook))
        return 0
    except protocol.PageError as exc:
        print(f"corrupt page: {exc}", file=sys.stderr)
        return EXIT_CORRUPT
    except replica.IdentityMismatch as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_MISMATCH
    except replica.StalePageConflict as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_STALE
    except replica.CursorMismatch as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_GAP
    except replica.ReplicaError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_USAGE


def _cmd_replica_position(args: argparse.Namespace) -> int:
    try:
        _out(replica.get_position(args.db))
        return 0
    except replica.ReplicaError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_USAGE


def _cmd_replica_facts(args: argparse.Namespace) -> int:
    try:
        facts = list(replica.iter_facts(args.db))
    except replica.ReplicaError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_USAGE
    json.dump(facts, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def _add_senders(p: argparse.ArgumentParser) -> None:
    p.add_argument("--sender", action="append", required=True,
                   help="subscribed sender (repeatable); the fixed set is"
                        " normalised (deduped, sorted) into the"
                        " subscription identity")


def sync_main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="hl7recv sync", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("upgrade", help="offline upgrade: enable publishing")
    up.add_argument("--db", required=True)
    up.add_argument("--source-id", default=None,
                    help="pin the source identity (default: random)")
    up.add_argument("--fault-after-rows", type=int, default=None,
                    help=argparse.SUPPRESS)  # drill-only crash simulation
    up.set_defaults(func=_cmd_sync_upgrade)

    wm = sub.add_parser("watermark", help="report the publish upper bound")
    wm.add_argument("--db", required=True)
    _add_senders(wm)
    wm.set_defaults(func=_cmd_sync_watermark)

    pg = sub.add_parser("page", help="fetch one deterministic page file")
    pg.add_argument("--db", required=True)
    _add_senders(pg)
    pg.add_argument("--start", type=int, required=True)
    pg.add_argument("--bound", type=int, required=True)
    pg.add_argument("--budget", type=int, required=True)
    pg.add_argument("--output", required=True)
    pg.set_defaults(func=_cmd_sync_page)

    args = ap.parse_args(argv)
    return args.func(args)


def replica_main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="hl7recv replica", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    ini = sub.add_parser("init", help="initialise a replica database")
    ini.add_argument("--db", required=True)
    ini.add_argument("--source-id", required=True)
    _add_senders(ini)
    ini.set_defaults(func=_cmd_replica_init)

    imp = sub.add_parser("import", help="validate and apply one page file")
    imp.add_argument("--db", required=True)
    imp.add_argument("--page", required=True)
    imp.add_argument("--fault-inject", default=None,
                     choices=["import_before_commit", "import_after_commit"],
                     help=argparse.SUPPRESS)  # drill-only crash simulation
    imp.set_defaults(func=_cmd_replica_import)

    pos = sub.add_parser("position", help="print the current cursor")
    pos.add_argument("--db", required=True)
    pos.set_defaults(func=_cmd_replica_position)

    fac = sub.add_parser("facts", help="dump landed facts (JSON)")
    fac.add_argument("--db", required=True)
    fac.set_defaults(func=_cmd_replica_facts)

    args = ap.parse_args(argv)
    return args.func(args)
