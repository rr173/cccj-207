"""``python -m shiftledger`` command line entry point.

Subcommands::

    register   validate a manifest directory, create a ledger, publish v1
    decide     apply a decision request (JSON file) and publish a new version
    extend     move the cut-off later, read a fresh fact batch, re-settle
    versions   list every published version
    overview   header + counts of one version
    items      page through receivable items (filter by status)
    item       one item with all candidate/potential edges and evidence
    observations  page through observation dispositions
    groups     page through group attributions with mapping detail
    decisions  the append-only manual decision log

Successful commands print one deterministic JSON document on stdout.  Usage
or validation failures print one JSON error object on stderr and exit 4
(or 5 for a stale version); nothing is half-written.  The receiver database
is only opened read-only.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import reader, service
from .errors import LedgerError, StaleVersionError

EXIT_OK = 0
EXIT_USAGE = 4
EXIT_STALE = 5


def _emit(err: Exception) -> int:
    code = getattr(err, "code", "ledger_error")
    print(json.dumps({"error": code, "detail": str(err)},
                     sort_keys=True, ensure_ascii=False), file=sys.stderr)
    return EXIT_STALE if isinstance(err, StaleVersionError) else EXIT_USAGE


def _load_request(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        req = json.load(fh)
    if not isinstance(req, dict):
        raise LedgerError("decision request must be a JSON object")
    return req


def _print(obj) -> None:
    sys.stdout.write(json.dumps(obj, sort_keys=True, ensure_ascii=False,
                                indent=2) + "\n")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="shiftledger",
                                 description="Local shift handover reconciliation ledger.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register", help="register a manifest and settle v1")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--receiver-db", required=True)
    p.add_argument("--manifest-dir", required=True)
    p.add_argument("--cutoff", help="override the manifest cut-off")
    p.add_argument("--ledger-id")

    p = sub.add_parser("decide", help="apply a decision request")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--request", required=True, help="decision request JSON file")

    p = sub.add_parser("extend", help="extend cut-off and re-settle")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--receiver-db", required=True)
    p.add_argument("--request", required=True, help="extension request JSON file")

    p = sub.add_parser("versions", help="list published versions")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--ledger-id", required=True)

    p = sub.add_parser("overview", help="version header and counts")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--ledger-id", required=True)
    p.add_argument("--version", type=int)

    for name in ("items", "observations", "groups"):
        p = sub.add_parser(name, help=f"page through {name}")
        p.add_argument("--ledger-db", required=True)
        p.add_argument("--ledger-id", required=True)
        p.add_argument("--version", type=int)
        p.add_argument("--status")
        p.add_argument("--page", type=int, default=500)

    p = sub.add_parser("item", help="single item with edges and evidence")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--ledger-id", required=True)
    p.add_argument("--item-id", required=True)
    p.add_argument("--version", type=int)

    p = sub.add_parser("decisions", help="manual decision log")
    p.add_argument("--ledger-db", required=True)
    p.add_argument("--ledger-id", required=True)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "register":
            out = service.register_and_settle(
                args.ledger_db, args.receiver_db, args.manifest_dir,
                cutoff=args.cutoff, ledger_id=args.ledger_id)
        elif args.cmd == "decide":
            req = _load_request(args.request)
            out = service.submit_decisions(args.ledger_db, req)
        elif args.cmd == "extend":
            req = _load_request(args.request)
            out = service.extend_cutoff(
                args.ledger_db, args.receiver_db,
                ledger_id=req["ledger_id"], base_version=req["base_version"],
                new_cutoff=req["new_cutoff"], requested_by=req["requested_by"],
                withdrawals=req.get("withdrawals"))
        elif args.cmd == "versions":
            out = reader.list_versions(args.ledger_db, args.ledger_id)
        elif args.cmd == "overview":
            out = reader.version_overview(
                args.ledger_db, args.ledger_id, args.version)
        elif args.cmd == "items":
            out = list(reader.iter_items(
                args.ledger_db, args.ledger_id, args.version,
                status=args.status, page=args.page))
        elif args.cmd == "item":
            out = reader.item_detail(
                args.ledger_db, args.ledger_id, args.item_id, args.version)
        elif args.cmd == "observations":
            out = list(reader.iter_observations(
                args.ledger_db, args.ledger_id, args.version,
                disposition=args.status, page=args.page))
        elif args.cmd == "groups":
            out = list(reader.iter_groups(
                args.ledger_db, args.ledger_id, args.version,
                status=args.status, page=args.page))
        elif args.cmd == "decisions":
            out = reader.list_decisions(args.ledger_db, args.ledger_id)
        else:  # pragma: no cover
            raise LedgerError(f"unknown command {args.cmd!r}")
    except (LedgerError, json.JSONDecodeError, OSError) as exc:
        return _emit(exc if isinstance(exc, LedgerError)
                     else LedgerError(str(exc)))
    _print(out)
    return EXIT_OK
