"""``python -m hl7recv audit`` command line entry point.

Usage::

    python -m hl7recv audit --db PATH --policy POLICY.json \
        [--baseline-start ISO --baseline-end ISO \
         --current-start ISO --current-end ISO] \
        [--output REPORT.json]

Exit codes (also embedded in the JSON report as ``exit_code``):

    0  no anomalies (all metrics ok)
    1  at least one warning, no failure
    2  at least one failure
    3  inconclusive: one or more metrics not_computable, no warning/failure
    4  usage error (invalid policy, unreadable database, bad parameters)

The success path writes *only* the deterministic JSON report: stdout when
``--output`` is absent (or ``-``), otherwise the given file.  Usage errors
emit a single JSON error object on stderr -- stdout never carries
non-JSON content.  The audit opens the database read-only and never
mutates the receiver's state.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sqlite3
import sys

from .policy import AuditError, Window, load_policy
from .engine import (
    EXIT_USAGE,
    render_report,
    run_audit,
)


def _emit_usage_error(message: str) -> int:
    payload = json.dumps(
        {"error": "audit_usage_error", "detail": message},
        sort_keys=True, ensure_ascii=False,
    )
    print(payload, file=sys.stderr)
    return EXIT_USAGE


def _merge_windows(policy, args: argparse.Namespace):
    cli_given = [
        args.baseline_start, args.baseline_end,
        args.current_start, args.current_end,
    ]
    if any(cli_given):
        if not all(cli_given):
            raise AuditError(
                "window overrides are all-or-nothing: provide all four of "
                "--baseline-start/--baseline-end/--current-start/--current-end"
            )
        baseline = Window(start=args.baseline_start, end=args.baseline_end)
        current = Window(start=args.current_start, end=args.current_end)
        return dataclasses.replace(policy, baseline=baseline, current=current)
    if policy.baseline is None:
        raise AuditError(
            "no windows: either declare windows in the policy or pass all "
            "four --*-start/--*-end arguments"
        )
    return policy


def audit_main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="hl7recv audit",
        description="Deterministic two-window data-quality audit over "
                    "confirmed result facts (read-only).",
    )
    ap.add_argument("--db", required=True, help="SQLite receiver database")
    ap.add_argument("--policy", required=True, help="versioned JSON policy file")
    ap.add_argument("--baseline-start", help="ISO-8601 inclusive baseline start")
    ap.add_argument("--baseline-end", help="ISO-8601 exclusive baseline end")
    ap.add_argument("--current-start", help="ISO-8601 inclusive current start")
    ap.add_argument("--current-end", help="ISO-8601 exclusive current end")
    ap.add_argument(
        "--output", "-o",
        help="report path; '-' or absent writes the report to stdout",
    )
    args = ap.parse_args(argv)

    try:
        policy = load_policy(args.policy)
        policy = _merge_windows(policy, args)
        if not os.path.exists(args.db):
            raise AuditError(f"database not found: {args.db!r}")
        report, exit_code = run_audit(args.db, policy)
    except AuditError as exc:
        return _emit_usage_error(str(exc))
    except sqlite3.Error as exc:
        return _emit_usage_error(f"database error: {exc}")

    text = render_report(report)
    if args.output and args.output != "-":
        try:
            with open(args.output, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
        except OSError as exc:
            return _emit_usage_error(f"cannot write report {args.output!r}: {exc}")
    else:
        sys.stdout.write(text)
    return exit_code
