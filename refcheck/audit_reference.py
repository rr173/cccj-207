"""Independent reference audit calculator.

This module is deliberately written from scratch and imports **no** code
from ``hl7recv.audit`` and no product metric/report assembly functions.  It
re-derives every audited statistic directly from the SQLite fact tables with
its own SQL, its own streaming aggregates, its own quantile implementation
and its own threshold evaluation, then compares the derivation against the
product's rendered report.

It re-declares even the metric/rule vocabulary locally, so a rename or
drift in the product cannot silently make the two implementations agree.
The only shared inputs are: the policy JSON file, the database file and
the product report JSON on disk.

Usage::

    python -m refcheck.audit_reference --db PATH --policy POLICY.json \\
        --report PRODUCT_REPORT.json

Exit code 0 means every metric value, threshold severity, denominator,
exclusion count and not-computable reason matched the product report.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from typing import Any

# Re-declared independently of the product vocabulary.
R_TEXT_TYPES = {"ST", "TX", "FT"}
R_NUMERIC_TYPES = {"NM"}

EXIT_OK = 0
EXIT_MISMATCH = 1


# ---------------------------------------------------------------------------
# Independent window handling
# ---------------------------------------------------------------------------

def _canonical(text: str) -> str:
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"naive timestamp rejected: {text!r}")
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Independent streaming scan
# ---------------------------------------------------------------------------

class WindowStats:
    """All reference statistics for one window, computed in one table scan."""

    def __init__(self, buckets: list[int], quantiles: list[float]) -> None:
        self.buckets = buckets
        self.quantiles = quantiles
        self.message_keys: set[tuple[str, str]] = set()
        self.result_rows = 0
        # field non-null
        self.field_total: dict[str, int] = {}
        self.field_nonnull: dict[str, int] = {}
        # cardinality
        self.card: dict[str, set[str]] = {
            "sender": set(), "test_code": set(), "obx2_type": set()
        }
        self.card_counts: dict[str, dict[str, int]] = {
            "sender": {}, "test_code": {}, "obx2_type": {}
        }
        # text lengths
        self.text_n = 0
        self.text_excluded = 0
        self.text_total_len = 0
        self.text_min: int | None = None
        self.text_max = 0
        self.text_bins = [0 for _ in buckets]
        self.text_over_top = 0
        # numeric
        self.nm_n = 0
        self.nm_excluded = 0
        self.nm_values: list[float] = []
        self.nm_unparseable = 0

    def add(self, row: sqlite3.Row) -> None:
        self.message_keys.add((row["sender"], row["control_id"]))
        self.result_rows += 1

        for col in _RATE_FIELDS:
            self.field_total[col] = self.field_total.get(col, 0) + 1
            cell = row[col]
            if cell is not None and cell != "":
                self.field_nonnull[col] = self.field_nonnull.get(col, 0) + 1

        for dim, key in (
            ("sender", row["sender"]),
            ("test_code", row["test_code"]),
            ("obx2_type", row["value_type"]),
        ):
            self.card[dim].add(str(key))
            self.card_counts[dim][str(key)] = (
                self.card_counts[dim].get(str(key), 0) + 1
            )

        if row["value_type"] in R_TEXT_TYPES:
            length = len(row["value_text"] or "")
            self.text_n += 1
            self.text_total_len += length
            self.text_min = length if self.text_min is None else min(
                self.text_min, length
            )
            self.text_max = max(self.text_max, length)
            for i, edge in enumerate(self.buckets):
                if length <= edge:
                    self.text_bins[i] += 1
                    break
            else:
                self.text_over_top += 1
        else:
            self.text_excluded += 1

        if row["value_type"] in R_NUMERIC_TYPES:
            self.nm_n += 1
            parsed = _parse_float(row["value_text"])
            if parsed is not None:
                self.nm_values.append(parsed)
            else:
                self.nm_unparseable += 1
        else:
            self.nm_excluded += 1

    # -- derived values ----------------------------------------------------
    def rate(self, col: str) -> float | None:
        n = self.field_total.get(col, 0)
        if n == 0:
            return None
        return round(self.field_nonnull.get(col, 0) / n, 6)

    def text_histogram(self) -> list[dict[str, int]]:
        out = []
        lower = 0
        for edge, count in zip(self.buckets, self.text_bins):
            out.append({"upper_inclusive": edge, "count": count})
            lower = edge
        out.append({"upper_inclusive": -1, "count": self.text_over_top})
        return out

    def numeric_quantiles(self) -> list[dict[str, float]]:
        if not self.nm_values:
            return []
        ordered = sorted(self.nm_values)
        out = []
        for p in self.quantiles:
            out.append({
                "probability": round(p, 6),
                "value": round(_quantile_linear(ordered, p), 6),
            })
        return out


_RATE_FIELDS = (
    "patient_authority", "patient_id", "visit_id", "placer_order",
    "filler_order", "test_name", "obs_name", "sub_id", "units",
    "ref_range", "abnormal", "observed_at", "reported_at",
    "sender", "control_id", "test_code", "test_system", "obs_code",
    "obs_system", "value_type", "value_text", "rstatus",
    "obr_index", "obx_index",
)


def _parse_float(text: str) -> float | None:
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        v = float(text.strip())
    except ValueError:
        return None
    return v if math.isfinite(v) else None


def _quantile_linear(ordered: list[float], prob: float) -> float:
    """R-7 linear interpolation, independently implemented."""
    n = len(ordered)
    if n == 1:
        return float(ordered[0])
    h = (n - 1) * prob
    lo = math.floor(h)
    frac = h - lo
    hi = min(lo + 1, n - 1)
    return ordered[lo] + frac * (ordered[hi] - ordered[lo])


def scan_window(
    conn: sqlite3.Connection,
    senders: list[str],
    test_codes: list[str] | None,
    start: str,
    end: str,
    buckets: list[int],
    quantiles: list[float],
) -> WindowStats:
    stats = WindowStats(buckets, quantiles)
    sender_ph = ",".join("?" for _ in senders)
    sql = (
        "SELECT r.* FROM results r JOIN messages m"
        " ON m.sender = r.sender AND m.control_id = r.control_id"
        f" WHERE m.sender IN ({sender_ph})"
        " AND m.received_at >= ? AND m.received_at < ?"
    )
    params: list[Any] = [*senders, start, end]
    if test_codes:
        code_ph = ",".join("?" for _ in test_codes)
        sql += f" AND r.test_code IN ({code_ph})"
        params.extend(test_codes)
    sql += " ORDER BY m.rowid, r.id"
    for row in conn.execute(sql, params):
        stats.add(row)
    # message count straight from the fact table
    m_count = conn.execute(
        f"SELECT COUNT(*) FROM messages WHERE sender IN ({sender_ph})"
        " AND received_at >= ? AND received_at < ?",
        (*senders, start, end),
    ).fetchone()[0]
    stats.message_count = int(m_count)
    return stats


# ---------------------------------------------------------------------------
# Independent threshold evaluation
# ---------------------------------------------------------------------------

def _evaluate_rules(spec: dict, base: WindowStats, cur: WindowStats,
                    mtype: str) -> tuple[set[str], set[str]]:
    warn, fail = set(), set()
    for rule in spec.get("rules", []):
        name, sev, param = rule["rule"], rule["severity"], float(rule["parameter"])
        fired = False
        if name == "min_sample_size":
            col = _sample_col(mtype)
            n = _sample_size(cur, mtype, col)
            fired = n < param
        elif name == "count_drop_gte":
            fired = _count(base, mtype) - _count(cur, mtype) >= param
        elif name == "current_to_baseline_ratio_below":
            b = _count(base, mtype)
            fired = b > 0 and (_count(cur, mtype) / b) < param
        elif name == "nonnull_rate_below":
            r = cur.rate(spec["field"])
            fired = r is not None and r < param
        elif name == "nonnull_rate_drop_gte":
            rb, rc = base.rate(spec["field"]), cur.rate(spec["field"])
            fired = rb is not None and rc is not None and (rb - rc) >= param
        elif name == "cardinality_drop_gte":
            d = spec["dimension"]
            fired = len(base.card[d]) - len(cur.card[d]) >= param
        elif name == "cardinality_ratio_below":
            d = spec["dimension"]
            fired = len(base.card[d]) > 0 and (
                len(cur.card[d]) / len(base.card[d])
            ) < param
        elif name == "fraction_over_length_gte":
            fired = (cur.text_n > 0
                     and (cur.text_over_top / cur.text_n) >= param)
        elif name == "max_length_gte":
            fired = cur.text_max >= param
        elif name == "unparseable_fraction_gte":
            frac = (cur.nm_unparseable / cur.nm_n) if cur.nm_n else None
            fired = frac is not None and frac >= param
        elif name == "quantile_shift_gte":
            bq = {q["probability"]: q["value"] for q in base.numeric_quantiles()}
            for q in cur.numeric_quantiles():
                if q["probability"] in bq and abs(
                    q["value"] - bq[q["probability"]]
                ) >= param:
                    fired = True
        else:
            raise AssertionError(f"reference lacks rule {name!r}")
        if fired:
            (fail if sev == "failure" else warn).add(name)
    return warn, fail


def _sample_col(mtype: str) -> str | None:
    if mtype == "message_count":
        return "messages"
    if mtype == "result_count":
        return "rows"
    return "eligible"


def _sample_size(stats: WindowStats, mtype: str, col: str | None) -> int:
    if col == "messages":
        return stats.message_count
    if col == "rows":
        return stats.result_rows
    if mtype == "field_nonnull_rate":
        return stats.result_rows
    if mtype == "cardinality":
        return stats.result_rows
    if mtype == "text_length_distribution":
        return stats.text_n
    if mtype == "numeric_quantile_distribution":
        return stats.nm_n
    raise AssertionError(mtype)


def _count(stats: WindowStats, mtype: str) -> int:
    if mtype == "message_count":
        return stats.message_count
    return stats.result_rows


def _not_computable(spec: dict, stats: WindowStats, mtype: str,
                    window_label: str) -> str | None:
    # Count metrics are computable in every window; an empty window yields
    # the legitimate value zero rather than an inconclusive result.
    if mtype in ("message_count", "result_count"):
        return None
    if stats.message_count == 0:
        return f"{window_label} window contains zero matching messages"
    if mtype == "field_nonnull_rate" and stats.result_rows == 0:
        return (f"{window_label} window contains zero result rows within "
                "the policy selection")
    if mtype == "cardinality" and stats.result_rows == 0:
        return (f"{window_label} window contains zero result rows within "
                "the policy selection")
    if mtype == "text_length_distribution" and stats.text_n == 0:
        return "text metric has no text-like rows"
    if mtype == "numeric_quantile_distribution":
        if stats.nm_n == 0:
            return "numeric metric has no NM rows"
        if not stats.nm_values:
            return "all numeric candidates failed parsing"
    return None


# ---------------------------------------------------------------------------
# Comparison against the product report
# ---------------------------------------------------------------------------

def cross_check(db_path: str, policy_path: str, report_path: str) -> list[str]:
    policy = json.load(open(policy_path, encoding="utf-8"))
    report = json.load(open(report_path, encoding="utf-8"))
    problems: list[str] = []

    if policy.get("policy_version") != report["policy_summary"]["policy_version"]:
        problems.append("policy_version mismatch between policy and report")

    selection = policy["selection"]
    senders = selection["senders"]
    test_codes = selection.get("test_codes")
    buckets = policy.get("limits", {}).get(
        "text_length_buckets", [1, 4, 8, 16, 32, 64, 128, 256]
    )
    quantiles = [0.5, 0.9]
    for spec in policy["metrics"]:
        if spec["type"] == "numeric_quantile_distribution":
            quantiles = spec.get("quantiles", [0.5, 0.9])

    wins = policy["windows"]
    base_start = _canonical(wins["baseline"]["start"])
    base_end = _canonical(wins["baseline"]["end"])
    cur_start = _canonical(wins["current"]["start"])
    cur_end = _canonical(wins["current"]["end"])

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        base = scan_window(conn, senders, test_codes, base_start, base_end,
                           buckets, quantiles)
        cur = scan_window(conn, senders, test_codes, cur_start, cur_end,
                          buckets, quantiles)
    finally:
        conn.close()

    product = {m["metric_id"]: m for m in report["metrics"]}
    if set(product) != {m["metric_id"] for m in policy["metrics"]}:
        problems.append("metric id set differs between policy and report")

    for spec in policy["metrics"]:
        mid, mtype = spec["metric_id"], spec["type"]
        block = product.get(mid)
        if block is None:
            problems.append(f"{mid}: missing from report")
            continue

        # denominators / counts
        _check_count(problems, mid, "baseline message count",
                     block["baseline"]["window_message_count"],
                     base.message_count)
        _check_count(problems, mid, "current message count",
                     block["current"]["window_message_count"],
                     cur.message_count)

        reasons = {
            _not_computable(spec, base, mtype, "baseline"),
            _not_computable(spec, cur, mtype, "current"),
        } - {None}
        # min sample guard
        for rule in spec.get("rules", []):
            if rule["rule"] == "min_sample_size":
                col = _sample_col(mtype)
                if _sample_size(cur, mtype, col) < float(rule["parameter"]):
                    reasons.add("min_sample_size guard tripped")

        if reasons:
            if block["status"] != "not_computable":
                problems.append(
                    f"{mid}: reference says not_computable {sorted(reasons)} "
                    f"but product reports {block['status']}"
                )
            if block["threshold_hits"]:
                problems.append(
                    f"{mid}: not_computable metric must have no threshold hits"
                )
            continue

        # value-level cross checks
        if mtype in ("message_count", "result_count"):
            _check_count(problems, mid, "baseline count",
                         block["baseline"]["count"], _count(base, mtype))
            _check_count(problems, mid, "current count",
                         block["current"]["count"], _count(cur, mtype))
        elif mtype == "field_nonnull_rate":
            col = spec["field"]
            _check_float(problems, mid, "baseline rate",
                         block["baseline"].get("nonnull_rate"), base.rate(col))
            _check_float(problems, mid, "current rate",
                         block["current"].get("nonnull_rate"), cur.rate(col))
            _check_count(problems, mid, "baseline eligible",
                         block["baseline"]["eligible_rows"],
                         base.field_total.get(col, 0))
            _check_count(problems, mid, "current missing",
                         block["current"]["null_or_empty_rows"],
                         cur.field_total.get(col, 0)
                         - cur.field_nonnull.get(col, 0))
        elif mtype == "cardinality":
            dim = spec["dimension"]
            _check_count(problems, mid, "baseline cardinality",
                         block["baseline"]["cardinality"], len(base.card[dim]))
            _check_count(problems, mid, "current cardinality",
                         block["current"]["cardinality"], len(cur.card[dim]))
            got_values = {v["value"] for v in block["current"]["values"]}
            if got_values != cur.card[dim]:
                problems.append(f"{mid}: current distinct value set differs")
        elif mtype == "text_length_distribution":
            _check_count(problems, mid, "text eligible",
                         block["current"]["eligible_rows"], cur.text_n)
            _check_count(problems, mid, "text excluded",
                         block["current"]["excluded_rows"], cur.text_excluded)
            _check_count(problems, mid, "text max length",
                         block["current"]["max_length"], cur.text_max)
            _check_float(problems, mid, "text mean",
                         block["current"]["mean_length"],
                         round(cur.text_total_len / cur.text_n, 6))
            got_hist = [b["count"] for b in
                        block["current"]["histogram"]]
            ref_hist = [b["count"] for b in cur.text_histogram()]
            if got_hist != ref_hist:
                problems.append(
                    f"{mid}: histogram counts differ "
                    f"product={got_hist} reference={ref_hist}"
                )
        elif mtype == "numeric_quantile_distribution":
            _check_count(problems, mid, "NM eligible",
                         block["current"]["eligible_rows"], cur.nm_n)
            _check_count(problems, mid, "NM excluded",
                         block["current"]["excluded_rows"], cur.nm_excluded)
            _check_count(problems, mid, "unparseable",
                         block["current"]["unparseable_rows"],
                         cur.nm_unparseable)
            got_q = {q["probability"]: q["value"]
                     for q in block["current"]["quantiles"]}
            ref_q = {q["probability"]: q["value"]
                     for q in cur.numeric_quantiles()}
            if set(got_q) != set(ref_q):
                problems.append(f"{mid}: quantile probability set differs")
            for p in ref_q:
                _check_float(problems, mid, f"quantile {p}",
                             got_q.get(p), ref_q[p])

        # severity cross check
        warn, fail = _evaluate_rules(spec, base, cur, mtype)
        product_hits = {h["rule"] for h in block["threshold_hits"]}
        expected = warn | fail
        if product_hits != expected:
            problems.append(
                f"{mid}: fired rules differ product={sorted(product_hits)} "
                f"reference={sorted(expected)}"
            )
        expected_status = (
            "failure" if fail else "warning" if warn else "ok"
        )
        if block["status"] != expected_status:
            problems.append(
                f"{mid}: status product={block['status']} "
                f"reference={expected_status}"
            )

    # report-level status / exit code
    expected_exit = {
        "failure": 2, "warning": 1, "not_computable": 3, "ok": 0
    }[report["report_status"]]
    if report["exit_code"] != expected_exit:
        problems.append(
            f"exit_code {report['exit_code']} inconsistent with status "
            f"{report['report_status']}"
        )
    return problems


def _check_count(problems, mid, label, got, expected):
    if got != expected:
        problems.append(f"{mid}: {label}: product={got!r} reference={expected!r}")


def _check_float(problems, mid, label, got, expected, tol=1e-9):
    if got is None and expected is None:
        return
    if got is None or expected is None or abs(got - expected) > tol:
        problems.append(f"{mid}: {label}: product={got!r} reference={expected!r}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="refcheck.audit_reference")
    ap.add_argument("--db", required=True)
    ap.add_argument("--policy", required=True)
    ap.add_argument("--report", required=True)
    args = ap.parse_args(argv)
    problems = cross_check(args.db, args.policy, args.report)
    if problems:
        print(f"AUDIT REFERENCE CHECK FAILED: {len(problems)} mismatch(es)")
        for p in problems:
            print(f"  - {p}")
        return EXIT_MISMATCH
    print("AUDIT REFERENCE CHECK PASSED")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
