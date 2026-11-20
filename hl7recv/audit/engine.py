"""Audit engine: snapshot one view, compute both windows, assemble the report.

Report status vocabulary (per metric and for the whole run):

``ok``
    Metric computed in both windows and no threshold rule fired.
``warning`` / ``failure``
    At least one rule of that severity fired (failure outranks warning).
``not_computable``
    The metric has no calculable value for an applicable reason: an empty
    window, zero eligible rows, an all-empty column, or a policy
    ``min_sample_size`` guard.  Explicit reasons are always included; such
    metrics never fire threshold rules and can never be mistaken for "zero
    anomaly".

Exit codes: 0 ok, 1 warning, 2 failure, 3 inconclusive (any
not_computable metric without a higher-severity finding), 4 usage
(invalid policy/parameters -- no report is produced).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .metrics import build_accumulators, round_finite
from .policy import (
    AuditError,
    METRIC_CARDINALITY,
    METRIC_FIELD_NONNULL,
    METRIC_MESSAGE_COUNT,
    METRIC_NUMERIC_QUANTILES,
    METRIC_RESULT_COUNT,
    METRIC_TEXT_LENGTH,
    REQUIRED_FIELDS,
    RULE_CARD_DROP_GTE,
    RULE_CARD_RATIO_BELOW,
    RULE_COUNT_DROP_GTE,
    RULE_COUNT_RATIO_BELOW,
    RULE_LEN_FRAC_OVER_GTE,
    RULE_LEN_MAX_GTE,
    RULE_MIN_SAMPLE,
    RULE_NUM_QSHIFT_GTE,
    RULE_NUM_UNPARSEABLE_FRAC_GTE,
    RULE_RATE_BELOW,
    RULE_RATE_DROP_GTE,
    SEVERITY_FAILURE,
    SEVERITY_WARNING,
    TEXTUAL_OBX2_TYPES,
    MetricSpec,
    Policy,
)
from .snapshot import AuditSnapshot
from .timebounds import resolve_windows

REPORT_FORMAT_VERSION = 1

EXIT_OK = 0
EXIT_WARNING = 1
EXIT_FAILURE = 2
EXIT_INCONCLUSIVE = 3
EXIT_USAGE = 4

STATUS_OK = "ok"
STATUS_WARNING = "warning"
STATUS_FAILURE = "failure"
STATUS_NOT_COMPUTABLE = "not_computable"

# ---------------------------------------------------------------------------
# Window computation
# ---------------------------------------------------------------------------

@dataclass
class WindowResult:
    message_count: int
    accumulator_values: dict[str, dict]


def _compute_window(
    snapshot: AuditSnapshot,
    policy: Policy,
    start_canonical: str,
    end_canonical: str,
) -> WindowResult:
    accs = build_accumulators(
        policy.metrics, policy.max_samples, policy.length_buckets
    )
    message_count = snapshot.count_messages_in_window(
        policy.senders, start_canonical, end_canonical
    )
    for row in snapshot.stream_window_results(
        policy.senders, policy.test_codes, start_canonical, end_canonical
    ):
        for acc in accs.values():
            acc.note_message(row)
            acc.add_row(row)
    values = {mid: acc.value() for mid, acc in accs.items()}
    return WindowResult(
        message_count=message_count, accumulator_values=values
    )


# ---------------------------------------------------------------------------
# Not-computable classification
# ---------------------------------------------------------------------------

def _not_computable_reason(
    spec: MetricSpec, window_name: str, value: dict, message_count: int
) -> str | None:
    mtype = spec.metric_type
    # Count metrics are always computable: an empty window yields the
    # legitimate value zero, not an inconclusive metric.
    if mtype in (METRIC_MESSAGE_COUNT, METRIC_RESULT_COUNT):
        return None
    if message_count == 0:
        return f"{window_name} window contains zero matching messages"
    if mtype == METRIC_FIELD_NONNULL:
        if value["eligible_rows"] == 0:
            return (f"{window_name} window contains zero result rows within "
                    "the policy selection")
        return None
    if mtype == METRIC_CARDINALITY:
        if value["eligible_rows"] == 0:
            return (f"{window_name} window contains zero result rows within "
                    "the policy selection")
        return None
    if mtype == METRIC_TEXT_LENGTH:
        if value["eligible_rows"] == 0:
            return (f"no text-like OBX-2 rows {sorted(TEXTUAL_OBX2_TYPES)} "
                    f"in the {window_name} window (eligible rows of other "
                    f"types: {value['excluded_rows']})")
        return None
    if mtype == METRIC_NUMERIC_QUANTILES:
        if value["eligible_rows"] == 0:
            return (f"no numeric OBX-2 rows in the {window_name} window "
                    f"(excluded non-numeric rows: {value['excluded_rows']})")
        if value["parseable_rows"] == 0:
            return (f"all {value['eligible_rows']} numeric-candidate rows in "
                    "the current window failed finite-number parsing")
        return None
    return None


# ---------------------------------------------------------------------------
# Rule evaluation
# ---------------------------------------------------------------------------

def _rule_fired(spec: MetricSpec, rule_name: str, param: float,
                base: dict, cur: dict) -> tuple[bool, dict]:
    """Return (fired, evidence).  Evidence is rule-specific and stable."""
    mtype = spec.metric_type

    if rule_name == RULE_MIN_SAMPLE:
        col = ("eligible_rows" if mtype != METRIC_MESSAGE_COUNT
               and mtype != METRIC_RESULT_COUNT else None)
        if col is None:
            col = ("eligible_result_rows" if mtype == METRIC_RESULT_COUNT
                   else "eligible_messages")
        n_cur = cur.get(col, 0)
        return n_cur < param, {"current_sample_size": n_cur,
                               "required_sample_size": param}

    if rule_name == RULE_COUNT_DROP_GTE:
        drop = base["count"] - cur["count"]
        return drop >= param, {"baseline_count": base["count"],
                               "current_count": cur["count"],
                               "absolute_drop": drop}

    if rule_name == RULE_COUNT_RATIO_BELOW:
        if base["count"] == 0:
            return False, {"baseline_count": 0, "ratio": None}
        ratio = cur["count"] / base["count"]
        return ratio < param, {"baseline_count": base["count"],
                               "current_count": cur["count"],
                               "ratio": round_finite(ratio)}

    if rule_name == RULE_RATE_BELOW:
        rate = cur.get("nonnull_rate")
        if rate is None:
            return False, {}
        return rate < param, {"current_nonnull_rate": rate}

    if rule_name == RULE_RATE_DROP_GTE:
        rb, rc = base.get("nonnull_rate"), cur.get("nonnull_rate")
        if rb is None or rc is None:
            return False, {}
        drop = rb - rc
        return drop >= param, {"baseline_nonnull_rate": rb,
                               "current_nonnull_rate": rc,
                               "rate_drop": round_finite(drop)}

    if rule_name == RULE_CARD_DROP_GTE:
        drop = base["cardinality"] - cur["cardinality"]
        return drop >= param, {"baseline_cardinality": base["cardinality"],
                               "current_cardinality": cur["cardinality"],
                               "absolute_drop": drop}

    if rule_name == RULE_CARD_RATIO_BELOW:
        if base["cardinality"] == 0:
            return False, {"baseline_cardinality": 0, "ratio": None}
        ratio = cur["cardinality"] / base["cardinality"]
        return ratio < param, {
            "baseline_cardinality": base["cardinality"],
            "current_cardinality": cur["cardinality"],
            "ratio": round_finite(ratio),
        }

    if rule_name == RULE_LEN_FRAC_OVER_GTE:
        # the last histogram bin is the open-ended "(top_edge, inf)" bin;
        # its lower edge equals the largest configured bucket edge
        over_cur = (cur["histogram"][-1]["count"]
                    if cur.get("histogram") else 0)
        over_base = (base["histogram"][-1]["count"]
                     if base.get("histogram") else 0)
        top_edge = (cur["histogram"][-1]["lower_exclusive"]
                    if cur.get("histogram") else None)
        n_cur = cur["eligible_rows"]
        n_base = base["eligible_rows"]
        frac_cur = over_cur / n_cur if n_cur else 0.0
        frac_base = over_base / n_base if n_base else 0.0
        return frac_cur >= param, {
            "length_threshold": top_edge,
            "baseline_over_count": over_base,
            "current_over_count": over_cur,
            "baseline_fraction_over": round_finite(frac_base),
            "current_fraction_over": round_finite(frac_cur),
        }

    if rule_name == RULE_LEN_MAX_GTE:
        mx = cur.get("max_length")
        if mx is None:
            return False, {}
        return mx >= param, {"current_max_length": mx,
                             "baseline_max_length": base.get("max_length")}

    if rule_name == RULE_NUM_UNPARSEABLE_FRAC_GTE:
        frac = cur.get("unparseable_fraction")
        if frac is None:
            return False, {}
        return frac >= param, {"current_unparseable_fraction": frac,
                               "baseline_unparseable_fraction":
                                   base.get("unparseable_fraction")}

    if rule_name == RULE_NUM_QSHIFT_GTE:
        # param is interpreted as {"probability": p, "absolute_shift": s}
        # encoded via two rules?  Policy keeps this rule parameterized with
        # a single absolute shift applied to every declared quantile.
        shifts = []
        fired = False
        base_q = {q["probability"]: q["value"] for q in base.get("quantiles", [])}
        for q in cur.get("quantiles", []):
            p = q["probability"]
            if p not in base_q:
                continue
            shift = abs(q["value"] - base_q[p])
            shifts.append({"probability": p,
                           "baseline_value": base_q[p],
                           "current_value": q["value"],
                           "absolute_shift": round_finite(shift)})
            if shift >= param:
                fired = True
        return fired, {"shifts": shifts, "shift_threshold": param}

    raise AuditError(f"rule {rule_name!r} has no evaluator")  # pragma: no cover


# ---------------------------------------------------------------------------
# Per-metric comparison and evidence
# ---------------------------------------------------------------------------

def _diff(spec: MetricSpec, base: dict, cur: dict) -> dict:
    mtype = spec.metric_type
    if mtype in (METRIC_MESSAGE_COUNT, METRIC_RESULT_COUNT):
        return {
            "baseline": base["count"],
            "current": cur["count"],
            "absolute_diff": cur["count"] - base["count"],
        }
    if mtype == METRIC_FIELD_NONNULL:
        rb, rc = base.get("nonnull_rate"), cur.get("nonnull_rate")
        return {
            "baseline": rb,
            "current": rc,
            "absolute_diff": (round_finite(rc - rb)
                              if rb is not None and rc is not None else None),
        }
    if mtype == METRIC_CARDINALITY:
        return {
            "baseline": base["cardinality"],
            "current": cur["cardinality"],
            "absolute_diff": cur["cardinality"] - base["cardinality"],
        }
    if mtype == METRIC_TEXT_LENGTH:
        return {
            "baseline": {
                "eligible_rows": base.get("eligible_rows", 0),
                "max_length": base.get("max_length"),
                "mean_length": base.get("mean_length"),
            },
            "current": {
                "eligible_rows": cur.get("eligible_rows", 0),
                "max_length": cur.get("max_length"),
                "mean_length": cur.get("mean_length"),
            },
            "absolute_diff": {
                "max_length": (_diff_num(cur.get("max_length"),
                                         base.get("max_length"))),
                "mean_length": (_diff_num(cur.get("mean_length"),
                                          base.get("mean_length"))),
            },
        }
    if mtype == METRIC_NUMERIC_QUANTILES:
        base_q = {q["probability"]: q["value"] for q in base.get("quantiles", [])}
        rows = []
        for q in cur.get("quantiles", []):
            p = q["value"]
            bv = base_q.get(q["probability"])
            rows.append({
                "probability": q["probability"],
                "baseline": bv,
                "current": p,
                "absolute_diff": round_finite(p - bv) if bv is not None else None,
            })
        return {"quantiles": rows,
                "baseline_unparseable_fraction": base.get("unparseable_fraction"),
                "current_unparseable_fraction": cur.get("unparseable_fraction")}
    raise AuditError(f"metric type {mtype!r} has no diff")  # pragma: no cover


def _diff_num(cur, base):
    if cur is None or base is None:
        return None
    return round_finite(cur - base)


def _evidence_samples(spec: MetricSpec, base: dict, cur: dict) -> dict:
    """Collect bounded, fixed-order samples supporting the metric finding."""
    mtype = spec.metric_type
    if mtype in (METRIC_MESSAGE_COUNT, METRIC_RESULT_COUNT):
        return {"current_window_samples": cur.get("samples", _blank())}
    if mtype == METRIC_FIELD_NONNULL:
        return {"missing_value_samples": cur.get("missing_samples", _blank())}
    if mtype == METRIC_CARDINALITY:
        return {
            "baseline_values": base.get("values", []),
            "current_values": cur.get("values", []),
        }
    if mtype == METRIC_TEXT_LENGTH:
        return {
            "long_value_samples": cur.get("long_value_samples", _blank()),
        }
    if mtype == METRIC_NUMERIC_QUANTILES:
        return {
            "unparseable_samples": cur.get("unparseable_samples", _blank()),
        }
    return {}


def _blank() -> dict:
    return {"sample_cap": 0, "candidate_count": 0, "returned_count": 0,
            "truncated": False, "items": []}


# ---------------------------------------------------------------------------
# Metric status assembly
# ---------------------------------------------------------------------------

def _evaluate_metric(
    spec: MetricSpec,
    base: dict,
    cur: dict,
    base_messages: int,
    cur_messages: int,
) -> dict:
    base_block: dict = {"window_message_count": base_messages, **base}
    cur_block: dict = {"window_message_count": cur_messages, **cur}

    reason_b = _not_computable_reason(spec, "baseline", base, base_messages)
    reason_c = _not_computable_reason(spec, "current", cur, cur_messages)
    reasons = [r for r in (reason_b, reason_c) if r]

    # min_sample_size guard is a policy-declared applicability boundary.
    min_rule = next((r for r in spec.rules if r[0] == RULE_MIN_SAMPLE), None)
    if not reasons and min_rule is not None:
        _, _, param = min_rule
        mtype = spec.metric_type
        if mtype in (METRIC_MESSAGE_COUNT, METRIC_RESULT_COUNT):
            col = ("eligible_result_rows" if mtype == METRIC_RESULT_COUNT
                   else "eligible_messages")
        else:
            col = "eligible_rows"
        if cur.get(col, 0) < param:
            reasons.append(
                f"current window sample size {cur.get(col, 0)} is below the "
                f"policy min_sample_size {int(param)}"
            )

    fired_rules: list[dict] = []
    if not reasons:
        for rule_name, severity, param in spec.rules:
            if rule_name == RULE_MIN_SAMPLE:
                continue
            ok, evidence = _rule_fired(spec, rule_name, param, base, cur)
            if ok:
                fired_rules.append({
                    "rule": rule_name,
                    "severity": severity,
                    "parameter": param,
                    "evidence": evidence,
                })

    if reasons:
        status = STATUS_NOT_COMPUTABLE
    elif any(r["severity"] == SEVERITY_FAILURE for r in fired_rules):
        status = STATUS_FAILURE
    elif any(r["severity"] == SEVERITY_WARNING for r in fired_rules):
        status = STATUS_WARNING
    else:
        status = STATUS_OK

    out = {
        "metric_id": spec.metric_id,
        "type": spec.metric_type,
        "status": status,
        "spec": spec.as_dict(),
        "calculation": _calculation_note(spec),
        "baseline": base_block,
        "current": cur_block,
        "diff": _diff(spec, base, cur) if not reasons else None,
        "threshold_hits": fired_rules,
        "not_computable_reasons": reasons,
        "evidence": _evidence_samples(spec, base, cur),
    }
    if spec.metric_type == METRIC_FIELD_NONNULL:
        out["field_classification"] = (
            "required" if spec.field in REQUIRED_FIELDS else "optional"
        )
    return out


def _calculation_note(spec: MetricSpec) -> dict:
    mtype = spec.metric_type
    notes = {
        METRIC_MESSAGE_COUNT: {
            "population": "committed messages matching selection in window",
            "denominator_basis": "messages.received_at [start, end)",
        },
        METRIC_RESULT_COUNT: {
            "population": "result facts of matching committed messages",
            "denominator_basis": "results joined to in-window messages",
        },
        METRIC_FIELD_NONNULL: {
            "population": "all result rows in selection",
            "nonnull_definition": "value is neither SQL NULL nor empty string",
        },
        METRIC_CARDINALITY: {
            "population": "all result rows in selection",
            "definition": "count of DISTINCT dimension values",
        },
        METRIC_TEXT_LENGTH: {
            "population": f"rows whose OBX-2 type is in "
                          f"{sorted(TEXTUAL_OBX2_TYPES)}",
            "length_definition": "Python character count of stored value_text",
            "excluded_rows_reported": True,
        },
        METRIC_NUMERIC_QUANTILES: {
            "population": "rows whose OBX-2 type is NM",
            "parse_rule": "float() on stripped text; non-finite/non-numeric "
                          "counted as unparseable, original text unchanged",
            "quantile_method": "R-7 linear interpolation on sorted values",
            "clinical_use": "shape statistics only; no medical interpretation",
        },
    }
    return notes[mtype]


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_audit(db_path: str, policy: Policy) -> tuple[dict, int]:
    """Run one audit and return (report_dict, exit_code)."""
    if policy.baseline is None or policy.current is None:
        raise AuditError(
            "policy does not declare windows; supply --baseline-start/"
            "--baseline-end/--current-start/--current-end"
        )
    base_win, cur_win = resolve_windows(policy.baseline, policy.current)

    with AuditSnapshot(db_path) as snapshot:
        snap_info = snapshot.snapshot.as_dict()
        base_res = _compute_window(
            snapshot, policy,
            base_win["start_canonical"], base_win["end_canonical"],
        )
        cur_res = _compute_window(
            snapshot, policy,
            cur_win["start_canonical"], cur_win["end_canonical"],
        )

    metric_blocks = [
        _evaluate_metric(
            spec,
            base_res.accumulator_values[spec.metric_id],
            cur_res.accumulator_values[spec.metric_id],
            base_res.message_count,
            cur_res.message_count,
        )
        for spec in policy.metrics
    ]

    statuses = [b["status"] for b in metric_blocks]
    if STATUS_FAILURE in statuses:
        run_status, exit_code = STATUS_FAILURE, EXIT_FAILURE
    elif STATUS_WARNING in statuses:
        run_status, exit_code = STATUS_WARNING, EXIT_WARNING
    elif STATUS_NOT_COMPUTABLE in statuses:
        run_status, exit_code = STATUS_NOT_COMPUTABLE, EXIT_INCONCLUSIVE
    else:
        run_status, exit_code = STATUS_OK, EXIT_OK

    report = {
        "report_format_version": REPORT_FORMAT_VERSION,
        "report_status": run_status,
        "exit_code": exit_code,
        "policy_summary": {
            "name": policy.name,
            "policy_version": 1,
            "source_path": policy.source_path,
            "selection": policy.selection_dict(),
            "limits": policy.limits_dict(),
            "metric_ids": [m.metric_id for m in policy.metrics],
        },
        "windows": {"baseline": base_win, "current": cur_win},
        "query_filters": _query_filters(policy, base_win, cur_win),
        "snapshot": snap_info,
        "metrics": metric_blocks,
        "summary": {
            "metrics_total": len(metric_blocks),
            "metrics_ok": statuses.count(STATUS_OK),
            "metrics_warning": statuses.count(STATUS_WARNING),
            "metrics_failure": statuses.count(STATUS_FAILURE),
            "metrics_not_computable": statuses.count(STATUS_NOT_COMPUTABLE),
            "baseline_matching_messages": base_res.message_count,
            "current_matching_messages": cur_res.message_count,
        },
        "scope_statement": (
            "Describes data shape and pipeline quality of confirmed result "
            "facts only. Rejected or unconfirmed inbox records are excluded. "
            "No clinical interpretation of result values is performed."
        ),
    }
    return report, exit_code


def _query_filters(policy: Policy, base_win: dict, cur_win: dict) -> dict:
    return {
        "fact_tables": ["messages", "results"],
        "excluded_tables": ["inbox", "frame_errors", "notes", "patients"],
        "message_status_filter": "messages.status = 'committed'",
        "sender_filter": list(policy.senders),
        "test_code_filter": (
            [] if policy.test_codes is None else list(policy.test_codes)
        ),
        "join": "results JOIN messages ON (sender, control_id)",
        "window_predicate": "received_at >= start AND received_at < end",
        "baseline_sql_literals": [
            base_win["start_canonical"], base_win["end_canonical"]
        ],
        "current_sql_literals": [
            cur_win["start_canonical"], cur_win["end_canonical"]
        ],
    }


def render_report(report: dict) -> str:
    """Byte-stable serialization: sorted keys, fixed separators, trailing LF."""
    return json.dumps(
        report, indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
