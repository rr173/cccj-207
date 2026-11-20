"""Versioned JSON audit policy (strategy) model and strict validation.

A policy selects facts (sender, optional test codes) and declares the two
half-open comparison windows, the metric catalogue and warning/failure
threshold rules.  Every malformed, unsupported or self-inconsistent policy
raises :class:`AuditError` *before* any database work happens -- an invalid
policy can never produce a report that looks like "zero anomalies".

Policy document (``policy_version`` 1)::

    {
      "policy_version": 1,
      "name": "...",                      # optional, echoed into the report
      "selection": {
        "senders": ["APP^FAC", ...],      # >= 1 required
        "test_codes": ["GLU", ...]        # optional; absent/null = all codes
      },
      "windows": {                        # optional when given via CLI
        "baseline": {"start": "...", "end": "..."},
        "current":  {"start": "...", "end": "..."}
      },
      "limits": {"max_samples_per_metric": 20, "text_length_buckets": [...]},
      "metrics": [ {metric spec}, ... ]
    }

Window bounds are ISO-8601 instants; semantics are ``[start, end)`` on the
message ``received_at`` column.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

POLICY_VERSION = 1

# ---------------------------------------------------------------------------
# Metric vocabulary
# ---------------------------------------------------------------------------
METRIC_MESSAGE_COUNT = "message_count"
METRIC_RESULT_COUNT = "result_count"
METRIC_FIELD_NONNULL = "field_nonnull_rate"
METRIC_CARDINALITY = "cardinality"
METRIC_TEXT_LENGTH = "text_length_distribution"
METRIC_NUMERIC_QUANTILES = "numeric_quantile_distribution"

METRIC_TYPES = frozenset(
    {
        METRIC_MESSAGE_COUNT,
        METRIC_RESULT_COUNT,
        METRIC_FIELD_NONNULL,
        METRIC_CARDINALITY,
        METRIC_TEXT_LENGTH,
        METRIC_NUMERIC_QUANTILES,
    }
)

# Rule vocabulary.  Which rules apply to which metric is enforced below.
RULE_COUNT_DROP_GTE = "count_drop_gte"          # count metrics: absolute drop
RULE_COUNT_RATIO_BELOW = "current_to_baseline_ratio_below"
RULE_RATE_BELOW = "nonnull_rate_below"          # rate metrics
RULE_RATE_DROP_GTE = "nonnull_rate_drop_gte"
RULE_CARD_DROP_GTE = "cardinality_drop_gte"     # cardinality metrics
RULE_CARD_RATIO_BELOW = "cardinality_ratio_below"
RULE_LEN_FRAC_OVER_GTE = "fraction_over_length_gte"  # text length metric
RULE_LEN_MAX_GTE = "max_length_gte"
RULE_NUM_QSHIFT_GTE = "quantile_shift_gte"      # numeric quantile metric
RULE_NUM_UNPARSEABLE_FRAC_GTE = "unparseable_fraction_gte"
RULE_MIN_SAMPLE = "min_sample_size"             # rate/card/distribution only

COUNT_RULES = frozenset(
    {RULE_COUNT_DROP_GTE, RULE_COUNT_RATIO_BELOW, RULE_MIN_SAMPLE}
)
RATE_RULES = frozenset({RULE_RATE_BELOW, RULE_RATE_DROP_GTE, RULE_MIN_SAMPLE})
CARD_RULES = frozenset(
    {RULE_CARD_DROP_GTE, RULE_CARD_RATIO_BELOW, RULE_MIN_SAMPLE}
)
TEXT_RULES = frozenset(
    {RULE_LEN_FRAC_OVER_GTE, RULE_LEN_MAX_GTE, RULE_MIN_SAMPLE}
)
NUMERIC_RULES = frozenset(
    {RULE_NUM_QSHIFT_GTE, RULE_NUM_UNPARSEABLE_FRAC_GTE, RULE_MIN_SAMPLE}
)

RULES_BY_METRIC = {
    METRIC_MESSAGE_COUNT: COUNT_RULES,
    METRIC_RESULT_COUNT: COUNT_RULES,
    METRIC_FIELD_NONNULL: RATE_RULES,
    METRIC_CARDINALITY: CARD_RULES,
    METRIC_TEXT_LENGTH: TEXT_RULES,
    METRIC_NUMERIC_QUANTILES: NUMERIC_RULES,
}

# Cardinality dimensions map directly to result-fact columns.
CARD_SENDER = "sender"
CARD_TEST_CODE = "test_code"
CARD_OBX2_TYPE = "obx2_type"
CARDINALITY_DIMENSIONS = frozenset(
    {CARD_SENDER, CARD_TEST_CODE, CARD_OBX2_TYPE}
)

# Fact columns whose non-null rate can be audited.  Required and optional
# facts follow the profile in docs/PROFILE.md; the NOT NULL columns are
# marked required (their rate is always 1 but the metric is still useful as
# a structural invariant and for drill coverage).
REQUIRED_FIELDS = frozenset(
    {
        "sender",
        "control_id",
        "test_code",
        "test_system",
        "obs_code",
        "obs_system",
        "value_type",
        "value_text",
        "rstatus",
        "obr_index",
        "obx_index",
    }
)
OPTIONAL_FIELDS = frozenset(
    {
        "patient_authority",
        "patient_id",
        "visit_id",
        "placer_order",
        "filler_order",
        "test_name",
        "obs_name",
        "sub_id",
        "units",
        "ref_range",
        "abnormal",
        "observed_at",
        "reported_at",
    }
)
NONNULL_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS

# OBX-2 types whose OBX-5 is textual (free text / coded text rendered as
# text).  Numeric (NM), date/time (DT/TM/TS) are not text-like; coded CE/CWE
# identifiers are codes rather than free text.
TEXTUAL_OBX2_TYPES = frozenset({"ST", "TX", "FT"})
# OBX-2 types whose values are eligible for numeric parsing attempts.
NUMERIC_OBX2_TYPES = frozenset({"NM"})

DEFAULT_MAX_SAMPLES = 20
# Fixed, documented length buckets (upper edges in characters, last bin is
# open-ended).  Streaming counts keep the text metric O(buckets) in memory.
DEFAULT_LENGTH_BUCKETS = [1, 4, 8, 16, 32, 64, 128, 256]
DEFAULT_QUANTILES = [0.5, 0.9]

SEVERITY_WARNING = "warning"
SEVERITY_FAILURE = "failure"
SEVERITIES = frozenset({SEVERITY_WARNING, SEVERITY_FAILURE})


class AuditError(ValueError):
    """The audit cannot run as requested (invalid policy/parameters)."""


@dataclass(frozen=True)
class Window:
    start: str
    end: str

    def as_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True)
class MetricSpec:
    metric_id: str
    metric_type: str
    # field_nonnull_rate: fact column name
    field: str | None = None
    # cardinality: which dimension to count
    dimension: str | None = None
    # numeric quantiles: quantile probabilities in [0, 1]
    quantiles: tuple[float, ...] = ()
    # ordered list of (rule_name, severity, parameter)
    rules: tuple[tuple[str, str, float], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"metric_id": self.metric_id, "type": self.metric_type}
        if self.field is not None:
            out["field"] = self.field
        if self.dimension is not None:
            out["dimension"] = self.dimension
        if self.quantiles:
            out["quantiles"] = list(self.quantiles)
        out["rules"] = [
            {"rule": name, "severity": severity, "parameter": param}
            for name, severity, param in self.rules
        ]
        return out


@dataclass(frozen=True)
class Policy:
    name: str
    senders: tuple[str, ...]
    test_codes: tuple[str, ...] | None  # None -> no code restriction
    baseline: Window | None
    current: Window | None
    metrics: tuple[MetricSpec, ...]
    max_samples: int
    length_buckets: tuple[int, ...]
    source_path: str | None = field(default=None)

    def selection_dict(self) -> dict[str, Any]:
        return {
            "senders": list(self.senders),
            "test_codes": None if self.test_codes is None else list(self.test_codes),
        }

    def limits_dict(self) -> dict[str, Any]:
        return {
            "max_samples_per_metric": self.max_samples,
            "text_length_buckets": list(self.length_buckets),
        }


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise AuditError(msg)


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _validate_window(raw: Any, label: str) -> Window:
    _require(isinstance(raw, dict), f"windows.{label} must be an object")
    start, end = raw.get("start"), raw.get("end")
    _require(isinstance(start, str) and start != "",
             f"windows.{label}.start must be a non-empty string")
    _require(isinstance(end, str) and end != "",
             f"windows.{label}.end must be a non-empty string")
    return Window(start=start, end=end)


def _validate_rules(
    raw_rules: Any, metric_type: str, metric_id: str
) -> tuple[tuple[str, str, float], ...]:
    allowed = RULES_BY_METRIC[metric_type]
    _require(isinstance(raw_rules, list),
             f"metric {metric_id!r}: rules must be a list")
    parsed: list[tuple[str, str, float]] = []
    seen: set[str] = set()
    for i, rule in enumerate(raw_rules):
        _require(isinstance(rule, dict),
                 f"metric {metric_id!r}: rule #{i} must be an object")
        name = rule.get("rule")
        severity = rule.get("severity")
        param = rule.get("parameter")
        _require(isinstance(name, str) and name in allowed,
                 f"metric {metric_id!r}: rule {name!r} is not valid for "
                 f"metric type {metric_type!r} (allowed: {sorted(allowed)})")
        _require(severity in SEVERITIES,
                 f"metric {metric_id!r}: rule {name!r} severity must be one "
                 f"of {sorted(SEVERITIES)}")
        _require(_is_number(param),
                 f"metric {metric_id!r}: rule {name!r} parameter must be a number")
        _require(name not in seen,
                 f"metric {metric_id!r}: duplicate rule {name!r}")
        seen.add(name)
        # parameter-specific sanity bounds
        if name == RULE_MIN_SAMPLE:
            _require(float(param) >= 0 and float(param) == int(param),
                     f"metric {metric_id!r}: min_sample_size must be a "
                     "non-negative integer")
        elif name in (RULE_COUNT_RATIO_BELOW, RULE_CARD_RATIO_BELOW):
            _require(float(param) >= 0,
                     f"metric {metric_id!r}: ratio parameter must be >= 0")
        elif name in (
            RULE_RATE_BELOW, RULE_RATE_DROP_GTE, RULE_LEN_FRAC_OVER_GTE,
            RULE_NUM_UNPARSEABLE_FRAC_GTE,
        ):
            _require(0.0 <= float(param) <= 1.0,
                     f"metric {metric_id!r}: fraction parameter must be in [0, 1]")
        elif name in (RULE_COUNT_DROP_GTE, RULE_CARD_DROP_GTE):
            _require(float(param) >= 0,
                     f"metric {metric_id!r}: drop parameter must be >= 0")
        parsed.append((name, severity, float(param)))
    return tuple(parsed)


def _validate_metric(raw: Any, seen_ids: set[str]) -> MetricSpec:
    _require(isinstance(raw, dict), "metrics entries must be objects")
    metric_id = raw.get("metric_id")
    _require(isinstance(metric_id, str) and metric_id != "",
             "every metric needs a non-empty string metric_id")
    _require(metric_id not in seen_ids, f"duplicate metric_id {metric_id!r}")
    seen_ids.add(metric_id)

    metric_type = raw.get("type")
    _require(metric_type in METRIC_TYPES,
             f"metric {metric_id!r}: unknown type {metric_type!r}; "
             f"known: {sorted(METRIC_TYPES)}")

    field = dimension = None
    quantiles: tuple[float, ...] = ()

    if metric_type == METRIC_FIELD_NONNULL:
        field = raw.get("field")
        _require(isinstance(field, str) and field in NONNULL_FIELDS,
                 f"metric {metric_id!r}: field must name a result-fact "
                 f"column (one of {sorted(NONNULL_FIELDS)})")
    elif metric_type == METRIC_CARDINALITY:
        dimension = raw.get("dimension")
        _require(dimension in CARDINALITY_DIMENSIONS,
                 f"metric {metric_id!r}: dimension must be one of "
                 f"{sorted(CARDINALITY_DIMENSIONS)}")
    elif metric_type == METRIC_NUMERIC_QUANTILES:
        q = raw.get("quantiles", list(DEFAULT_QUANTILES))
        _require(isinstance(q, list) and q,
                 f"metric {metric_id!r}: quantiles must be a non-empty list")
        for p in q:
            _require(_is_number(p) and 0.0 <= float(p) <= 1.0,
                     f"metric {metric_id!r}: quantile probabilities must be "
                     "numbers in [0, 1]")
        quantiles = tuple(sorted(float(p) for p in q))
        _require(len(set(quantiles)) == len(quantiles),
                 f"metric {metric_id!r}: duplicate quantile probabilities")

    rules = _validate_rules(raw.get("rules", []), metric_type, metric_id)
    return MetricSpec(
        metric_id=metric_id, metric_type=metric_type, field=field,
        dimension=dimension, quantiles=quantiles, rules=rules,
    )


def _validate_limits(raw: Any) -> tuple[int, tuple[int, ...]]:
    if raw is None:
        return DEFAULT_MAX_SAMPLES, tuple(DEFAULT_LENGTH_BUCKETS)
    _require(isinstance(raw, dict), "limits must be an object")
    max_samples = raw.get("max_samples_per_metric", DEFAULT_MAX_SAMPLES)
    _require(isinstance(max_samples, int) and not isinstance(max_samples, bool)
             and max_samples >= 1,
             "limits.max_samples_per_metric must be a positive integer")
    buckets = raw.get("text_length_buckets", list(DEFAULT_LENGTH_BUCKETS))
    _require(isinstance(buckets, list) and len(buckets) >= 1,
             "limits.text_length_buckets must be a non-empty list")
    out: list[int] = []
    for b in buckets:
        _require(isinstance(b, int) and not isinstance(b, bool) and b >= 1,
                 "text length bucket edges must be positive integers")
        _require(not out or b > out[-1],
                 "text length bucket edges must be strictly increasing")
        out.append(b)
    return max_samples, tuple(out)


def parse_policy_text(text: str, *, source_path: str | None = None) -> Policy:
    """Parse and validate policy JSON text."""
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AuditError(f"policy is not valid JSON: {exc}") from exc
    _require(isinstance(doc, dict), "policy document must be a JSON object")

    version = doc.get("policy_version")
    _require(version == POLICY_VERSION,
             f"unsupported policy_version {version!r}; this build supports "
             f"{POLICY_VERSION}")

    name = doc.get("name")
    _require(name is None or isinstance(name, str),
             "policy name must be a string when present")

    selection = doc.get("selection")
    _require(isinstance(selection, dict), "selection must be an object")
    senders = selection.get("senders")
    _require(isinstance(senders, list) and len(senders) >= 1,
             "selection.senders must be a non-empty list of 'APP^FAC' strings")
    for s in senders:
        _require(isinstance(s, str) and s != "",
                 "selection.senders entries must be non-empty strings")
    _require(len(set(senders)) == len(senders),
             "selection.senders contains duplicates")
    test_codes = selection.get("test_codes")
    if test_codes is not None:
        _require(isinstance(test_codes, list) and len(test_codes) >= 1,
                 "selection.test_codes must be a non-empty list when present")
        for c in test_codes:
            _require(isinstance(c, str) and c != "",
                     "selection.test_codes entries must be non-empty strings")
        _require(len(set(test_codes)) == len(test_codes),
                 "selection.test_codes contains duplicates")
        test_codes = tuple(test_codes)

    windows = doc.get("windows")
    baseline = current = None
    if windows is not None:
        _require(isinstance(windows, dict), "windows must be an object")
        if "baseline" in windows:
            baseline = _validate_window(windows["baseline"], "baseline")
        if "current" in windows:
            current = _validate_window(windows["current"], "current")
        _require((baseline is None) == (current is None),
                 "windows must define both baseline and current, or neither")

    metric_docs = doc.get("metrics")
    _require(isinstance(metric_docs, list) and len(metric_docs) >= 1,
             "metrics must be a non-empty list")
    seen_ids: set[str] = set()
    metrics = tuple(_validate_metric(m, seen_ids) for m in metric_docs)

    max_samples, buckets = _validate_limits(doc.get("limits"))

    return Policy(
        name=name or "",
        senders=tuple(senders),
        test_codes=test_codes,
        baseline=baseline,
        current=current,
        metrics=metrics,
        max_samples=max_samples,
        length_buckets=buckets,
        source_path=source_path,
    )


def load_policy_text(text: str, *, source_path: str | None = None) -> Policy:
    """Public alias kept stable for callers importing from the package."""
    return parse_policy_text(text, source_path=source_path)


def load_policy(path: str) -> Policy:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        raise AuditError(f"cannot read policy file {path!r}: {exc}") from exc
    return parse_policy_text(text, source_path=path)
