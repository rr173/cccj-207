"""Streaming quality-metric accumulators.

Design properties
-----------------
* One accumulator instance per metric per window; result rows are fed in
  stable order from a single :class:`~hl7recv.audit.snapshot.AuditSnapshot`.
* Bounded memory: cardinality keeps distinct-key sets (the quantity being
  measured); text-length uses fixed histogram bins; numeric values use a
  compact ``array.array('d')``; evidence samples never exceed the policy
  sample cap.  Nothing retains the full result set.
* Every value carries explicit ``eligible`` / ``excluded`` / ``unparseable``
  counts so denominators are auditable.  Empty windows, all-empty columns
  and inapplicable metrics surface as ``not_computable`` -- never as a
  silent zero.
* Numeric parsing never rewrites the stored value and is used only for
  shape statistics, never for medical interpretation.
"""

from __future__ import annotations

import array
import math
from dataclasses import dataclass, field

from .policy import (
    CARD_OBX2_TYPE,
    CARD_SENDER,
    CARD_TEST_CODE,
    METRIC_CARDINALITY,
    METRIC_FIELD_NONNULL,
    METRIC_MESSAGE_COUNT,
    METRIC_NUMERIC_QUANTILES,
    METRIC_RESULT_COUNT,
    METRIC_TEXT_LENGTH,
    NONNULL_FIELDS,
    NUMERIC_OBX2_TYPES,
    TEXTUAL_OBX2_TYPES,
    MetricSpec,
)

# ---------------------------------------------------------------------------
# Source coordinates
# ---------------------------------------------------------------------------

def source_coord(row) -> dict[str, object]:
    """Stable source coordinate for one result row."""
    return {
        "sender": row["sender"],
        "control_id": row["control_id"],
        "obr_index": row["obr_index"],
        "obx_index": row["obx_index"],
        "obr_seg": row["obr_seg"],
        "obx_seg": row["obx_seg"],
    }


def _coord_key(coord: dict[str, object]) -> tuple:
    return (
        str(coord["sender"]),
        str(coord["control_id"]),
        int(coord["obr_index"]),
        int(coord["obx_index"]),
        int(coord["obr_seg"]),
        int(coord["obx_seg"]),
    )


# ---------------------------------------------------------------------------
# Numeric parsing (shape-only; original value_text is never overwritten)
# ---------------------------------------------------------------------------

def try_parse_number(text: str) -> float | None:
    """Parse an OBX-5 numeric candidate.

    Accepts standard decimal notation with optional sign and exponent, and
    the HL7-style leading-plus indicator.  Returns None for anything that
    is not a finite number (comparators like ``<0.5``, ranges, empty text).
    """
    if not isinstance(text, str) or text == "":
        return None
    candidate = text.strip()
    if candidate in ("", "+", "-", ".", "+.", "-."):
        return None
    try:
        value = float(candidate)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


# ---------------------------------------------------------------------------
# Quantiles: deterministic R-7 (the numpy/Excel default), linear interpolation
# ---------------------------------------------------------------------------

def quantile_r7(sorted_values: array.array, prob: float) -> float:
    n = len(sorted_values)
    if n == 1:
        return float(sorted_values[0])
    h = (n - 1) * prob
    lo = math.floor(h)
    frac = h - lo
    hi = min(lo + 1, n - 1)
    return float(sorted_values[lo]) + frac * (
        float(sorted_values[hi]) - float(sorted_values[lo])
    )


def round_finite(value: float, digits: int = 6) -> float:
    """Stable rounding that keeps -0.0 from leaking into JSON."""
    out = round(value, digits)
    if out == 0:
        out = 0.0
    return out


# ---------------------------------------------------------------------------
# Bounded evidence sample bookkeeping
# ---------------------------------------------------------------------------

@dataclass
class SampleBook:
    """Keeps at most ``cap`` evidence samples in fixed coordinate order."""

    cap: int
    items: list[dict[str, object]] = field(default_factory=list)
    total_seen: int = 0

    def add(self, coord: dict[str, object], detail: dict[str, object]) -> None:
        self.total_seen += 1
        if len(self.items) >= self.cap:
            return
        entry = dict(coord)
        # deterministic key order within each sample entry
        for key in sorted(detail):
            entry[key] = detail[key]
        self.items.append(entry)

    def finalize(self) -> dict[str, object]:
        items = sorted(self.items, key=lambda e: _coord_key(e))
        return {
            "sample_cap": self.cap,
            "candidate_count": self.total_seen,
            "returned_count": len(items),
            "truncated": self.total_seen > len(items),
            "items": items,
        }


# ---------------------------------------------------------------------------
# Accumulators
# ---------------------------------------------------------------------------

class _BaseAccumulator:
    metric_type: str = ""

    def __init__(self, spec: MetricSpec, sample_cap: int,
                 buckets: tuple[int, ...]) -> None:
        self.spec = spec
        self.sample_cap = sample_cap
        self.buckets = buckets
        # window-level counters always present
        self.messages_seen = 0
        self._seen_message_keys: set[tuple[str, str]] = set()

    def note_message(self, row) -> None:
        key = (row["sender"], row["control_id"])
        if key not in self._seen_message_keys:
            self._seen_message_keys.add(key)
            self.messages_seen += 1

    def add_row(self, row) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def value(self) -> dict[str, object]:  # pragma: no cover - overridden
        raise NotImplementedError


class CountAccumulator(_BaseAccumulator):
    """message_count / result_count."""

    def __init__(self, spec, sample_cap, buckets) -> None:
        super().__init__(spec, sample_cap, buckets)
        self.metric_type = spec.metric_type
        self.result_rows = 0
        self.samples = SampleBook(sample_cap)

    def add_row(self, row) -> None:
        self.result_rows += 1
        self.samples.add(source_coord(row), {
            "test_code": row["test_code"],
            "value_type": row["value_type"],
        })

    def value(self) -> dict[str, object]:
        if self.metric_type == METRIC_MESSAGE_COUNT:
            count = self.messages_seen
        else:
            count = self.result_rows
        return {
            "count": count,
            "eligible_messages": self.messages_seen,
            "eligible_result_rows": self.result_rows,
            "excluded_rows": 0,
            "samples": self.samples.finalize(),
        }


class NonNullAccumulator(_BaseAccumulator):
    """Non-null (and non-empty) rate for one result-fact column."""

    def __init__(self, spec, sample_cap, buckets) -> None:
        super().__init__(spec, sample_cap, buckets)
        self.column = spec.field
        assert self.column in NONNULL_FIELDS
        self.eligible = 0
        self.nonnull = 0
        self.missing_samples = SampleBook(sample_cap)

    def add_row(self, row) -> None:
        self.eligible += 1
        cell = row[self.column]
        if cell is not None and cell != "":
            self.nonnull += 1
        else:
            self.missing_samples.add(source_coord(row), {
                "field": self.column,
                "stored_value": None,
                "reason": "NULL" if cell is None else "empty string",
            })

    def value(self) -> dict[str, object]:
        out: dict[str, object] = {
            "field": self.column,
            "eligible_rows": self.eligible,
            "nonnull_rows": self.nonnull,
            "null_or_empty_rows": self.eligible - self.nonnull,
            "excluded_rows": 0,
            "unparseable_rows": 0,
            "missing_samples": self.missing_samples.finalize(),
        }
        if self.eligible > 0:
            out["nonnull_rate"] = round_finite(self.nonnull / self.eligible)
        return out


class CardinalityAccumulator(_BaseAccumulator):
    """Distinct-value cardinality of sender / test_code / OBX-2 type."""

    def __init__(self, spec, sample_cap, buckets) -> None:
        super().__init__(spec, sample_cap, buckets)
        self.dimension = spec.dimension
        self.eligible = 0
        self.values: set[str] = set()
        self.per_value: dict[str, int] = {}
        self.coord_by_value: dict[str, dict[str, object]] = {}

    def _key_of(self, row) -> str:
        if self.dimension == CARD_SENDER:
            return str(row["sender"])
        if self.dimension == CARD_TEST_CODE:
            return str(row["test_code"])
        if self.dimension == CARD_OBX2_TYPE:
            return str(row["value_type"])
        raise AssertionError(self.dimension)

    def add_row(self, row) -> None:
        self.eligible += 1
        key = self._key_of(row)
        if key not in self.values:
            self.values.add(key)
            self.coord_by_value[key] = source_coord(row)
        self.per_value[key] = self.per_value.get(key, 0) + 1

    def value(self) -> dict[str, object]:
        detail = [
            {
                "value": value,
                "row_count": self.per_value[value],
                "first_seen": self.coord_by_value[value],
            }
            for value in sorted(self.values)
        ]
        return {
            "dimension": self.dimension,
            "eligible_rows": self.eligible,
            "excluded_rows": 0,
            "unparseable_rows": 0,
            "cardinality": len(self.values),
            "values": detail,
        }


class TextLengthAccumulator(_BaseAccumulator):
    """Value-length distribution for text-like OBX-2 types only."""

    def __init__(self, spec, sample_cap, buckets) -> None:
        super().__init__(spec, sample_cap, buckets)
        self.eligible_text_rows = 0
        self.excluded_rows = 0
        self.total_len = 0
        self.min_len: int | None = None
        self.max_len = 0
        self.bin_counts = [0 for _ in buckets]
        self.over_bucket_top = 0
        self.long_samples = SampleBook(sample_cap)

    def add_row(self, row) -> None:
        if row["value_type"] not in TEXTUAL_OBX2_TYPES:
            self.excluded_rows += 1
            return
        text = row["value_text"] if row["value_text"] is not None else ""
        length = len(text)
        self.eligible_text_rows += 1
        self.total_len += length
        if self.min_len is None or length < self.min_len:
            self.min_len = length
        if length > self.max_len:
            self.max_len = length
        placed = False
        for i, edge in enumerate(self.buckets):
            if length <= edge:
                self.bin_counts[i] += 1
                placed = True
                break
        if not placed:
            self.over_bucket_top += 1
        if length > self.buckets[-1]:
            self.long_samples.add(source_coord(row), {
                "value_type": row["value_type"],
                "length": length,
            })

    def value(self) -> dict[str, object]:
        out: dict[str, object] = {
            "textual_obx2_types": sorted(TEXTUAL_OBX2_TYPES),
            "eligible_rows": self.eligible_text_rows,
            "excluded_rows": self.excluded_rows,
            "unparseable_rows": 0,
            "exclusion_reason": "OBX-2 type is not one of the textual types",
            # evidence block is always well-formed, even with zero candidates
            "long_value_samples": self.long_samples.finalize(),
        }
        if self.eligible_text_rows == 0:
            return out
        histogram = []
        lower = 0
        for edge, count in zip(self.buckets, self.bin_counts):
            histogram.append({
                "range": f"({lower}, {edge}]",
                "lower_exclusive": lower,
                "upper_inclusive": edge,
                "count": count,
            })
            lower = edge
        histogram.append({
            "range": f"({lower}, inf)",
            "lower_exclusive": lower,
            "upper_inclusive": None,
            "count": self.over_bucket_top,
        })
        out.update({
            "min_length": self.min_len,
            "max_length": self.max_len,
            "mean_length": round_finite(self.total_len / self.eligible_text_rows),
            "histogram": histogram,
            "long_value_samples": self.long_samples.finalize(),
        })
        return out


class NumericQuantileAccumulator(_BaseAccumulator):
    """Parseable-numeric quantile distribution (NM values only).

    The original ``value_text`` is always retained; parsing failures are
    counted, never coerced, and numeric outputs describe distribution shape
    only -- they are not lab interpretations.
    """

    def __init__(self, spec, sample_cap, buckets) -> None:
        super().__init__(spec, sample_cap, buckets)
        self.quantiles = list(spec.quantiles)
        self.eligible_nm_rows = 0
        self.excluded_rows = 0
        self.parseable = array.array("d")
        self.unparseable = 0
        self.unparseable_samples = SampleBook(sample_cap)
        self.quantile_samples: dict[float, SampleBook] = {
            p: SampleBook(sample_cap) for p in self.quantiles
        }

    def add_row(self, row) -> None:
        if row["value_type"] not in NUMERIC_OBX2_TYPES:
            self.excluded_rows += 1
            return
        self.eligible_nm_rows += 1
        parsed = try_parse_number(row["value_text"])
        if parsed is None:
            self.unparseable += 1
            self.unparseable_samples.add(source_coord(row), {
                "value_type": row["value_type"],
                "stored_value_text": row["value_text"],
                "reason": "not a parseable finite number",
            })
            return
        self.parseable.append(parsed)

    def value(self) -> dict[str, object]:
        out: dict[str, object] = {
            "numeric_obx2_types": sorted(NUMERIC_OBX2_TYPES),
            "eligible_rows": self.eligible_nm_rows,
            "excluded_rows": self.excluded_rows,
            "exclusion_reason": "OBX-2 type is not one of the numeric types",
            "parseable_rows": len(self.parseable),
            "unparseable_rows": self.unparseable,
        }
        if self.eligible_nm_rows > 0:
            out["unparseable_fraction"] = round_finite(
                self.unparseable / self.eligible_nm_rows
            )
        if len(self.parseable) == 0:
            out["quantiles"] = []
            out["unparseable_samples"] = self.unparseable_samples.finalize()
            return out
        ordered = array.array("d", sorted(self.parseable))
        q_out = []
        for prob in self.quantiles:
            q_out.append({
                "probability": round_finite(prob, 6),
                "value": round_finite(quantile_r7(ordered, prob)),
            })
        out.update({
            "quantiles": q_out,
            "min": round_finite(ordered[0]),
            "max": round_finite(ordered[-1]),
            "mean": round_finite(sum(ordered) / len(ordered)),
            "unparseable_samples": self.unparseable_samples.finalize(),
        })
        return out


_ACCUMULATORS = {
    METRIC_MESSAGE_COUNT: CountAccumulator,
    METRIC_RESULT_COUNT: CountAccumulator,
    METRIC_FIELD_NONNULL: NonNullAccumulator,
    METRIC_CARDINALITY: CardinalityAccumulator,
    METRIC_TEXT_LENGTH: TextLengthAccumulator,
    METRIC_NUMERIC_QUANTILES: NumericQuantileAccumulator,
}


def build_accumulators(
    specs: tuple[MetricSpec, ...], sample_cap: int,
    buckets: tuple[int, ...],
) -> dict[str, _BaseAccumulator]:
    return {
        spec.metric_id: _ACCUMULATORS[spec.metric_type](
            spec, sample_cap, buckets
        )
        for spec in specs
    }
