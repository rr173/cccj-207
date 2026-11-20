"""Policy parsing and strict validation tests.

Every malformed strategy must be rejected *before* any database work, so an
invalid policy can never masquerade as a clean (zero-anomaly) report.
"""

from __future__ import annotations

import copy
import json
import unittest

from hl7recv.audit.policy import (
    AuditError,
    METRIC_CARDINALITY,
    METRIC_FIELD_NONNULL,
    METRIC_MESSAGE_COUNT,
    METRIC_NUMERIC_QUANTILES,
    METRIC_TEXT_LENGTH,
    parse_policy_text,
)


def _base_doc() -> dict:
    return {
        "policy_version": 1,
        "name": "t",
        "selection": {"senders": ["A^B"]},
        "windows": {
            "baseline": {"start": "2026-01-01T00:00:00+00:00",
                         "end": "2026-01-02T00:00:00+00:00"},
            "current": {"start": "2026-01-02T00:00:00+00:00",
                        "end": "2026-01-03T00:00:00+00:00"},
        },
        "metrics": [
            {"metric_id": "m1", "type": METRIC_MESSAGE_COUNT, "rules": []}
        ],
    }


def _expect_error(doc: dict, fragment: str) -> None:
    with unittest.TestCase().assertRaises(AuditError) as ctx:
        parse_policy_text(json.dumps(doc))
    assert fragment in str(ctx.exception), str(ctx.exception)


class TestPolicyValidation(unittest.TestCase):
    def test_valid_minimal_policy_round_trips(self):
        pol = parse_policy_text(json.dumps(_base_doc()))
        self.assertEqual(pol.senders, ("A^B",))
        self.assertIsNone(pol.test_codes)
        self.assertEqual(pol.metrics[0].metric_id, "m1")
        self.assertEqual(pol.max_samples, 20)  # documented default

    def test_not_json(self):
        _expect_error_raw("not json{", "not valid JSON")

    def test_wrong_version(self):
        doc = _base_doc()
        doc["policy_version"] = 2
        _expect_error(doc, "unsupported policy_version")

    def test_senders_required_and_unique(self):
        doc = _base_doc()
        doc["selection"]["senders"] = []
        _expect_error(doc, "senders must be a non-empty list")
        doc["selection"]["senders"] = ["A^B", "A^B"]
        _expect_error(doc, "duplicates")

    def test_test_codes_optional_but_non_empty(self):
        doc = _base_doc()
        doc["selection"]["test_codes"] = []
        _expect_error(doc, "test_codes")
        doc["selection"]["test_codes"] = ["GLU"]
        pol = parse_policy_text(json.dumps(doc))
        self.assertEqual(pol.test_codes, ("GLU",))

    def test_windows_all_or_nothing(self):
        doc = _base_doc()
        del doc["windows"]["current"]
        _expect_error(doc, "both baseline and current")

    def test_metric_id_required_unique(self):
        doc = _base_doc()
        doc["metrics"] = [
            {"metric_id": "x", "type": METRIC_MESSAGE_COUNT},
            {"metric_id": "x", "type": METRIC_MESSAGE_COUNT},
        ]
        _expect_error(doc, "duplicate metric_id")
        doc["metrics"] = [{"type": METRIC_MESSAGE_COUNT}]
        _expect_error(doc, "metric_id")

    def test_unknown_metric_type(self):
        doc = _base_doc()
        doc["metrics"] = [{"metric_id": "x", "type": "mystery"}]
        _expect_error(doc, "unknown type")

    def test_nonnull_field_must_name_real_column(self):
        doc = _base_doc()
        doc["metrics"] = [
            {"metric_id": "f", "type": METRIC_FIELD_NONNULL, "field": "nope"}
        ]
        _expect_error(doc, "result-fact column")

    def test_cardinality_dimension_enforced(self):
        doc = _base_doc()
        doc["metrics"] = [
            {"metric_id": "c", "type": METRIC_CARDINALITY, "dimension": "nope"}
        ]
        _expect_error(doc, "dimension must be one of")

    def test_quantile_bounds_and_uniqueness(self):
        doc = _base_doc()
        doc["metrics"] = [{
            "metric_id": "q", "type": METRIC_NUMERIC_QUANTILES,
            "quantiles": [0.5, 1.5],
        }]
        _expect_error(doc, "quantile probabilities")
        doc["metrics"] = [{
            "metric_id": "q", "type": METRIC_NUMERIC_QUANTILES,
            "quantiles": [0.5, 0.5],
        }]
        _expect_error(doc, "duplicate quantile")

    def test_rule_must_fit_metric_type(self):
        doc = _base_doc()
        doc["metrics"] = [{
            "metric_id": "m1", "type": METRIC_MESSAGE_COUNT,
            "rules": [{"rule": "nonnull_rate_below", "severity": "warning",
                       "parameter": 0.5}],
        }]
        _expect_error(doc, "not valid for metric type")

    def test_rule_severity_enforced(self):
        doc = _base_doc()
        doc["metrics"] = [{
            "metric_id": "m1", "type": METRIC_MESSAGE_COUNT,
            "rules": [{"rule": "count_drop_gte", "severity": "catastrophic",
                       "parameter": 1}],
        }]
        _expect_error(doc, "severity must be one of")

    def test_rule_parameter_bounds(self):
        doc = _base_doc()
        doc["metrics"] = [{
            "metric_id": "f", "type": METRIC_FIELD_NONNULL, "field": "units",
            "rules": [{"rule": "nonnull_rate_below", "severity": "warning",
                       "parameter": 1.5}],
        }]
        _expect_error(doc, "fraction parameter")

    def test_duplicate_rule_rejected(self):
        doc = _base_doc()
        doc["metrics"] = [{
            "metric_id": "m1", "type": METRIC_MESSAGE_COUNT,
            "rules": [
                {"rule": "count_drop_gte", "severity": "warning",
                 "parameter": 1},
                {"rule": "count_drop_gte", "severity": "failure",
                 "parameter": 2},
            ],
        }]
        _expect_error(doc, "duplicate rule")

    def test_bucket_edges_strictly_increasing(self):
        doc = _base_doc()
        doc["limits"] = {"text_length_buckets": [4, 4]}
        _expect_error(doc, "strictly increasing")

    def test_sample_cap_positive(self):
        doc = _base_doc()
        doc["limits"] = {"max_samples_per_metric": 0}
        _expect_error(doc, "max_samples_per_metric")

    def test_text_metric_defaults_quantile_metric(self):
        doc = _base_doc()
        doc["metrics"] = [
            {"metric_id": "t", "type": METRIC_TEXT_LENGTH, "rules": []},
            {"metric_id": "q", "type": METRIC_NUMERIC_QUANTILES},
        ]
        pol = parse_policy_text(json.dumps(doc))
        self.assertEqual(pol.metrics[1].quantiles, (0.5, 0.9))

    def test_deep_copy_independence(self):
        # policies are frozen value objects; re-parsing must be independent
        first = parse_policy_text(json.dumps(_base_doc()))
        doc = _base_doc()
        doc["name"] = "other"
        second = parse_policy_text(json.dumps(doc))
        self.assertNotEqual(first.name, second.name)
        self.assertEqual(copy.deepcopy(first).senders, first.senders)


def _expect_error_raw(text: str, fragment: str) -> None:
    with unittest.TestCase().assertRaises(AuditError) as ctx:
        parse_policy_text(text)
    assert fragment in str(ctx.exception)


if __name__ == "__main__":
    unittest.main()
