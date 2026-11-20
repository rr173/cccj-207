"""Query API over the fact store.

All queries return plain dicts with stable ordering so both the CLI and
external tools can rely on them.  Source coordinates (segment ordinals,
OBR/OBX indexes) are always included.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from .store import Store

RESULT_COLUMNS = (
    "obr_index", "obx_index", "patient_authority", "patient_id", "visit_id",
    "placer_order", "filler_order", "test_code", "test_system", "test_name",
    "obs_code", "obs_system", "obs_name", "sub_id", "value_type", "value_text",
    "units", "ref_range", "abnormal", "rstatus", "observed_at", "reported_at",
    "obr_seg", "obx_seg", "merged",
)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def list_messages(store: Store) -> list[dict[str, Any]]:
    return [_row_to_dict(r) for r in store.list_messages()]


def message_detail(store: Store, sender: str, control_id: str) -> dict[str, Any] | None:
    msg = store.get_message(sender, control_id)
    if msg is None:
        return None
    out = _row_to_dict(msg)
    out["raw_size"] = len(out.pop("raw"))
    out["results"] = [
        _row_to_dict(r) for r in store.results_for_message(sender, control_id)
    ]
    out["notes"] = [_row_to_dict(n) for n in store.notes_for_message(sender, control_id)]
    out["patients"] = [
        _row_to_dict(p) for p in store.patients_for_message(sender, control_id)
    ]
    return out


def results_by_message(store: Store, sender: str, control_id: str) -> list[dict[str, Any]]:
    return [_row_to_dict(r) for r in store.results_for_message(sender, control_id)]


def results_by_patient(store: Store, authority: str, identifier: str) -> list[dict[str, Any]]:
    return [_row_to_dict(r) for r in store.results_for_patient(authority, identifier)]


def results_by_order(store: Store, order: str) -> list[dict[str, Any]]:
    return [_row_to_dict(r) for r in store.results_for_order(order)]


def diagnostics(store: Store) -> dict[str, Any]:
    diag = store.diagnostics()
    inbox = []
    for row in diag["inbox"]:
        d = _row_to_dict(row)
        evidence = d.pop("evidence", None)
        d["evidence_size"] = len(evidence) if evidence is not None else 0
        inbox.append(d)
    return {
        "inbox": inbox,
        "frame_errors": [_row_to_dict(r) for r in diag["frame_errors"]],
    }
