"""Handover manifest loading and validation (streaming).

A manifest is a directory of newline-delimited JSON files::

    manifest.json      meta document, exactly one JSON object
    work_orders.jsonl  one object per work order
    items.jsonl        one object per receivable item
    mappings.jsonl     one object per external order-number mapping

JSONL keeps large manifests (up to hundreds of thousands of items)
streamable: a line is validated as soon as it is read and the caller
persists it immediately; the full manifest is never materialised in memory.

Validation rules (a single failure rejects the whole manifest and no ledger
is created):

* every timestamp is explicit-offset ISO-8601 and compared as an instant;
* item ids are globally stable and unique; work order ids are unique;
* every item and mapping references a known work order / participating sender;
* mapping windows are half-open ``[valid_from, valid_to)``;
* a null ``sub_id`` is an explicit condition, never a wildcard -- it only
  matches observations whose sub id is also null;
* the cut-off (if supplied at registration) must be strictly later than the
  shift start.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterator

from .errors import ManifestError
from .timeutil import parse_instant

MANIFEST_FILE = "manifest.json"
WORK_ORDERS_FILE = "work_orders.jsonl"
ITEMS_FILE = "items.jsonl"
MAPPINGS_FILE = "mappings.jsonl"


@dataclass(frozen=True)
class ManifestMeta:
    shift_id: str
    shift_start: int
    cutoff: int | None
    senders: tuple[str, ...]

    @property
    def sender_set(self) -> frozenset[str]:
        return frozenset(self.senders)


def _read_json_objects(path: str) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ManifestError(
                    f"{os.path.basename(path)}:{lineno}: invalid JSON: {exc.msg}"
                ) from exc
            if not isinstance(obj, dict):
                raise ManifestError(
                    f"{os.path.basename(path)}:{lineno}: expected a JSON object"
                )
            obj["__lineno__"] = lineno
            yield obj


def _require_str(obj: dict, key: str, fname: str) -> str:
    val = obj.get(key)
    if not isinstance(val, str) or not val:
        raise ManifestError(
            f"{fname}:{obj.get('__lineno__')}: {key!r} must be a non-empty string"
        )
    return val


def load_meta(manifest_dir: str) -> ManifestMeta:
    path = os.path.join(manifest_dir, MANIFEST_FILE)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError as exc:
        raise ManifestError(f"manifest not found: {path!r}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{MANIFEST_FILE}: invalid JSON: {exc.msg}") from exc
    if not isinstance(doc, dict):
        raise ManifestError(f"{MANIFEST_FILE}: expected a JSON object")

    shift_id = _require_str(doc, "shift_id", MANIFEST_FILE)
    shift_start = parse_instant(
        _require_str(doc, "shift_start", MANIFEST_FILE), field="shift_start"
    )
    cutoff = None
    if doc.get("cutoff") is not None:
        cutoff = parse_instant(doc["cutoff"], field="cutoff")
        if cutoff <= shift_start:
            raise ManifestError("cutoff must be strictly later than shift_start")
    senders = doc.get("senders")
    if not isinstance(senders, list) or not senders or not all(
        isinstance(s, str) and s for s in senders
    ):
        raise ManifestError("senders must be a non-empty list of non-empty strings")
    if len(set(senders)) != len(senders):
        raise ManifestError("senders must not contain duplicates")
    return ManifestMeta(shift_id, shift_start, cutoff, tuple(senders))


def iter_work_orders(manifest_dir: str) -> Iterator[dict]:
    """Yield validated work order rows: (work_order_id, patient_domain, patient_id)."""
    path = os.path.join(manifest_dir, WORK_ORDERS_FILE)
    if not os.path.exists(path):
        raise ManifestError(f"missing required file {WORK_ORDERS_FILE}")
    for obj in _read_json_objects(path):
        wid = _require_str(obj, "work_order_id", WORK_ORDERS_FILE)
        domain = _require_str(obj, "patient_domain", WORK_ORDERS_FILE)
        pid = _require_str(obj, "patient_id", WORK_ORDERS_FILE)
        yield {
            "work_order_id": wid,
            "patient_domain": domain,
            "patient_id": pid,
        }


def iter_items(manifest_dir: str) -> Iterator[dict]:
    """Yield validated receivable item rows with times as epoch microseconds."""
    path = os.path.join(manifest_dir, ITEMS_FILE)
    if not os.path.exists(path):
        raise ManifestError(f"missing required file {ITEMS_FILE}")
    for obj in _read_json_objects(path):
        item_id = _require_str(obj, "item_id", ITEMS_FILE)
        wid = _require_str(obj, "work_order_id", ITEMS_FILE)
        code_system = _require_str(obj, "code_system", ITEMS_FILE)
        code = _require_str(obj, "code", ITEMS_FILE)
        # sub_id: null is an explicit condition; a present value must be a string.
        sub_id = obj.get("sub_id", None)
        if sub_id is not None and not isinstance(sub_id, str):
            raise ManifestError(
                f"{ITEMS_FILE}:{obj['__lineno__']}: sub_id must be a string or null"
            )
        due = parse_instant(
            _require_str(obj, "due_at", ITEMS_FILE), field="due_at"
        )
        yield {
            "item_id": item_id,
            "work_order_id": wid,
            "code_system": code_system,
            "code": code,
            "sub_id": sub_id,
            "due_at": due,
        }


def iter_mappings(manifest_dir: str) -> Iterator[dict]:
    """Yield validated external order-number mapping rows."""
    path = os.path.join(manifest_dir, MAPPINGS_FILE)
    if not os.path.exists(path):
        raise ManifestError(f"missing required file {MAPPINGS_FILE}")
    for obj in _read_json_objects(path):
        sender = _require_str(obj, "sender", MAPPINGS_FILE)
        number_class = _require_str(obj, "number_class", MAPPINGS_FILE)
        local_number = _require_str(obj, "local_number", MAPPINGS_FILE)
        wid = _require_str(obj, "work_order_id", MAPPINGS_FILE)
        valid_from = parse_instant(
            _require_str(obj, "valid_from", MAPPINGS_FILE), field="valid_from"
        )
        valid_to = parse_instant(
            _require_str(obj, "valid_to", MAPPINGS_FILE), field="valid_to"
        )
        if valid_to <= valid_from:
            raise ManifestError(
                f"{MAPPINGS_FILE}:{obj['__lineno__']}: "
                "valid_to must be strictly later than valid_from"
            )
        yield {
            "sender": sender,
            "number_class": number_class,
            "local_number": local_number,
            "work_order_id": wid,
            "valid_from": valid_from,
            "valid_to": valid_to,
        }
