"""Wire protocol for result-subscription pages (docs/SYNC.md §3).

A page is a single canonical-JSON object.  Canonical means: UTF-8,
``sort_keys=True``, ``separators=(",", ":")``, ``ensure_ascii=False`` --
for the same logical content the bytes are always identical, which is
what makes same-parameter re-fetches byte-identical across restarts,
concurrent receiver writes and ``VACUUM``.

Integrity: ``digest`` is the SHA-256 hex of the canonical bytes of the
page object *without* the ``digest`` key.  It covers the header fields
and the whole payload, so a truncated tail (invalid JSON) and any
in-place corruption (digest mismatch) are both detected.  This protects
against accidental corruption only; it is not a cryptographic trust
system.

Subscription identity: the fixed sender set is normalised (strip,
reject empties, deduplicate, sort) and hashed, so equivalent spellings
of the same subscription map to one identity and different
subscriptions can never share cursors or pages.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

PROTOCOL = "hl7recv-sync/1"

# Fields every page header must carry (payload lives under "messages").
HEADER_KEYS = (
    "protocol",
    "source_id",
    "subscription_id",
    "bound",
    "start",
    "end",
    "budget",
    "terminal",
    "messages",
    "digest",
)


class PageError(ValueError):
    """Raised when a page file is malformed, corrupt or inconsistent."""


def canonical_bytes(obj: Any) -> bytes:
    """Serialise *obj* to the deterministic canonical byte form."""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def normalize_subscription(senders: Iterable[str]) -> list[str]:
    """Normalise the fixed sender set: strip, non-empty, dedupe, sort."""
    cleaned = []
    for raw in senders:
        s = str(raw).strip()
        if not s:
            raise ValueError("subscription senders must be non-empty")
        cleaned.append(s)
    unique = sorted(set(cleaned))
    if not unique:
        raise ValueError("subscription must name at least one sender")
    return unique


def subscription_id(senders: Iterable[str]) -> str:
    """Stable identity of a normalised subscription."""
    canon = canonical_bytes(normalize_subscription(senders))
    return "sub1:" + hashlib.sha256(canon).hexdigest()


def build_page(
    *,
    source_id: str,
    subscription: list[str],
    bound: int,
    start: int,
    end: int,
    budget: int,
    terminal: bool,
    messages: list[dict],
) -> bytes:
    """Assemble a page object and return its canonical file bytes."""
    page = {
        "protocol": PROTOCOL,
        "source_id": source_id,
        "subscription_id": subscription_id(subscription),
        "bound": bound,
        "start": start,
        "end": end,
        "budget": budget,
        "terminal": 1 if terminal else 0,
        "messages": messages,
    }
    page["digest"] = hashlib.sha256(canonical_bytes(page)).hexdigest()
    return canonical_bytes(page)


def _check_int(page: dict, key: str, lo: int) -> int:
    val = page[key]
    if not isinstance(val, int) or isinstance(val, bool) or val < lo:
        raise PageError(f"page field {key!r} must be an integer >= {lo}")
    return val


def _check_result_row(row: Any) -> dict:
    if not isinstance(row, dict):
        raise PageError("result row must be an object")
    out = {}
    for key in ("obr_index", "obx_index"):
        val = row.get(key)
        if not isinstance(val, int) or isinstance(val, bool) or val < 1:
            raise PageError(f"result field {key!r} must be an integer >= 1")
        out[key] = val
    for key in ("obs_system", "obs_code", "value_type", "value_text"):
        val = row.get(key)
        if not isinstance(val, str):
            raise PageError(f"result field {key!r} must be a string")
        out[key] = val
    for key in ("sub_id", "units"):
        val = row.get(key)
        if val is not None and not isinstance(val, str):
            raise PageError(f"result field {key!r} must be a string or null")
        out[key] = val
    unknown = set(row) - set(out)
    if unknown:
        raise PageError(f"result row has unknown fields: {sorted(unknown)}")
    return out


def _check_message(msg: Any) -> dict:
    if not isinstance(msg, dict):
        raise PageError("message entry must be an object")
    pos = msg.get("position")
    if not isinstance(pos, int) or isinstance(pos, bool) or pos < 1:
        raise PageError("message position must be an integer >= 1")
    out = {"position": pos}
    for key in ("sender", "control_id", "received_at"):
        val = msg.get(key)
        if not isinstance(val, str):
            raise PageError(f"message field {key!r} must be a string")
        out[key] = val
    results = msg.get("results")
    if not isinstance(results, list):
        raise PageError("message results must be a list")
    out["results"] = [_check_result_row(r) for r in results]
    unknown = set(msg) - set(out)
    if unknown:
        raise PageError(f"message entry has unknown fields: {sorted(unknown)}")
    return out


def parse_page(data: bytes) -> dict:
    """Parse and fully validate a page file; raise PageError on any defect.

    Returns a normalised page dict (only known fields, checked types).
    """
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PageError(f"page is not valid UTF-8: {exc}") from exc
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        # covers a lost tail and any structural corruption
        raise PageError(f"page is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise PageError("page must be a JSON object")
    unknown = set(obj) - set(HEADER_KEYS)
    if unknown:
        raise PageError(f"page has unknown fields: {sorted(unknown)}")
    missing = set(HEADER_KEYS) - set(obj)
    if missing:
        raise PageError(f"page is missing fields: {sorted(missing)}")

    digest = obj["digest"]
    if not isinstance(digest, str) or len(digest) != 64:
        raise PageError("page digest must be a SHA-256 hex string")
    body = {k: v for k, v in obj.items() if k != "digest"}
    actual = hashlib.sha256(canonical_bytes(body)).hexdigest()
    if actual != digest:
        raise PageError("page digest mismatch (content corrupted)")

    if obj["protocol"] != PROTOCOL:
        raise PageError(f"unsupported protocol {obj['protocol']!r}")
    for key in ("source_id", "subscription_id"):
        if not isinstance(obj[key], str) or not obj[key]:
            raise PageError(f"page field {key!r} must be a non-empty string")
    page = {
        "protocol": obj["protocol"],
        "source_id": obj["source_id"],
        "subscription_id": obj["subscription_id"],
        "digest": digest,
        "bound": _check_int(obj, "bound", 0),
        "start": _check_int(obj, "start", 0),
        "end": _check_int(obj, "end", 0),
        "budget": _check_int(obj, "budget", 1),
    }
    terminal = obj["terminal"]
    if terminal not in (0, 1):
        raise PageError("page field 'terminal' must be 0 or 1")
    page["terminal"] = terminal
    if not (page["start"] <= page["end"] <= page["bound"]):
        raise PageError("page positions must satisfy start <= end <= bound")
    if page["terminal"] and page["end"] != page["bound"]:
        raise PageError("terminal page must end exactly at its bound")
    messages = obj["messages"]
    if not isinstance(messages, list):
        raise PageError("page messages must be a list")
    page["messages"] = [_check_message(m) for m in messages]
    # payload positions must be strictly increasing and inside (start, end]
    last = page["start"]
    for msg in page["messages"]:
        if not (last < msg["position"] <= page["end"]):
            raise PageError("message positions must lie in (start, end] in order")
        last = msg["position"]
    return page
