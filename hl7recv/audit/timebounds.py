"""Window bound parsing and canonical comparison semantics.

The receiver writes ``messages.received_at`` as millisecond-precision UTC
ISO-8601 strings (``YYYY-MM-DDTHH:MM:SS.mmm+00:00``).  Window bounds supplied
by the caller are parsed as ISO-8601 instants, then canonicalised to the same
millisecond UTC form so the half-open ``[start, end)`` predicate is a plain
string comparison executed in SQL (lexicographic order of these canonical
strings equals chronological order).  The caller's original bound text is
preserved in the report alongside the canonical form.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .policy import AuditError, Window


def canonical_bound(text: str, *, which: str) -> str:
    """Convert an ISO-8601 bound into millisecond UTC canonical text."""
    parsed = _parse_iso(text, which=which)
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _parse_iso(text: str, *, which: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise AuditError(
            f"window bound {which}={text!r} is not a valid ISO-8601 instant"
        ) from exc
    if parsed.tzinfo is None:
        raise AuditError(
            f"window bound {which}={text!r} must carry an explicit UTC offset "
            "(e.g. 'Z' or '+00:00'); naive timestamps are rejected so window "
            "semantics cannot depend on the host's local zone"
        )
    return parsed


def resolve_window(window: Window) -> dict[str, str]:
    """Return the report block describing a resolved half-open window."""
    start_c = canonical_bound(window.start, which="start")
    end_c = canonical_bound(window.end, which="end")
    if not start_c < end_c:
        raise AuditError(
            f"window must satisfy start < end; got start={window.start!r} "
            f"end={window.end!r}"
        )
    return {
        "start": window.start,
        "end": window.end,
        "start_canonical": start_c,
        "end_canonical": end_c,
        "inclusivity": "[start, end)",
        "time_basis": "messages.received_at (UTC, millisecond precision)",
    }


def resolve_windows(
    baseline: Window, current: Window
) -> tuple[dict[str, str], dict[str, str]]:
    base = resolve_window(baseline)
    cur = resolve_window(current)
    # Overlapping windows are legal (a caller may re-run with moving
    # windows); disjoint or reversed ordering is also legal.  We only need
    # each individual window to be well-formed.
    return base, cur
