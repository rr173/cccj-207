"""Time handling: compare actual instants, never original strings.

All timestamps crossing the ledger boundary must carry an explicit offset
(ISO-8601, e.g. ``2026-11-20T08:00:00+08:00`` or ``...Z``).  Naive strings
are rejected so wall-clock text can never be compared lexicographically.

Internally every instant is an integer number of microseconds since the
Unix epoch.  Using integers makes half-open window tests ``[from, to)``
exact and keeps SQLite comparisons index-friendly.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .errors import ManifestError


def parse_instant(text: str, *, field: str = "timestamp") -> int:
    """Parse an explicit-offset ISO-8601 string into epoch microseconds.

    ``Z`` is accepted as UTC.  Naive datetimes raise :class:`ManifestError`.
    """
    if not isinstance(text, str) or not text:
        raise ManifestError(f"{field}: expected non-empty ISO-8601 timestamp")
    raw = text
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ManifestError(f"{field}: unparseable timestamp {raw!r}: {exc}") from exc
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ManifestError(
            f"{field}: timestamp {raw!r} lacks an explicit timezone offset"
        )
    # Normalise through UTC so two different wall-clock representations of
    # the same instant compare equal.
    dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp()) * 1_000_000 + dt.microsecond


def instant_to_iso(epoch_us: int) -> str:
    """Render epoch microseconds as a canonical UTC ISO-8601 string."""
    secs, micros = divmod(int(epoch_us), 1_000_000)
    dt = datetime.fromtimestamp(secs, tz=timezone.utc).replace(microsecond=micros)
    return dt.isoformat().replace("+00:00", "Z")


def iso_for_db(epoch_us: int) -> str:
    """Canonical textual form stored alongside the integer (for readability)."""
    return instant_to_iso(epoch_us)
