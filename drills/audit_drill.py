"""Deterministic synthetic two-window dataset for audit drills.

Timeline (all instants UTC, millisecond canonical form)::

    2026-11-10T09:00:00.000   out-of-window message (before baseline)
    2026-11-10T10:00:00.000   baseline window starts  [inclusive)
    2026-11-10T10:00:00.000   B1 lands exactly on the start boundary -> in
    2026-11-10T10:30:00.000   B2
    2026-11-10T10:45:00.000   B3
    2026-11-10T11:00:00.000   baseline end / current start; Bx at 11:00
                              belongs to current only (half-open both sides)
    2026-11-10T11:15:00.000   C2
    2026-11-10T11:45:00.000   C3
    2026-11-10T12:00:00.000   current end; message at 12:00 is excluded
    2026-11-10T12:30:00.000   rejected message (inbox only, never a fact)

Rows are declared as explicit Python records so every metric can be
re-derived by hand in the tests and again, independently, in the
reference checker.
"""

from __future__ import annotations

from dataclasses import dataclass

from hl7recv.builder import build_message
from hl7recv.store import Store

# Canonical window instants.
BASELINE_START = "2026-11-10T10:00:00.000+00:00"
BASELINE_END = "2026-11-10T11:00:00.000+00:00"
CURRENT_START = "2026-11-10T11:00:00.000+00:00"
CURRENT_END = "2026-11-10T12:00:00.000+00:00"

SENDER_A = "LAB1^HOSP"
SENDER_B = "LAB2^HOSP"


@dataclass(frozen=True)
class DrillRow:
    """One intended result fact (OBX) inside a drill message."""

    value_type: str
    obs_code: str
    value_text: str
    units: str | None = None
    ref_range: str | None = None
    abnormal: str | None = None
    sub_id: str | None = None
    observed_at: str | None = None
    test_code: str = "CBC"
    test_system: str = "LN"
    test_name: str | None = None
    obs_name: str | None = None
    reported_at: str | None = None
    placer_order: str = "PL-1"
    filler_order: str = "FL-1"
    visit_id: str | None = "V1"
    rstatus: str = "F"


def _escape(text: str) -> str:
    return (
        text.replace("\\", "\\E\\").replace("|", "\\F\\")
        .replace("^", "\\S\\").replace("~", "\\R\\").replace("&", "\\T\\")
    )


def build_oru_message(
    sender: str,
    control_id: str,
    rows: list[DrillRow],
    *,
    version: str = "2.5.1",
) -> bytes:
    """Render drill rows into a minimal profile-valid ORU^R01 message."""
    app, fac = sender.split("^", 1)
    parts = [
        f"MSH|^~\\&|{app}|{fac}|RCV|RCVF|20261110100000||ORU^R01|{control_id}"
        f"|P|{version}\r",
        "PID|1||P1^^^HOSP^MR||Doe^J\r",
        "PV1|1|O\r",
    ]
    # group rows by (placer, filler, test code) into OBR groups
    groups: dict[tuple, list[DrillRow]] = {}
    order: list[tuple] = []
    for row in rows:
        key = (row.placer_order, row.filler_order, row.test_code,
               row.test_system, row.test_name)
        if key not in groups:
            groups[key] = []
            order.append(key)
    obr_index = 0
    for key in order:
        placer, filler, tcode, tsys, tname = key
        group_rows = [r for r in rows
                      if (r.placer_order, r.filler_order, r.test_code,
                          r.test_system, r.test_name) == key]
        obr_index += 1
        name = f"^{_escape(tname) if tname else ''}^"
        parts.append(
            f"OBR|{obr_index}|{placer}|{filler}|{tcode}{name}{tsys}"
            f"||||||||||||||||||||{group_rows[0].reported_at or ''}\r"
        )
        for i, row in enumerate(group_rows, start=1):
            obs = f"{row.obs_code}^{_escape(row.obs_name) if row.obs_name else ''}^LN"
            # Pipe tokens after the segment name: OBX positions 1..14.
            # 1 set-id, 2 value type, 3 obs id, 4 sub-id, 5 value, 6 units,
            # 7 ref range, 8 abnormal, 9 probability, 10 abnorm severity,
            # 11 status, 12 eff date/time, 13 defined access checks,
            # 14 observation datetime
            fields = [
                "OBX", str(i), row.value_type, obs, row.sub_id or "",
                _escape(row.value_text),
                row.units if row.units is not None else "",
                row.ref_range if row.ref_range is not None else "",
                row.abnormal if row.abnormal is not None else "",
                "", "", row.rstatus, "", "",
                row.observed_at or "",
            ]
            assert len(fields) == 15  # name + positions 1..14
            parts.append("|".join(fields) + "\r")
    return "".join(parts).encode("utf-8")


# ---------------------------------------------------------------------------
# The hand-designed row catalogue
# ---------------------------------------------------------------------------

def _baseline_rows() -> dict[str, list[DrillRow]]:
    return {
        # B1: sender A, exactly on the inclusive start boundary.
        # NM x3 (parseable), ST one long-ish text, one TX, one FT, CE/CWE.
        "B1": [
            DrillRow("NM", "WBC", "7.5", units="10*9/L", ref_range="4-10",
                     abnormal="N", observed_at="20261110100000"),
            DrillRow("NM", "RBC", "4.2", units="10*12/L",
                     observed_at="20261110100000"),
            DrillRow("NM", "HGB", "130", observed_at="20261110100000"),
            DrillRow("ST", "NOTE", "normal morphology",
                     observed_at="20261110100000"),
            DrillRow("TX", "MEMO", "short", observed_at="20261110100000"),
            DrillRow("FT", "FT1", "ok", observed_at="20261110100000"),
            DrillRow("CWE", "METH", "POCT^Point of Care^HL7OBS",
                     observed_at="20261110100000"),
        ],
        # B2: sender B; includes empty-string units (present-but-empty) and
        # a NULL ref_range/abnormal/observed_at on one row.
        "B2": [
            DrillRow("NM", "GLU", "5.5", units="", ref_range=None,
                     abnormal=None, observed_at=None),
            DrillRow("ST", "CMT", "adequate sample quality noted",
                     units=None, observed_at=None),
        ],
        # B3: sender A again; NULL units + NULL observed_at on the NM row.
        "B3": [
            DrillRow("NM", "PLT", "250", units=None, observed_at=None),
            DrillRow("CE", "FLAG", "A^Abnormal^HL7OBS", observed_at=None),
        ],
    }


def _current_rows() -> dict[str, list[DrillRow]]:
    return {
        # C1: sender A, exactly at 11:00:00 -> current window, NOT baseline.
        # Only two rows: deliberately small sample (low-sample guard drill).
        # One NM is unparseable text ("N/A") -> unparseable fraction.
        "C1": [
            DrillRow("NM", "WBC", "N/A", units="10*9/L",
                     observed_at="20261110110000"),
            DrillRow("ST", "NOTE", "x",
                     observed_at="20261110110000"),
        ],
        # C2: sender B; one very long ST value (length threshold breach),
        # NM values shifted vs baseline, an empty ST (all-empty column drill
        # for a field metric targeting value text is separate), NULL units.
        "C2": [
            DrillRow("NM", "GLU", "18.9", units=None, observed_at=None),
            DrillRow("NM", "LAC", "9.75", units=None, observed_at=None),
            DrillRow(
                "ST", "CMT",
                "x" * 200,  # 200-char text -> over default top bin
                units=None, observed_at=None,
            ),
            DrillRow("TX", "MEMO", "tiny", units=None, observed_at=None),
        ],
        # C3: sender A; unparseable NM again, FT disappeared (type
        # cardinality drop).  obs_name is never populated by the drill, so a
        # field metric on obs_name exercises the all-NULL-column case;
        # OBX-5 itself stays profile-valid (it is required at parse time).
        "C3": [
            DrillRow("NM", "WBC", "pending", units=None, observed_at=None),
            DrillRow("ST", "NOTE", "flagged for review",
                     units=None, observed_at=None),
            DrillRow("CWE", "METH", "MAN^Manual^HL7OBS",
                     units=None, observed_at=None),
        ],
    }


# (control_id, sender, received_at, version)
def _message_plan() -> list[tuple[str, str, str, str]]:
    return [
        ("OUT1", SENDER_A, "2026-11-10T09:00:00.000+00:00", "2.5.1"),
        ("B1", SENDER_A, BASELINE_START, "2.5.1"),
        ("B2", SENDER_B, "2026-11-10T10:30:00.000+00:00", "2.5.1"),
        ("B3", SENDER_A, "2026-11-10T10:45:00.000+00:00", "2.5.1"),
        ("C1", SENDER_A, CURRENT_START, "2.5.1"),
        ("C2", SENDER_B, "2026-11-10T11:15:00.000+00:00", "2.5.1"),
        ("C3", SENDER_A, "2026-11-10T11:45:00.000+00:00", "2.5.1"),
        ("EDGE_OUT", SENDER_A, CURRENT_END, "2.5.1"),
    ]


def _all_rows() -> dict[str, list[DrillRow]]:
    rows: dict[str, list[DrillRow]] = {}
    rows.update(_baseline_rows())
    rows.update(_current_rows())
    rows["OUT1"] = [DrillRow("NM", "OLD", "1.0", observed_at="20261110090000")]
    rows["EDGE_OUT"] = [DrillRow("NM", "EDGE", "2.0",
                                 observed_at="20261110120000")]
    return rows


def populate(store: Store) -> dict[str, list[DrillRow]]:
    """Insert the full drill dataset through the real Store commit path.

    Returns the row catalogue keyed by control id so tests and the
    reference checker derive expectations from the same source of truth.
    """
    catalogue = _all_rows()
    clocks = {cid: ts for cid, _s, ts, _v in _message_plan()}

    for control_id, sender, ts, version in _message_plan():
        raw = build_oru_message(
            sender, control_id, catalogue[control_id], version=version
        )
        # round-trip through the real parser/builder, exactly like the
        # network path would
        parsed = build_message(raw)
        original_clock = store._clock
        store._clock = lambda ts=ts: ts  # noqa: E731 - deterministic seam
        try:
            attempt = store.begin_attempt(0, 1, raw)
            outcome = store.commit_message(attempt, parsed, raw)
            assert outcome == "committed", (control_id, outcome)
            store.mark_acked(attempt)
        finally:
            store._clock = original_clock
    return catalogue


def rejected_payload() -> bytes:
    """A structurally invalid message: OBX-11 status missing -> rejected.

    It is delivered through the real server in tests so an inbox row in a
    non-committed state exists; the audit must never count it.
    """
    return (
        "MSH|^~\\&|LAB1|HOSP|RCV|RCVF|20261110123000||ORU^R01|REJ1|P|2.5.1\r"
        "PID|1||P1^^^HOSP^MR||Doe^J\r"
        "PV1|1|O\r"
        "OBR|1|PL-X|FL-X|CBC^Blood Count^LN\r"
        "OBX|1|NM|WBC^White Cells^LN|1|9.9|10*9/L||||||20261110123000\r"
    ).encode("utf-8")
