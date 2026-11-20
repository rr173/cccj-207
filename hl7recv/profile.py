"""Confined HL7 profile tables.

This receiver implements a deliberately small, explicit subset of HL7
v2.3.1 and v2.5.1 for ORU^R01 messages only.  Anything outside the tables
below is rejected -- the receiver never guesses mappings.

See docs/PROFILE.md for the human-readable description; this module is the
machine-readable source of truth shared by the parser and the builder.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MESSAGE_TYPE = "ORU"
TRIGGER = "R01"

# Segments this profile reads, in the order they may appear.  Any other
# segment is rejected (strict profile: no guessing).
KNOWN_SEGMENTS = ("MSH", "PID", "PV1", "ORC", "OBR", "OBX", "NTE")

# HL7 table 0085 - observation result status codes (both versions).
RESULT_STATUS_ALLOWED = frozenset("CDFINOPRSUWX")


@dataclass(frozen=True)
class FieldSpec:
    """One field the profile reads from a segment."""

    pos: int  # HL7 field position (MSH-3 -> 3)
    name: str
    required: bool = False
    kind: str = "ST"  # ST | TS | CX | CE | EI
    repeating: bool = False


@dataclass(frozen=True)
class Profile:
    version: str
    obx2_allowed: frozenset[str]
    segments: dict[str, tuple[FieldSpec, ...]] = field(default_factory=dict)


_COMMON_SEGMENTS: dict[str, tuple[FieldSpec, ...]] = {
    "MSH": (
        FieldSpec(3, "sending_application", required=True),
        FieldSpec(4, "sending_facility", required=True),
        FieldSpec(7, "message_datetime", required=True, kind="TS"),
        FieldSpec(9, "message_type", required=True),
        FieldSpec(10, "message_control_id", required=True),
        FieldSpec(11, "processing_id"),
        FieldSpec(12, "version_id", required=True),
    ),
    "PID": (
        FieldSpec(3, "patient_identifier_list", required=True, kind="CX", repeating=True),
        FieldSpec(5, "patient_name"),
    ),
    "PV1": (
        FieldSpec(2, "patient_class"),
        FieldSpec(19, "visit_number", kind="CX"),
    ),
    "ORC": (
        FieldSpec(2, "placer_order_number", kind="EI"),
        FieldSpec(3, "filler_order_number", kind="EI"),
    ),
    "OBR": (
        FieldSpec(1, "set_id"),
        FieldSpec(2, "placer_order_number", kind="EI"),
        FieldSpec(3, "filler_order_number", kind="EI"),
        FieldSpec(4, "universal_service_id", required=True, kind="CE"),
        FieldSpec(7, "observation_datetime", kind="TS"),
        FieldSpec(22, "results_report_datetime", kind="TS"),
    ),
    "OBX": (
        FieldSpec(1, "set_id"),
        FieldSpec(2, "value_type", required=True),
        FieldSpec(3, "observation_identifier", required=True, kind="CE"),
        FieldSpec(4, "observation_sub_id"),
        FieldSpec(5, "observation_value", required=True),
        FieldSpec(6, "units", kind="CE"),
        FieldSpec(7, "references_range"),
        FieldSpec(8, "abnormal_flags"),
        FieldSpec(11, "observation_result_status", required=True),
        FieldSpec(14, "observation_datetime", kind="TS"),
    ),
    "NTE": (
        FieldSpec(1, "set_id"),
        FieldSpec(3, "comment", required=True, repeating=True),
    ),
}

# OBX-2 value types accepted by this profile.  v2.5.1 adds CWE/CNE which do
# not exist in v2.3.1 -- a real, checkable difference between the two
# supported profile versions.
_OBX2_V231 = frozenset({"NM", "ST", "TX", "FT", "CE", "DT", "TM", "TS"})
_OBX2_V251 = _OBX2_V231 | {"CWE", "CNE"}

PROFILES: dict[str, Profile] = {
    "2.3.1": Profile(
        version="2.3.1",
        obx2_allowed=_OBX2_V231,
        segments=_COMMON_SEGMENTS,
    ),
    "2.5.1": Profile(
        version="2.5.1",
        obx2_allowed=_OBX2_V251,
        segments=_COMMON_SEGMENTS,
    ),
}

SUPPORTED_VERSIONS = tuple(PROFILES.keys())


def get_profile(version: str) -> Profile | None:
    return PROFILES.get(version)
