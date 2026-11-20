"""Build a ParsedMessage (clinical fact model) from a parsed envelope.

Implements the confined profile:

* message type must be ORU^R01, version must be a supported profile;
* segment layout: MSH, PID, PV1, then one or more OBR groups.  Each group
  is ``[ORC] OBR (NTE|OBX...)*``; an NTE directly after OBR (before the
  first OBX) belongs to the OBR, an NTE after an OBX belongs to that OBX;
* required fields per the profile tables, including CX (identifier +
  assigning authority) and CE (code + coding system) decomposition;
* duplicate in-message result keys merge only when payload and source
  semantics are identical, otherwise the whole message is rejected.

Any failure raises a locatable Hl7Error; nothing is partially produced.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import Hl7Error, ParseError, ProfileError, StructureError
from .hl7 import Delimiters, Envelope, Segment, parse_envelope
from .profile import (
    KNOWN_SEGMENTS,
    MESSAGE_TYPE,
    RESULT_STATUS_ALLOWED,
    TRIGGER,
    Profile,
    get_profile,
)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
@dataclass
class Nte:
    seg_index: int
    text: str


@dataclass
class ObxFact:
    obr_index: int
    obx_index: int
    obr_seg: int
    obx_seg: int
    value_type: str
    obs_code: str
    obs_system: str
    obs_name: str | None
    sub_id: str | None
    value_text: str
    units: str | None
    ref_range: str | None
    abnormal: str | None
    status: str
    observed_at: str | None
    ntes: list[Nte] = field(default_factory=list)
    merged: int = 0

    def result_key(self, order_key: tuple[str | None, ...]) -> tuple:
        return (
            order_key,
            self.obs_code,
            self.obs_system,
            self.sub_id or "",
        )

    def payload(self) -> tuple:
        return (
            self.value_type,
            self.value_text,
            self.units,
            self.ref_range,
            self.abnormal,
            self.status,
            self.observed_at,
            tuple(n.text for n in self.ntes),
        )


@dataclass
class ObrGroup:
    obr_index: int
    obr_seg: int
    placer_order: str | None
    filler_order: str | None
    test_code: str
    test_system: str
    test_name: str | None
    obr_observed_at: str | None
    reported_at: str | None
    ntes: list[Nte] = field(default_factory=list)
    obxs: list[ObxFact] = field(default_factory=list)

    @property
    def order_key(self) -> tuple[str | None, ...]:
        return (self.filler_order, self.placer_order, self.test_code, self.test_system)


@dataclass
class ParsedMessage:
    version: str
    sender_app: str
    sender_fac: str
    control_id: str
    sent_at: str | None
    processing_id: str | None
    patients: list[tuple[str, str]]  # (assigning_authority, identifier), in order
    patient_name: str | None
    patient_class: str | None
    visit_id: str | None
    groups: list[ObrGroup]

    @property
    def sender(self) -> str:
        return f"{self.sender_app}^{self.sender_fac}"


# ---------------------------------------------------------------------------
# Field extraction helpers (all locatable)
# ---------------------------------------------------------------------------
def _required_text(seg: Segment, pos: int, d: Delimiters) -> str:
    f = seg.field(pos)
    if not f.present:
        raise ProfileError(
            f"required field {seg.name}-{pos} is missing",
            segment=seg.name, seg_index=seg.index, field=pos,
        )
    text = f.text(d)
    if text is None or text == "":
        raise ProfileError(
            f"required field {seg.name}-{pos} is empty",
            segment=seg.name, seg_index=seg.index, field=pos,
        )
    return text


def _optional_text(seg: Segment, pos: int, d: Delimiters) -> str | None:
    f = seg.field(pos)
    if not f.present:
        return None
    return f.text(d)


def _ce(seg: Segment, pos: int, d: Delimiters, *, required: bool) -> tuple[str | None, str | None, str | None]:
    """Decompose a CE/CWE field -> (code, name, coding system)."""
    f = seg.field(pos)
    if not f.present or f.text(d) in (None, ""):
        if required:
            raise ProfileError(
                f"required coded field {seg.name}-{pos} is missing or empty",
                segment=seg.name, seg_index=seg.index, field=pos,
            )
        return None, None, None
    code = f.component(1, 1)
    system = f.component(1, 3)
    name = f.component(1, 2)
    if required:
        if not code:
            raise ProfileError(
                f"{seg.name}-{pos} code component is empty",
                segment=seg.name, seg_index=seg.index, field=pos, rep=1, comp=1,
            )
        if not system:
            raise ProfileError(
                f"{seg.name}-{pos} coding system component is empty",
                segment=seg.name, seg_index=seg.index, field=pos, rep=1, comp=3,
            )
    return code, name, system


def _cx(seg: Segment, pos: int, rep: int, d: Delimiters) -> tuple[str | None, str | None]:
    """Decompose one repetition of a CX field -> (identifier, authority)."""
    f = seg.field(pos)
    if not f.present or rep > f.n_reps:
        return None, None
    return f.component(rep, 1), f.component(rep, 4)


def _ei(seg: Segment, pos: int, d: Delimiters) -> str | None:
    f = seg.field(pos)
    if not f.present:
        return None
    return f.component(1, 1)


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------
def build_message(data: bytes, hook=None) -> ParsedMessage:
    """Parse and validate a raw message into a ParsedMessage.

    ``hook`` (optional) is invoked once after the MSH segment has been
    validated -- a deterministic fault-injection point "during parse".
    """
    env = parse_envelope(data)
    d = env.delimiters
    segs = env.segments

    if not segs or segs[0].name != "MSH":
        raise ParseError("message does not start with MSH segment", segment="MSH", seg_index=1)
    msh = segs[0]

    # -- MSH header fields -------------------------------------------------
    msg_type = msh.field(9)
    mt1 = msg_type.component(1, 1)
    mt2 = msg_type.component(1, 2)
    if mt1 != MESSAGE_TYPE or mt2 != TRIGGER:
        raise ProfileError(
            f"unsupported message type {mt1 or ''}^{mt2 or ''} (profile accepts "
            f"{MESSAGE_TYPE}^{TRIGGER} only)",
            segment="MSH", seg_index=1, field=9,
        )
    version = _required_text(msh, 12, d)
    prof: Profile | None = get_profile(version)
    if prof is None:
        raise ProfileError(
            f"unsupported profile version {version!r}",
            segment="MSH", seg_index=1, field=12,
        )
    control_id = _required_text(msh, 10, d)
    sender_app = _required_text(msh, 3, d)
    sender_fac = _required_text(msh, 4, d)
    sent_at = _optional_text(msh, 7, d)
    processing_id = _optional_text(msh, 11, d)

    if hook is not None:
        hook()

    # -- segment layout ----------------------------------------------------
    body = segs[1:]
    for seg in body:
        if seg.name not in KNOWN_SEGMENTS:
            raise StructureError(
                f"segment {seg.name} is outside the confined profile",
                segment=seg.name, seg_index=seg.index,
            )
        if seg.name == "MSH":
            raise StructureError(
                "duplicate MSH segment", segment="MSH", seg_index=seg.index
            )

    if len(body) < 2 or body[0].name != "PID":
        raise StructureError(
            "PID segment must follow MSH",
            segment=body[0].name if body else None,
            seg_index=body[0].index if body else None,
        )
    if body[1].name != "PV1":
        raise StructureError(
            "PV1 segment must follow PID",
            segment=body[1].name, seg_index=body[1].index,
        )

    # -- PID ---------------------------------------------------------------
    pid = body[0]
    pid3 = pid.field(3)
    if not pid3.present or pid3.n_reps == 0 or pid3.text(d) in (None, ""):
        raise ProfileError(
            "required field PID-3 patient identifier list is missing or empty",
            segment="PID", seg_index=pid.index, field=3,
        )
    patients: list[tuple[str, str]] = []
    for rep in range(1, pid3.n_reps + 1):
        ident, auth = _cx(pid, 3, rep, d)
        if not ident:
            raise ProfileError(
                "PID-3 identifier component is empty",
                segment="PID", seg_index=pid.index, field=3, rep=rep, comp=1,
            )
        if not auth:
            raise ProfileError(
                "PID-3 assigning authority component is empty",
                segment="PID", seg_index=pid.index, field=3, rep=rep, comp=4,
            )
        patients.append((auth, ident))
    patient_name = _optional_text(pid, 5, d)

    # -- PV1 ---------------------------------------------------------------
    pv1 = body[1]
    patient_class = _optional_text(pv1, 2, d)
    visit_id, _visit_auth = _cx(pv1, 19, 1, d)

    # -- OBR groups ----------------------------------------------------------
    groups: list[ObrGroup] = []
    current: ObrGroup | None = None
    last_kind: str | None = None  # last structural segment kind in group
    last_obx: ObxFact | None = None
    orc_placer: str | None = None
    orc_filler: str | None = None
    pending_orc = False

    for seg in body[2:]:
        name = seg.name
        if name == "ORC":
            if pending_orc:
                raise StructureError(
                    "duplicate ORC within one OBR group",
                    segment="ORC", seg_index=seg.index,
                )
            # ORC belongs to the OBR group it precedes; its order numbers
            # are fallbacks when OBR-2/OBR-3 are empty.
            orc_placer = _ei(seg, 2, d)
            orc_filler = _ei(seg, 3, d)
            pending_orc = True
            last_kind = "ORC"
            continue

        if name == "OBR":
            code, tname, system = _ce(seg, 4, d, required=True)
            current = ObrGroup(
                obr_index=len(groups) + 1,
                obr_seg=seg.index,
                placer_order=_ei(seg, 2, d) or orc_placer,
                filler_order=_ei(seg, 3, d) or orc_filler,
                test_code=code or "",
                test_system=system or "",
                test_name=tname,
                obr_observed_at=_optional_text(seg, 7, d),
                reported_at=_optional_text(seg, 22, d),
            )
            groups.append(current)
            pending_orc = False
            orc_placer = orc_filler = None
            last_kind = "OBR"
            last_obx = None
            continue

        if name == "OBX":
            if current is None:
                raise StructureError(
                    "OBX segment appears before any OBR (ownership unclear)",
                    segment="OBX", seg_index=seg.index,
                )
            obx = _build_obx(seg, current, d, prof)
            current.obxs.append(obx)
            last_obx = obx
            last_kind = "OBX"
            continue

        if name == "NTE":
            if current is None or pending_orc:
                raise StructureError(
                    "NTE ownership cannot be determined "
                    "(no OBR group open for this NTE)",
                    segment="NTE", seg_index=seg.index,
                )
            comment = _required_text(seg, 3, d)
            nte = Nte(seg_index=seg.index, text=comment)
            if last_kind == "OBX" and last_obx is not None:
                last_obx.ntes.append(nte)
                last_kind = "NTE_OBX"
            elif last_kind == "NTE_OBX" and last_obx is not None:
                last_obx.ntes.append(nte)
            elif last_kind in ("OBR", "ORC", "NTE_OBR"):
                current.ntes.append(nte)
                last_kind = "NTE_OBR"
            else:
                raise StructureError(
                    "NTE ownership cannot be determined",
                    segment="NTE", seg_index=seg.index,
                )
            continue

        raise StructureError(
            f"segment {name} is not allowed here",
            segment=name, seg_index=seg.index,
        )

    if not groups:
        raise StructureError("message contains no OBR group", segment="OBR")

    _merge_or_reject_duplicates(groups)

    return ParsedMessage(
        version=version,
        sender_app=sender_app,
        sender_fac=sender_fac,
        control_id=control_id,
        sent_at=sent_at,
        processing_id=processing_id,
        patients=patients,
        patient_name=patient_name,
        patient_class=patient_class,
        visit_id=visit_id,
        groups=groups,
    )


def _build_obx(seg: Segment, group: ObrGroup, d: Delimiters, prof: Profile) -> ObxFact:
    value_type = _required_text(seg, 2, d)
    if value_type not in prof.obx2_allowed:
        allowed = ",".join(sorted(prof.obx2_allowed))
        raise ProfileError(
            f"OBX-2 value type {value_type!r} not allowed by profile "
            f"{prof.version} (allowed: {allowed})",
            segment="OBX", seg_index=seg.index, field=2,
        )
    obs_code, obs_name, obs_system = _ce(seg, 3, d, required=True)
    value = _required_text(seg, 5, d)
    status = _required_text(seg, 11, d)
    if status not in RESULT_STATUS_ALLOWED:
        raise ProfileError(
            f"OBX-11 result status {status!r} not in HL7 table 0085",
            segment="OBX", seg_index=seg.index, field=11,
        )
    units, _units_name, _units_sys = _ce(seg, 6, d, required=False)
    units_text = _optional_text(seg, 6, d)
    return ObxFact(
        obr_index=group.obr_index,
        obx_index=len(group.obxs) + 1,
        obr_seg=group.obr_seg,
        obx_seg=seg.index,
        value_type=value_type,
        obs_code=obs_code or "",
        obs_system=obs_system or "",
        obs_name=obs_name,
        sub_id=_optional_text(seg, 4, d),
        value_text=value,
        units=units_text if units_text else (units or None),
        ref_range=_optional_text(seg, 7, d),
        abnormal=_optional_text(seg, 8, d),
        status=status,
        observed_at=_optional_text(seg, 14, d) or group.obr_observed_at,
    )


def _merge_or_reject_duplicates(groups: list[ObrGroup]) -> None:
    """Merge identical duplicate result keys; reject conflicting ones."""
    for group in groups:
        seen: dict[tuple, ObxFact] = {}
        kept: list[ObxFact] = []
        for obx in group.obxs:
            key = obx.result_key(group.order_key)
            prev = seen.get(key)
            if prev is None:
                seen[key] = obx
                kept.append(obx)
                continue
            if prev.payload() == obx.payload():
                prev.merged += 1 + obx.merged
                continue
            raise StructureError(
                "conflicting duplicate result key within message "
                f"(key={key!r}: first at OBX segment #{prev.obx_seg}, "
                f"conflicting at OBX segment #{obx.obx_seg})",
                segment="OBX",
                seg_index=obx.obx_seg,
            )
        group.obxs = kept
