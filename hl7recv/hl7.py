"""HL7 v2 hierarchical message parsing.

The parser honours the field separator and encoding characters *declared in
MSH* -- it never assumes ``|`` or ``^~\\&``.  Fields are decomposed into
repetitions -> components -> subcomponents; HL7 escape sequences are
resolved at the leaf level.  Empty fields (present but ``""``) stay
distinct from missing fields (absent from the segment).

This module only produces a generic segment tree plus delimiter
information.  Profile validation and clinical-fact extraction live in
``builder.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import ParseError

SEGMENT_TERM = "\r"


@dataclass(frozen=True)
class Delimiters:
    field: str
    component: str
    repetition: str
    escape: str
    subcomponent: str


@dataclass
class Field:
    """One parsed field.

    present=False  -> the field is missing (segment ended earlier).
    present=True with reps=[[[""]]] -> the field is present but empty.
    Leaf values are unescaped.
    """

    present: bool
    reps: list[list[list[str]]] = field(default_factory=list)
    raw: str = ""

    def text(self, d: Delimiters) -> str | None:
        """Logical text of the field: hierarchy re-joined with the declared
        delimiters, leaves unescaped.  None when the field is missing."""
        if not self.present:
            return None
        return d.repetition.join(
            d.component.join(d.subcomponent.join(sub for sub in comp) for comp in rep)
            for rep in self.reps
        )

    def component(self, rep: int, comp: int) -> str | None:
        """1-based component access; None when missing."""
        if not self.present:
            return None
        if rep - 1 >= len(self.reps):
            return None
        r = self.reps[rep - 1]
        if comp - 1 >= len(r):
            return None
        return "".join(r[comp - 1])

    @property
    def n_reps(self) -> int:
        return len(self.reps) if self.present else 0


@dataclass
class Segment:
    name: str
    index: int  # 1-based ordinal of the segment in the message
    fields: list[Field]  # fields[0] is field position 1
    raw: str

    def field(self, pos: int) -> Field:
        """1-based field access; returns a missing Field when absent."""
        if pos - 1 < len(self.fields):
            return self.fields[pos - 1]
        return Field(present=False)


@dataclass
class Envelope:
    """A parsed message: delimiters + ordered segments."""

    delimiters: Delimiters
    segments: list[Segment]
    text: str


def unescape(s: str, d: Delimiters) -> str:
    """Resolve HL7 escape sequences in a leaf value.

    Handled: \\F\\ \\S\\ \\R\\ \\E\\ \\T\\, \\Xhh..\\ (hex, decoded as UTF-8),
    \\H\\ / \\N\\ (formatting toggles, dropped), \\Z...\\ (local, content kept
    verbatim).  Unknown or unterminated sequences are kept literally.
    """
    esc = d.escape
    if esc not in s:
        return s
    out: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c != esc:
            out.append(c)
            i += 1
            continue
        j = s.find(esc, i + 1)
        if j < 0:  # unterminated escape: keep the rest literally
            out.append(s[i:])
            break
        seq = s[i + 1 : j]
        if seq == "F":
            out.append(d.field)
        elif seq == "S":
            out.append(d.component)
        elif seq == "R":
            out.append(d.repetition)
        elif seq == "E":
            out.append(d.escape)
        elif seq == "T":
            out.append(d.subcomponent)
        elif seq.startswith("X") and len(seq) > 1:
            try:
                out.append(bytes.fromhex(seq[1:]).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                out.append(s[i : j + 1])
        elif seq in ("H", "N"):
            pass  # highlighting toggles carry no text
        elif seq.startswith("Z"):
            out.append(seq)  # local escape: keep content as-is
        else:
            out.append(s[i : j + 1])
        i = j + 1
    return "".join(out)


def _parse_field(raw: str, d: Delimiters) -> Field:
    reps: list[list[list[str]]] = []
    for rep_raw in raw.split(d.repetition):
        rep: list[list[str]] = []
        for comp_raw in rep_raw.split(d.component):
            subs = [unescape(sub, d) for sub in comp_raw.split(d.subcomponent)]
            rep.append(subs)
        reps.append(rep)
    return Field(present=True, reps=reps, raw=raw)


def parse_envelope(data: bytes) -> Envelope:
    """Decode and hierarchically parse a raw HL7 message (frame payload)."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParseError(f"message is not valid UTF-8: {exc}") from exc

    if not text.startswith("MSH"):
        raise ParseError("message does not start with MSH segment", segment="MSH", seg_index=1)
    if len(text) < 4:
        raise ParseError("MSH segment truncated before field separator", segment="MSH", seg_index=1)
    field_sep = text[3]
    if field_sep == "\r" or field_sep.isalnum():
        raise ParseError(
            f"invalid MSH field separator {field_sep!r}", segment="MSH", seg_index=1, field=1
        )

    # Split into segments on CR; tolerate one LF directly after each CR.
    raw_segments = [s for s in text.split(SEGMENT_TERM)]
    seg_texts: list[str] = []
    for raw in raw_segments:
        if raw.startswith("\n"):
            raw = raw[1:]
        if raw == "":
            continue
        seg_texts.append(raw)

    first = seg_texts[0]
    msh_parts = first.split(field_sep)
    if len(msh_parts) < 2 or len(msh_parts[1]) < 4:
        raise ParseError(
            "MSH-2 encoding characters missing or shorter than 4 characters",
            segment="MSH",
            seg_index=1,
            field=2,
        )
    enc = msh_parts[1]
    delims = Delimiters(
        field=field_sep,
        component=enc[0],
        repetition=enc[1],
        escape=enc[2],
        subcomponent=enc[3],
    )
    for name, ch in (
        ("component", delims.component),
        ("repetition", delims.repetition),
        ("escape", delims.escape),
        ("subcomponent", delims.subcomponent),
    ):
        if ch == field_sep or ch == SEGMENT_TERM or ch.isalnum():
            raise ParseError(
                f"invalid {name} separator {ch!r}", segment="MSH", seg_index=1, field=2
            )

    segments: list[Segment] = []
    for idx, seg_text in enumerate(seg_texts, start=1):
        parts = seg_text.split(field_sep)
        name = parts[0]
        if len(name) != 3 or not name.isalnum() or name.upper() != name:
            raise ParseError(
                f"invalid segment name {name!r}", segment=name or None, seg_index=idx
            )
        if name == "MSH":
            # MSH-1 is the separator itself; MSH-2..n live in parts[1..n-1].
            fields = [Field(present=True, reps=[[[field_sep]]], raw=field_sep)]
            fields.extend(_parse_field(p, delims) for p in parts[1:])
        else:
            fields = [_parse_field(p, delims) for p in parts[1:]]
        segments.append(Segment(name=name, index=idx, fields=fields, raw=seg_text))

    return Envelope(delimiters=delims, segments=segments, text=text)


def best_effort_msh(data: bytes) -> dict[str, str | None]:
    """Extract sender/control-id/version from a possibly broken message.

    Used only to address an AE acknowledgement when full parsing failed.
    Never raises.
    """
    out: dict[str, str | None] = {
        "sending_application": None,
        "sending_facility": None,
        "message_control_id": None,
        "version_id": None,
    }
    try:
        text = data.decode("utf-8", errors="replace")
        first = text.split(SEGMENT_TERM)[0]
        if not first.startswith("MSH") or len(first) < 4:
            return out
        sep = first[3]
        parts = first.split(sep)
        # parts[0]="MSH", parts[1]=MSH-2, parts[k]=MSH-(k+1)
        if len(parts) > 2:
            out["sending_application"] = parts[2] or None
        if len(parts) > 3:
            out["sending_facility"] = parts[3] or None
        if len(parts) > 9:
            out["message_control_id"] = parts[9] or None
        if len(parts) > 11:
            out["version_id"] = parts[11] or None
    except Exception:
        pass
    return out
