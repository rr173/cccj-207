"""Locatable error types for the HL7 receiver.

Every parse/structure error carries optional coordinates so operators can
locate the problem: segment name, 1-based segment ordinal in the message,
field position, repetition and component.
"""

from __future__ import annotations


class Hl7Error(Exception):
    """Base class for locatable HL7 errors."""

    def __init__(
        self,
        message: str,
        *,
        segment: str | None = None,
        seg_index: int | None = None,
        field: int | None = None,
        rep: int | None = None,
        comp: int | None = None,
    ) -> None:
        self.segment = segment
        self.seg_index = seg_index
        self.field = field
        self.rep = rep
        self.comp = comp
        loc = self.location()
        super().__init__(f"{message} [{loc}]" if loc else message)
        self.reason = message

    def location(self) -> str:
        parts: list[str] = []
        if self.segment is not None:
            if self.seg_index is not None:
                parts.append(f"segment {self.segment} #{self.seg_index}")
            else:
                parts.append(f"segment {self.segment}")
        elif self.seg_index is not None:
            parts.append(f"segment #{self.seg_index}")
        if self.field is not None:
            parts.append(f"field {self.field}")
        if self.rep is not None:
            parts.append(f"rep {self.rep}")
        if self.comp is not None:
            parts.append(f"component {self.comp}")
        return ", ".join(parts)


class ParseError(Hl7Error):
    """Low-level syntax problem (framing of fields, decoding, escapes)."""


class ProfileError(Hl7Error):
    """Message violates the confined profile (version, type, required field)."""


class StructureError(Hl7Error):
    """Segment layout / grouping problem (OBR/OBX ownership, duplicates)."""
