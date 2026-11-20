"""HL7 ACK message construction.

The ACK mirrors the incoming message's version and swaps sender/receiver.
MSA-1 is AA (application accept) or AE (application error); a bounded ERR
segment carries a locatable diagnostic for rejections.  ACKs are only
built *after* the persistence decision has been made -- see server.py for
the commit/ACK ordering.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .mllp import CR, FS, VT

AA = "AA"
AE = "AE"

MAX_ERR_TEXT = 200


def _escape(text: str, field_sep: str = "|", comp: str = "^", rep: str = "~",
            esc: str = "\\", sub: str = "&") -> str:
    out = []
    for ch in text:
        if ch == field_sep:
            out.append(esc + "F" + esc)
        elif ch == comp:
            out.append(esc + "S" + esc)
        elif ch == rep:
            out.append(esc + "R" + esc)
        elif ch == esc:
            out.append(esc + "E" + esc)
        elif ch == sub:
            out.append(esc + "T" + esc)
        elif ch in ("\r", "\n"):
            out.append(" ")
        else:
            out.append(ch)
    return "".join(out)


def build_ack(
    code: str,
    control_id: str,
    *,
    version: str = "2.5.1",
    sending_app: str = "HL7RECV",
    sending_fac: str = "LOCAL",
    recv_app: str = "",
    recv_fac: str = "",
    ack_id: str | None = None,
    err_text: str | None = None,
) -> bytes:
    """Build a framed ACK message (VT ... FS CR)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ack_id = ack_id or f"ACK{ts}"
    lines = [
        f"MSH|^~\\&|{_escape(sending_app)}|{_escape(sending_fac)}|"
        f"{_escape(recv_app)}|{_escape(recv_fac)}|{ts}||ACK^R01|{ack_id}|P|{version}",
        f"MSA|{code}|{_escape(control_id)}",
    ]
    if err_text:
        bounded = err_text[:MAX_ERR_TEXT]
        lines.append(f"ERR|207^{_escape(bounded)}^HL70357")
    payload = "\r".join(lines) + "\r"
    return bytes([VT]) + payload.encode("utf-8") + bytes([FS, CR])


def parse_ack(payload: bytes) -> dict[str, str | None]:
    """Minimal ACK parser used by the local peer and the test-suite.

    Returns {"code": MSA-1, "control_id": MSA-2, "err": ERR text or None}.
    """
    text = payload.decode("utf-8", errors="replace")
    segs = [s for s in text.split("\r") if s]
    out: dict[str, str | None] = {"code": None, "control_id": None, "err": None}
    sep = "|"
    for seg in segs:
        if seg.startswith("MSH") and len(seg) > 3:
            sep = seg[3]
        parts = seg.split(sep)
        if parts[0] == "MSA" and len(parts) > 2:
            out["code"] = parts[1]
            out["control_id"] = parts[2]
        elif parts[0] == "ERR" and len(parts) > 1:
            err_parts = parts[1].split("^")
            if len(err_parts) > 1:
                out["err"] = err_parts[1]
    return out
