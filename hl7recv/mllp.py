"""MLLP stream framing.

Frames are ``VT <payload> FS CR``.  A TCP read boundary is *not* a frame
boundary: one read may carry half a header, several fragments, or multiple
frames.  This module implements an incremental state machine that the caller
feeds with raw bytes; it emits complete frame payloads and locatable errors.

Rules implemented here:

* bytes before a VT start delimiter are reported (locatable) and skipped;
* a frame larger than ``max_frame`` bytes is reported and discarded; the
  parser resynchronises on the next ``FS CR``;
* ``FS`` not followed by ``CR`` is reported; the payload is discarded and the
  byte after ``FS`` is reprocessed (it may start the next frame);
* on ``close()`` (peer disconnect) an unterminated frame is reported and
  discarded -- bytes are never carried into the next connection.

Offsets reported in errors are absolute byte offsets within the connection's
byte stream, so the server can log ``(conn_id, offset)`` locations.
"""

from __future__ import annotations

from dataclasses import dataclass

VT = 0x0B
FS = 0x1C
CR = 0x0D

DEFAULT_MAX_FRAME = 1 << 20  # 1 MiB


@dataclass
class FrameEvent:
    """One event produced by the parser.

    kind == "frame": payload holds the bytes between VT and FS CR.
    kind == "error": reason/offset/detail describe a locatable problem.
    """

    kind: str  # "frame" | "error"
    payload: bytes | None = None
    reason: str = ""
    offset: int = 0
    detail: str = ""


class MllpStreamParser:
    """Incremental MLLP frame parser for one connection."""

    IDLE = "IDLE"
    IN_FRAME = "IN_FRAME"
    OVERFLOW = "OVERFLOW"  # discarding an oversized frame until FS CR

    def __init__(self, max_frame: int = DEFAULT_MAX_FRAME) -> None:
        if max_frame < 16:
            raise ValueError("max_frame unreasonably small")
        self.max_frame = max_frame
        self._buf = bytearray()
        self._state = self.IDLE
        self._stream_offset = 0  # absolute offset of self._buf[0]
        self._frame_start = 0  # absolute offset of the current frame's VT

    # -- helpers ---------------------------------------------------------
    def _drop(self, n: int) -> None:
        del self._buf[:n]
        self._stream_offset += n

    @staticmethod
    def _error(reason: str, offset: int, detail: str = "") -> FrameEvent:
        return FrameEvent(kind="error", reason=reason, offset=offset, detail=detail)

    # -- public API ------------------------------------------------------
    def feed(self, data: bytes) -> list[FrameEvent]:
        """Feed bytes read from the socket; return frames and errors."""
        if not data:
            return []
        self._buf += data
        events: list[FrameEvent] = []

        while True:
            if self._state == self.IDLE:
                idx = self._buf.find(bytes([VT]))
                if idx < 0:
                    if self._buf:
                        events.append(
                            self._error(
                                "bytes before VT start delimiter",
                                self._stream_offset,
                                detail=_preview(self._buf),
                            )
                        )
                        self._drop(len(self._buf))
                    break
                if idx > 0:
                    events.append(
                        self._error(
                            "bytes before VT start delimiter",
                            self._stream_offset,
                            detail=_preview(self._buf[:idx]),
                        )
                    )
                    self._drop(idx)
                self._frame_start = self._stream_offset
                self._drop(1)  # consume VT
                self._state = self.IN_FRAME
                continue

            if self._state == self.IN_FRAME:
                idx = self._buf.find(bytes([FS]))
                if idx < 0:
                    if len(self._buf) > self.max_frame:
                        events.append(
                            self._error(
                                "frame exceeds max_frame "
                                f"({len(self._buf)} > {self.max_frame} bytes)",
                                self._frame_start,
                            )
                        )
                        self._state = self.OVERFLOW
                        continue
                    break  # wait for more data
                # FS found at idx
                if idx > self.max_frame:
                    events.append(
                        self._error(
                            "frame exceeds max_frame "
                            f"({idx} > {self.max_frame} bytes)",
                            self._frame_start,
                        )
                    )
                    # discard payload up to FS, then handle FS like OVERFLOW
                    self._drop(idx)
                    self._state = self.OVERFLOW
                    continue
                if idx + 1 >= len(self._buf):
                    break  # need the byte after FS
                nxt = self._buf[idx + 1]
                if nxt == CR:
                    payload = bytes(self._buf[:idx])
                    events.append(
                        FrameEvent(
                            kind="frame",
                            payload=payload,
                            offset=self._frame_start,
                        )
                    )
                    self._drop(idx + 2)
                    self._state = self.IDLE
                else:
                    events.append(
                        self._error(
                            "FS not followed by CR",
                            self._stream_offset + idx,
                            detail=f"found 0x{nxt:02x}",
                        )
                    )
                    self._drop(idx + 1)  # drop payload + FS, reprocess rest
                    self._state = self.IDLE
                continue

            if self._state == self.OVERFLOW:
                idx = self._buf.find(bytes([FS]))
                if idx < 0:
                    self._drop(len(self._buf))
                    break
                if idx + 1 >= len(self._buf):
                    # keep the FS so we can inspect the following byte later
                    self._drop(idx)
                    break
                nxt = self._buf[idx + 1]
                if nxt == CR:
                    self._drop(idx + 2)
                else:
                    events.append(
                        self._error(
                            "FS not followed by CR while discarding "
                            "oversized frame",
                            self._stream_offset + idx,
                            detail=f"found 0x{nxt:02x}",
                        )
                    )
                    self._drop(idx + 1)
                self._state = self.IDLE
                continue

            raise AssertionError(f"bad state {self._state}")  # unreachable

        return events

    def close(self) -> list[FrameEvent]:
        """Signal end of connection; discard any unterminated frame."""
        events: list[FrameEvent] = []
        if self._state == self.IN_FRAME or self._state == self.OVERFLOW:
            events.append(
                self._error(
                    "unterminated frame discarded at disconnect",
                    self._frame_start,
                    detail=f"{len(self._buf)} buffered bytes dropped",
                )
            )
        elif self._buf:
            events.append(
                self._error(
                    "bytes before VT start delimiter",
                    self._stream_offset,
                    detail=_preview(self._buf),
                )
            )
        self._buf = bytearray()
        self._state = self.IDLE
        return events


def _preview(buf: bytes | bytearray, limit: int = 24) -> str:
    raw = bytes(buf[:limit])
    suffix = "..." if len(buf) > limit else ""
    return f"{raw!r}{suffix}"
