"""Local MLLP peer (sender) used for drills and the acceptance suite.

Purely synthetic: it only speaks to a local receiver and never touches
real instruments.  Supports fragmented delivery (fixed-size chunks with
optional delays), raw byte streams for framing tests, and ACK reading.
"""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass

from hl7recv.ack import parse_ack
from hl7recv.mllp import CR, FS, VT


@dataclass
class AckResult:
    code: str | None  # MSA-1 (AA / AE) or None when no ACK arrived
    control_id: str | None
    err: str | None
    raw: bytes | None  # raw ACK frame payload


def frame_payload(payload: bytes) -> bytes:
    return bytes([VT]) + payload + bytes([FS, CR])


class AckReader:
    """Buffered ACK reader for one connection.

    ACK frames may arrive coalesced in a single TCP segment; the buffer is
    kept across calls so no ACK is ever lost between reads.
    """

    def __init__(self, conn: socket.socket, timeout: float = 5.0) -> None:
        self.conn = conn
        self.timeout = timeout
        self._buf = bytearray()

    def read(self) -> AckResult:
        self.conn.settimeout(self.timeout)
        try:
            while True:
                end = self._buf.find(bytes([FS, CR]))
                if end >= 0:
                    payload = bytes(self._buf[:end])
                    del self._buf[: end + 2]
                    if payload[:1] == bytes([VT]):
                        payload = payload[1:]
                    parsed = parse_ack(payload)
                    return AckResult(
                        code=parsed["code"],
                        control_id=parsed["control_id"],
                        err=parsed["err"],
                        raw=payload,
                    )
                data = self.conn.recv(65536)
                if not data:
                    break
                self._buf += data
        except (socket.timeout, TimeoutError):
            pass
        except OSError:
            pass
        return AckResult(code=None, control_id=None, err=None, raw=None)


def read_ack(conn: socket.socket, timeout: float = 5.0) -> AckResult:
    """Read one MLLP frame and parse it as an ACK (single-shot helper)."""
    return AckReader(conn, timeout).read()


def send_message(
    host: str,
    port: int,
    payload: bytes,
    *,
    chunk: int | None = None,
    delay: float = 0.0,
    timeout: float = 5.0,
    read_response: bool = True,
) -> AckResult:
    """Frame and send one message; return the parsed ACK.

    ``chunk`` splits the framed bytes into fixed-size writes (simulating
    arbitrary TCP segmentation); ``delay`` sleeps between writes.
    """
    framed = frame_payload(payload)
    with socket.create_connection((host, port), timeout=timeout) as conn:
        if chunk is None or chunk <= 0:
            conn.sendall(framed)
        else:
            for i in range(0, len(framed), chunk):
                conn.sendall(framed[i : i + chunk])
                if delay:
                    time.sleep(delay)
        if not read_response:
            return AckResult(code=None, control_id=None, err=None, raw=None)
        return read_ack(conn, timeout=timeout)


def send_raw(
    host: str,
    port: int,
    chunks: list[bytes],
    *,
    delay: float = 0.0,
    timeout: float = 5.0,
    expect_acks: int = 1,
) -> list[AckResult]:
    """Send raw byte chunks on one connection and read ``expect_acks`` ACKs.

    Used for framing tests (garbage bytes, multiple frames per read,
    oversized frames) where the caller controls the exact byte stream.
    """
    with socket.create_connection((host, port), timeout=timeout) as conn:
        for chunk_data in chunks:
            conn.sendall(chunk_data)
            if delay:
                time.sleep(delay)
        reader = AckReader(conn, timeout)
        return [reader.read() for _ in range(expect_acks)]
