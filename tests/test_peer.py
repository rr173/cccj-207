"""Peer-side helpers: buffered ACK reader, coalesced ACK frames."""

from __future__ import annotations

import socket
import unittest

from hl7recv.ack import build_ack
from peer.client import AckReader


class TestAckReader(unittest.TestCase):
    def test_coalesced_acks_both_read(self):
        a, b = socket.socketpair()
        try:
            frame1 = build_ack("AA", "C1")
            frame2 = build_ack("AE", "C2", err_text="boom")
            a.sendall(frame1 + frame2)  # both ACKs in one write/segment
            reader = AckReader(b, timeout=2.0)
            ack1 = reader.read()
            ack2 = reader.read()
            self.assertEqual((ack1.code, ack1.control_id), ("AA", "C1"))
            self.assertEqual((ack2.code, ack2.control_id), ("AE", "C2"))
            self.assertEqual(ack2.err, "boom")
        finally:
            a.close()
            b.close()

    def test_fragmented_ack(self):
        a, b = socket.socketpair()
        try:
            frame = build_ack("AA", "C9")
            a.sendall(frame[:5])
            a.sendall(frame[5:])
            reader = AckReader(b, timeout=2.0)
            self.assertEqual(reader.read().control_id, "C9")
        finally:
            a.close()
            b.close()

    def test_no_ack_returns_none_code(self):
        a, b = socket.socketpair()
        try:
            a.close()  # peer vanishes without ACK
            reader = AckReader(b, timeout=1.0)
            self.assertIsNone(reader.read().code)
        finally:
            b.close()


if __name__ == "__main__":
    unittest.main()
