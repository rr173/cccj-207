"""Unit tests for the MLLP stream parser."""

from __future__ import annotations

import unittest

from hl7recv.mllp import CR, FS, VT, MllpStreamParser


def frame(payload: bytes) -> bytes:
    return bytes([VT]) + payload + bytes([FS, CR])


class TestFraming(unittest.TestCase):
    def test_single_frame_single_read(self):
        p = MllpStreamParser()
        ev = p.feed(frame(b"MSH|^~\\&|x"))
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].kind, "frame")
        self.assertEqual(ev[0].payload, b"MSH|^~\\&|x")
        self.assertEqual(p.close(), [])

    def test_fragmented_delivery(self):
        p = MllpStreamParser()
        data = frame(b"MSH|^~\\&|abc|def")
        events = []
        for i in range(0, len(data), 3):  # 3-byte chunks
            events.extend(p.feed(data[i : i + 3]))
        frames = [e for e in events if e.kind == "frame"]
        errors = [e for e in events if e.kind == "error"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"MSH|^~\\&|abc|def")
        self.assertEqual(errors, [])

    def test_byte_by_byte(self):
        p = MllpStreamParser()
        data = frame(b"hello")
        events = []
        for b in data:
            events.extend(p.feed(bytes([b])))
        frames = [e for e in events if e.kind == "frame"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"hello")

    def test_multiple_frames_one_read(self):
        p = MllpStreamParser()
        ev = p.feed(frame(b"one") + frame(b"two") + frame(b"three"))
        frames = [e.payload for e in ev if e.kind == "frame"]
        self.assertEqual(frames, [b"one", b"two", b"three"])

    def test_frame_split_across_reads_then_second_frame(self):
        p = MllpStreamParser()
        data = frame(b"first") + frame(b"second")
        cut = len(frame(b"first")) - 1  # split inside trailing CR of frame 1
        ev1 = p.feed(data[:cut])
        ev2 = p.feed(data[cut:])
        frames = [e.payload for e in ev1 + ev2 if e.kind == "frame"]
        self.assertEqual(frames, [b"first", b"second"])

    def test_garbage_before_vt(self):
        p = MllpStreamParser()
        ev = p.feed(b"garbage" + frame(b"ok"))
        errors = [e for e in ev if e.kind == "error"]
        frames = [e for e in ev if e.kind == "frame"]
        self.assertEqual(len(errors), 1)
        self.assertIn("before VT", errors[0].reason)
        self.assertEqual(errors[0].offset, 0)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"ok")

    def test_fs_without_cr_recovers_next_frame(self):
        p = MllpStreamParser()
        bad = bytes([VT]) + b"broken" + bytes([FS])  # no CR, next byte is VT
        ev = p.feed(bad + frame(b"good"))
        errors = [e for e in ev if e.kind == "error"]
        frames = [e for e in ev if e.kind == "frame"]
        self.assertEqual(len(errors), 1)
        self.assertIn("FS not followed by CR", errors[0].reason)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"good")

    def test_oversized_frame_discarded_then_valid_frame(self):
        p = MllpStreamParser(max_frame=32)
        big = bytes([VT]) + b"x" * 100 + bytes([FS, CR])
        ev = p.feed(big + frame(b"after"))
        errors = [e for e in ev if e.kind == "error"]
        frames = [e for e in ev if e.kind == "frame"]
        self.assertEqual(len(errors), 1)
        self.assertIn("exceeds max_frame", errors[0].reason)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"after")

    def test_oversized_frame_arriving_in_chunks(self):
        p = MllpStreamParser(max_frame=32)
        big = bytes([VT]) + b"y" * 100 + bytes([FS, CR])
        events = []
        for i in range(0, len(big), 7):
            events.extend(p.feed(big[i : i + 7]))
        events.extend(p.feed(frame(b"tail")))
        errors = [e for e in events if e.kind == "error"]
        frames = [e for e in events if e.kind == "frame"]
        self.assertTrue(any("exceeds max_frame" in e.reason for e in errors))
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"tail")

    def test_oversized_frame_fs_cr_split_across_reads(self):
        p = MllpStreamParser(max_frame=16)
        # overflow, then FS arrives at end of one read, CR in the next
        ev1 = p.feed(bytes([VT]) + b"z" * 40 + bytes([FS]))
        ev2 = p.feed(bytes([CR]) + frame(b"next"))
        frames = [e for e in ev1 + ev2 if e.kind == "frame"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"next")

    def test_unterminated_frame_discarded_on_close(self):
        p = MllpStreamParser()
        p.feed(bytes([VT]) + b"partial")
        ev = p.close()
        self.assertEqual(len(ev), 1)
        self.assertIn("unterminated", ev[0].reason)

    def test_close_with_garbage_buffer(self):
        p = MllpStreamParser()
        ev = p.feed(b"junk")  # reported immediately at feed time
        self.assertEqual(len(ev), 1)
        self.assertIn("before VT", ev[0].reason)
        self.assertEqual(p.close(), [])

    def test_offsets_are_absolute(self):
        p = MllpStreamParser()
        p.feed(frame(b"abc"))  # 1 + 3 + 2 = 6 bytes
        ev = p.feed(b"XX" + frame(b"ok"))
        err = [e for e in ev if e.kind == "error"][0]
        self.assertEqual(err.offset, 6)

    def test_empty_frame(self):
        p = MllpStreamParser()
        ev = p.feed(bytes([VT, FS, CR]))
        frames = [e for e in ev if e.kind == "frame"]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].payload, b"")


if __name__ == "__main__":
    unittest.main()
