"""CLI for the local MLLP peer.

Examples:
  python -m peer --host 127.0.0.1 --port 2575 --file msg.hl7
  python -m peer --port 2575 --file msg.hl7 --chunk 7 --delay 0.05
  python -m peer --port 2575 --raw-file stream.bin --expect-acks 2
"""

from __future__ import annotations

import argparse
import sys

from .client import send_message, send_raw


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="peer", description="local MLLP peer (synthetic)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--file", help="HL7 message file to frame and send")
    ap.add_argument("--raw-file", help="raw bytes to send as-is (framing tests)")
    ap.add_argument("--chunk", type=int, default=None, help="split sends into N-byte writes")
    ap.add_argument("--delay", type=float, default=0.0, help="delay between chunk writes (s)")
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--expect-acks", type=int, default=1)
    ap.add_argument("--expect", choices=["AA", "AE", "NONE"], default=None,
                    help="exit non-zero unless the (first) ACK matches")
    args = ap.parse_args(argv)

    if not args.file and not args.raw_file:
        ap.error("one of --file or --raw-file is required")

    if args.raw_file:
        with open(args.raw_file, "rb") as fh:
            data = fh.read()
        chunks = [data] if not args.chunk else [
            data[i : i + args.chunk] for i in range(0, len(data), args.chunk)
        ]
        acks = send_raw(args.host, args.port, chunks, delay=args.delay,
                        timeout=args.timeout, expect_acks=args.expect_acks)
    else:
        with open(args.file, "rb") as fh:
            payload = fh.read()
        acks = [
            send_message(args.host, args.port, payload, chunk=args.chunk,
                         delay=args.delay, timeout=args.timeout)
        ]

    rc = 0
    for i, ack in enumerate(acks):
        print(f"ack[{i}]: code={ack.code} control_id={ack.control_id} err={ack.err}")
        if args.expect == "NONE":
            if ack.code is not None:
                rc = 1
        elif args.expect and ack.code != args.expect:
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
