"""Deterministic fault-injection points.

Covers the commit/ACK boundary states required by the drills:

* ``after_frame``              -- a complete frame was received
* ``during_parse``             -- parsing is under way (MSH already valid)
* ``before_commit``            -- about to open the commit transaction
* ``after_commit_before_ack``  -- commit done, ACK not yet sent
* ``after_ack``                -- ACK sent and recorded

A spec is ``point:action:n`` where action is ``exit`` (simulate a process
crash via ``os._exit``) or ``raise`` (simulate an internal failure), and
``n`` is the 1-based hit count at which the fault fires.  Given the same
input stream the faults fire deterministically.
"""

from __future__ import annotations

import os
from collections import Counter

POINTS = (
    "after_frame",
    "during_parse",
    "before_commit",
    "after_commit_before_ack",
    "after_ack",
)

EXIT_CODE = 70  # EX_SOFTWARE: distinctive, non-signal exit for the drills


class FaultInjected(Exception):
    """Raised when a 'raise' fault fires (simulated internal failure)."""


class FaultInjector:
    def __init__(self, specs: list[tuple[str, str, int]] | None = None) -> None:
        self._rules: dict[str, list[tuple[str, int]]] = {}
        for point, action, n in specs or []:
            if point not in POINTS:
                raise ValueError(f"unknown fault point {point!r}")
            if action not in ("exit", "raise"):
                raise ValueError(f"unknown fault action {action!r}")
            if n < 1:
                raise ValueError("fault count must be >= 1")
            self._rules.setdefault(point, []).append((action, n))
        self._hits: Counter[str] = Counter()

    @classmethod
    def from_strings(cls, specs: list[str] | None) -> "FaultInjector":
        parsed = []
        for spec in specs or []:
            parts = spec.split(":")
            if len(parts) != 3:
                raise ValueError(
                    f"fault spec {spec!r} must look like point:action:n"
                )
            parsed.append((parts[0], parts[1], int(parts[2])))
        return cls(parsed)

    @property
    def armed(self) -> bool:
        return bool(self._rules)

    def hit(self, point: str) -> None:
        if point not in self._rules:
            return
        self._hits[point] += 1
        for action, n in self._rules[point]:
            if self._hits[point] == n:
                if action == "exit":
                    # Simulate an abrupt process crash: no cleanup, no
                    # atexit handlers, no finally blocks.
                    os._exit(EXIT_CODE)
                raise FaultInjected(f"injected fault at {point} (hit {n})")


NULL_INJECTOR = FaultInjector()
