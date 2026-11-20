"""Deterministic test runner that keeps Python isolated (``-I`` semantics).

``python -I`` removes the working directory from ``sys.path`` and ignores
``PYTHONPATH``; this runner inserts the workspace root explicitly so the
packages import, while user site-packages and environment path overrides
stay disabled.  Usage::

    python3 -I tests/run_isolated.py
"""

from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(ROOT, "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
