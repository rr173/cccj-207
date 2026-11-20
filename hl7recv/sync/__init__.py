"""Result subscription and downstream replica (docs/SYNC.md).

Publisher side reads committed facts plus durable commit-order positions
from a receiver database and emits deterministic page files; the
analytics side imports pages into its own SQLite replica.  The two sides
never share a database; pages are plain local files handed over
intermittently.
"""
