"""hl7recv — reliable HL7 v2 MLLP receiver (synthetic-data drills).

Library interface:

    from hl7recv import Store, MllpServer, build_message, queries

See docs/PROFILE.md for the confined profile and commit/ACK semantics.
"""

from .builder import ParsedMessage, build_message
from .errors import Hl7Error, ParseError, ProfileError, StructureError
from .mllp import MllpStreamParser
from .server import MllpServer
from .store import Store

__all__ = [
    "Hl7Error",
    "MllpServer",
    "MllpStreamParser",
    "ParseError",
    "ParsedMessage",
    "ProfileError",
    "Store",
    "StructureError",
    "build_message",
    "queries",
]

__version__ = "1.0.0"
