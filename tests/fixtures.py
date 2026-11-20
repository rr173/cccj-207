"""Handwritten ORU^R01 fixtures used by the acceptance suite.

MSG1 (v2.5.1): two OBR groups, several OBX with different data types
(NM, ST, CWE), one NTE attached to the OBR, one NTE attached to an OBX.

MSG2 (v2.3.1): same patient and same order as MSG1, different message
control ID; uses CE-typed OBX (CWE does not exist in v2.3.1).

Every expected database value in the acceptance test is derived by hand
from these two messages.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Message 1 - v2.5.1, sender LAB1^HOSP, control id MSG00001
# ---------------------------------------------------------------------------
MSG1 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG00001|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San||19800101|M\r"
    "PV1|1|O|WARD1^^^^^C10||||||||||||||||V2026-0001\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN|||20261101075900|||||||||||||||20261101082900\r"
    "NTE|1||fasting sample for OBR one\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F|||20261101081000\r"
    "OBX|2|ST|WBCM^Morphology Comment^LN|1|No abnormal cells seen|||N|||F|||20261101081100\r"
    "NTE|1||comment belonging to OBX 2\r"
    "OBR|2|PLACER200|FILLER200|GLUP^Glucose Panel^LN|||20261101080000|||||||||||||||20261101082800\r"
    "OBX|1|CWE|GLUCM^Glucose Method^LN|1|POCT^Point of Care^HL7OBS|||N|||F|||20261101081500\r"
    "OBX|2|NM|GLU^Glucose^LN|1|5.5|mmol/L|3.9-6.1|H|||F|||20261101081600\r"
).encode("utf-8")

# ---------------------------------------------------------------------------
# Message 2 - v2.3.1, same patient (HOSP, P12345) and same order FILLER100,
# different control id MSG00002.  CE value type (legal in 2.3.1).
# ---------------------------------------------------------------------------
MSG2 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101090000||ORU^R01|MSG00002|P|2.3.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San||19800101|M\r"
    "PV1|1|O|WARD1^^^^^C10||||||||||||||||V2026-0001\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN|||20261102075900\r"
    "OBX|1|CE|RBCF^Red Cell Flag^LN|1|A^Abnormal^HL7OBS|||A|||P|||20261102081000\r"
    "OBX|2|NM|RBC^Red Blood Cell Count^LN|1|4.2|10*12/L|3.5-5.5|N|||F|||20261102081100\r"
).encode("utf-8")

# Same control id as MSG1 but different bytes -> identity conflict.
MSG1_CONFLICT = MSG1.replace(b"|7.5|", b"|7.6|")
assert MSG1_CONFLICT != MSG1

# Unknown profile version.
MSG_UNKNOWN_VERSION = MSG1.replace(b"|2.5.1\r", b"|9.9\r").replace(
    b"MSG00001", b"MSG0000V"
)

# CWE value type inside a v2.3.1 message -> not allowed by that profile.
MSG_CWE_IN_231 = (
    MSG2.replace(b"MSG00002", b"MSG0000C")
    .replace(b"|CE|RBCF", b"|CWE|RBCF")
)

# Invalid OBX: OBX-11 status missing -> whole message rejected, zero rows.
MSG_INVALID_OBX = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000B|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San||19800101|M\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N||||||20261101081000\r"
).encode("utf-8")

# Unsupported message type.
MSG_BAD_TYPE = MSG1.replace(b"ORU^R01", b"ADT^A01").replace(b"MSG00001", b"MSG0000T")

# Non-UTF-8 payload -> decode error.
MSG_UNDECODABLE = (
    b"MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000D|P|2.5.1\r"
    b"PID|1||P12345^^^HOSP^MR||\xff\xfeInvalid\r"
)

# Different field separator (*) and encoding characters (!@#$).  The units
# contain the field separator and must be escaped with the declared escape
# character: 10#F#9/L -> logical "10*9/L".
MSG_CUSTOM_SEP = (
    "MSH*!@#$*LAB1*HOSP*RCVAPP*RCVFAC*20261101083000**ORU!R01*MSG0000S*P*2.5.1\r"
    "PID*1**P12345!!!HOSP!MR**Zhang!San\r"
    "PV1*1*O\r"
    "OBR*1*PLACER100*FILLER100*CBC!Complete Blood Count!LN\r"
    "OBX*1*NM*WBC!White Blood Cell Count!LN*1*7.5*10#F#9/L*4.0-10.0*N***F\r"
).encode("utf-8")

# Escape sequences: OBX-5 holds "A|B^C~D\E&F" logically (escapes \F\ \S\ \R\ \E\ \T\).
MSG_ESCAPES = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000E|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|ST|NOTE^Note Text^LN|1|A\\F\\B\\S\\C\\R\\D\\E\\E\\T\\F|||N|||F\r"
).encode("utf-8")

# Repeating PID-3: two patient identifiers; key must be (authority, id).
MSG_REPEAT_PID3 = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000R|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR~P999^^^OTHER^PI||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
).encode("utf-8")

# Empty vs missing: OBX-7 present-but-empty (""), OBX-14 missing entirely
# (segment ends after OBX-11) so observed_at is NULL.
MSG_EMPTY_VS_MISSING = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000M|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|||||F\r"
).encode("utf-8")

# In-message duplicate result keys: identical payload -> merge allowed.
MSG_DUP_MERGE = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000G|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
    "OBX|2|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
).encode("utf-8")

# In-message duplicate result keys with conflicting payload -> reject.
MSG_DUP_CONFLICT = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|MSG0000H|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PLACER100|FILLER100|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|7.5|10*9/L|4.0-10.0|N|||F\r"
    "OBX|2|NM|WBC^White Blood Cell Count^LN|1|9.9|10*9/L|4.0-10.0|N|||F\r"
).encode("utf-8")


def make_big_message(n_obx: int, control_id: str = "MSGBIG") -> bytes:
    """Runtime-generated stress message (never committed to git as data)."""
    parts = [
        "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261101083000||ORU^R01|"
        f"{control_id}|P|2.5.1\r",
        "PID|1||P12345^^^HOSP^MR||Zhang^San\r",
        "PV1|1|O\r",
        "OBR|1|PLACER100|FILLER100|PANEL^Big Panel^LN\r",
    ]
    for i in range(1, n_obx + 1):
        parts.append(
            f"OBX|{i}|NM|AN{i}^Analyte {i}^LN|1|{i}.5|mmol/L|1.0-9.9|N|||F|||2026110108{i % 60:02d}00\r"
        )
    return "".join(parts).encode("utf-8")
