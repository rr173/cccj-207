"""Runtime-generated synthetic fixtures for handover reconciliation drills.

Everything is synthetic: no clinical values are interpreted, result value
text is fixed filler, and all data is generated at run time so no large data
set is committed.  Two builders are provided:

* :func:`build_acceptance_fixture` -- the three-work-order handover scenario
  from the acceptance story, plus edge cases (two items in one group, equal
  sub-id candidate competition, wrong/unmapped numbers, number-segment switch
  on the mapping boundary);
* :func:`build_scale_fixture` -- up to 200k receivable items and 500k
  observations for the bounded-memory run.

The receiver database uses the real product schema (created through
``hl7recv.store.Store``) but rows are inserted directly as already-settled
facts; the ledger only ever reads ``messages`` / ``results``.
"""

from __future__ import annotations

import json
import os

from hl7recv.store import Store

# Fixed synthetic timeline (explicit offsets).
T0 = "2026-11-20T08:00:00+08:00"      # shift start
T1 = "2026-11-20T09:00:00+08:00"      # early arrival
T2 = "2026-11-20T10:00:00+08:00"      # arrival
T3 = "2026-11-20T11:00:00+08:00"      # initial cutoff
T4 = "2026-11-20T12:30:00+08:00"      # late X for WO1
T5 = "2026-11-20T13:00:00+08:00"      # extended cutoff
DUE_EARLY = "2026-11-20T12:00:00+08:00"
DUE_LATE = "2026-11-20T09:30:00+08:00"

SENDER = "ANALYZER-A"
DOMAIN = "HOSP"
PATIENT = "P0001"
PATIENT_OTHER = "P0009"

LOINC = "LN"
CODE_X = "X"
CODE_Y = "Y"
CODE_Z = "Z"


# ---------------------------------------------------------------------------
# Low-level receiver fact writer (settled facts only)
# ---------------------------------------------------------------------------
def _insert_message(conn, sender: str, control_id: str, received_at: str) -> None:
    conn.execute(
        "INSERT INTO messages(sender, control_id, version, msg_type, sent_at,"
        " received_at, sha256, raw, status) VALUES (?,?,?,?,?,?,?,?,?)",
        (sender, control_id, "2.5.1", "ORU^R01", received_at, received_at,
         f"sha-{control_id}", b"SYNTHETIC", "committed"),
    )


def _insert_group(conn, sender: str, control_id: str, obr_index: int,
                  patient: tuple[str, str], placer: str | None,
                  filler: str | None, observations: list[tuple],
                  received_at: str) -> None:
    """observations: list of (obx_index, obs_system, obs_code, sub_id)."""
    _insert_message(conn, sender, control_id, received_at)
    p_domain, p_id = patient
    for obx_index, system, code, sub_id in observations:
        conn.execute(
            "INSERT INTO results(sender, control_id, obr_index, obx_index,"
            " patient_authority, patient_id, visit_id, placer_order,"
            " filler_order, test_code, test_system, test_name, obs_code,"
            " obs_system, obs_name, sub_id, value_type, value_text, units,"
            " ref_range, abnormal, rstatus, observed_at, reported_at,"
            " obr_seg, obx_seg, merged)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sender, control_id, obr_index, obx_index, p_domain, p_id, None,
             placer, filler, code, system, f"NAME-{code}", code, system,
             f"NAME-{code}", sub_id, "ST", "SYNTHETIC-VALUE", None, None,
             None, "F", received_at, received_at, 1, 1, 0),
        )


def new_receiver(path: str) -> Store:
    return Store(path, recover=False)


# ---------------------------------------------------------------------------
# Manifest writer
# ---------------------------------------------------------------------------
def write_manifest(manifest_dir: str, doc: dict, work_orders: list[dict],
                   items: list[dict], mappings: list[dict]) -> None:
    os.makedirs(manifest_dir, exist_ok=True)
    with open(os.path.join(manifest_dir, "manifest.json"), "w",
              encoding="utf-8") as fh:
        json.dump(doc, fh, sort_keys=True)
    for fname, rows in (
        ("work_orders.jsonl", work_orders),
        ("items.jsonl", items),
        ("mappings.jsonl", mappings),
    ):
        with open(os.path.join(manifest_dir, fname), "w",
                  encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, sort_keys=True) + "\n")


# ---------------------------------------------------------------------------
# Acceptance fixture
# ---------------------------------------------------------------------------
def build_acceptance_fixture(workdir: str) -> dict:
    """Create the receiver DB and manifest for the handover acceptance story.

    Layout (all three work orders share one patient except where noted):

    * WO1 / WO2 both expect an X; a shared number L-SHARED maps to both and a
      dedicated L-ONE maps only to WO1.  Two X observations arrive: one via
      the shared number (ambiguous group), one via the dedicated number.
    * WO3 expects a Z that never arrives; one Z item is still within its due
      time at the initial cutoff (pending_receipt) and one is already past
      due (overdue).
    * Extra edge groups cover a two-item group, equal sub-id competition, an
      unregistered number, a patient mismatch and a mapping window switch.
    """
    recv_path = os.path.join(workdir, "receiver.db")
    manifest_dir = os.path.join(workdir, "manifest")
    store = new_receiver(recv_path)
    conn = store._conn

    # Ambiguous group: shared number, one X.  Both placer and filler present.
    _insert_group(
        conn, SENDER, "MSG-SHARED", 1, (DOMAIN, PATIENT),
        "L-SHARED", "F-SHARED",
        [(1, LOINC, CODE_X, None)], T2,
    )
    # Dedicated group: maps only to WO1, one X (on time).
    _insert_group(
        conn, SENDER, "MSG-ONE", 1, (DOMAIN, PATIENT),
        "L-ONE", "F-ONE",
        [(1, LOINC, CODE_X, None)], T1,
    )
    # A group carrying two observations that settle two items of one WO (WO1).
    _insert_group(
        conn, SENDER, "MSG-TWO", 1, (DOMAIN, PATIENT),
        "L-TWO", None,
        [(1, LOINC, CODE_Y, None), (2, LOINC, CODE_Z, "S1")], T1,
    )
    # Equal sub-id competition: two observations match the same sub-id item.
    _insert_group(
        conn, SENDER, "MSG-COMP", 1, (DOMAIN, PATIENT),
        "L-COMP", None,
        [(1, LOINC, CODE_Y, "DUP"), (2, LOINC, CODE_Y, "DUP")], T1,
    )
    # Unregistered number: no mapping at all.
    _insert_group(
        conn, SENDER, "MSG-UNREG", 1, (DOMAIN, PATIENT),
        "L-UNKNOWN", None,
        [(1, LOINC, CODE_X, None)], T1,
    )
    # Patient mismatch: number maps to WO1 but patient differs.
    _insert_group(
        conn, SENDER, "MSG-MISMATCH", 1, (DOMAIN, PATIENT_OTHER),
        "L-ONE", None,
        [(1, LOINC, CODE_X, None)], T1,
    )
    # Number-segment switch: L-SEG maps to WO1 before T2, WO2 from T2 on;
    # this message arrives at T2 (exclusive end of the first window).  It
    # carries a distinct code W so the switch only tests attribution timing.
    _insert_group(
        conn, SENDER, "MSG-SEG", 1, (DOMAIN, PATIENT),
        "L-SEG", None,
        [(1, LOINC, "W", None)], T2,
    )
    conn.commit()
    store.close()

    doc = {
        "shift_id": "SHIFT-1",
        "shift_start": T0,
        "cutoff": T3,
        "senders": [SENDER],
    }
    work_orders = [
        {"work_order_id": "WO1", "patient_domain": DOMAIN, "patient_id": PATIENT},
        {"work_order_id": "WO2", "patient_domain": DOMAIN, "patient_id": PATIENT},
        {"work_order_id": "WO3", "patient_domain": DOMAIN, "patient_id": PATIENT},
    ]
    items = [
        # the two competing X items from the story
        {"item_id": "WO1-X", "work_order_id": "WO1", "code_system": LOINC,
         "code": CODE_X, "sub_id": None, "due_at": DUE_EARLY},
        {"item_id": "WO2-X", "work_order_id": "WO2", "code_system": LOINC,
         "code": CODE_X, "sub_id": None, "due_at": DUE_EARLY},
        # two items settled by the two observations of one group (WO1)
        {"item_id": "WO1-Y", "work_order_id": "WO1", "code_system": LOINC,
         "code": CODE_Y, "sub_id": None, "due_at": DUE_EARLY},
        {"item_id": "WO1-ZS1", "work_order_id": "WO1", "code_system": LOINC,
         "code": CODE_Z, "sub_id": "S1", "due_at": DUE_EARLY},
        # equal sub-id competition target (WO2)
        {"item_id": "WO2-YDUP", "work_order_id": "WO2", "code_system": LOINC,
         "code": CODE_Y, "sub_id": "DUP", "due_at": DUE_EARLY},
        # recipient of the segment-switch group after it moves to WO2 at T2
        {"item_id": "WO2-W", "work_order_id": "WO2", "code_system": LOINC,
         "code": "W", "sub_id": None, "due_at": DUE_EARLY},
        # never-returned Z: one still due, one overdue at T3
        {"item_id": "WO3-Z-OPEN", "work_order_id": "WO3", "code_system": LOINC,
         "code": CODE_Z, "sub_id": None, "due_at": DUE_EARLY},
        {"item_id": "WO3-Z-LATE", "work_order_id": "WO3", "code_system": LOINC,
         "code": CODE_Z, "sub_id": None, "due_at": DUE_LATE},
    ]
    mappings = [
        # shared number -> both WO1 and WO2 at T2; number can point to several WOs
        {"sender": SENDER, "number_class": "placer", "local_number": "L-SHARED",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T5},
        {"sender": SENDER, "number_class": "placer", "local_number": "L-SHARED",
         "work_order_id": "WO2", "valid_from": T0, "valid_to": T5},
        {"sender": SENDER, "number_class": "filler", "local_number": "F-SHARED",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T5},
        {"sender": SENDER, "number_class": "filler", "local_number": "F-SHARED",
         "work_order_id": "WO2", "valid_from": T0, "valid_to": T5},
        # dedicated number -> WO1 only
        {"sender": SENDER, "number_class": "placer", "local_number": "L-ONE",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T5},
        {"sender": SENDER, "number_class": "filler", "local_number": "F-ONE",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T5},
        # two-item group number -> WO1
        {"sender": SENDER, "number_class": "placer", "local_number": "L-TWO",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T5},
        # competition group -> WO2
        {"sender": SENDER, "number_class": "placer", "local_number": "L-COMP",
         "work_order_id": "WO2", "valid_from": T0, "valid_to": T5},
        # segment switch: WO1 [T0,T2), WO2 [T2,T5); message at T2 -> WO2
        {"sender": SENDER, "number_class": "placer", "local_number": "L-SEG",
         "work_order_id": "WO1", "valid_from": T0, "valid_to": T2},
        {"sender": SENDER, "number_class": "placer", "local_number": "L-SEG",
         "work_order_id": "WO2", "valid_from": T2, "valid_to": T5},
    ]
    write_manifest(manifest_dir, doc, work_orders, items, mappings)
    return {
        "receiver_db": recv_path,
        "manifest_dir": manifest_dir,
        "ledger_db": os.path.join(workdir, "ledger.db"),
        "keys": {
            "shared_group": f"{SENDER}\x1fMSG-SHARED\x1f1",
            "one_group": f"{SENDER}\x1fMSG-ONE\x1f1",
            "seg_group": f"{SENDER}\x1fMSG-SEG\x1f1",
            "shared_x_obs": f"{SENDER}\x1fMSG-SHARED\x1f1\x1f1",
            "one_x_obs": f"{SENDER}\x1fMSG-ONE\x1f1\x1f1",
        },
    }


def add_late_x_for_wo1(receiver_path: str) -> str:
    """After the initial cutoff, a dedicated late X for WO1 arrives (T4)."""
    store = Store(receiver_path, recover=False)
    conn = store._conn
    _insert_group(
        conn, SENDER, "MSG-LATE", 1, (DOMAIN, PATIENT),
        "L-ONE", None,
        [(1, LOINC, CODE_X, None)], T4,
    )
    conn.commit()
    store.close()
    return f"{SENDER}\x1fMSG-LATE\x1f1\x1f1"


# ---------------------------------------------------------------------------
# Scale fixture
# ---------------------------------------------------------------------------
def build_scale_fixture(workdir: str, n_items: int = 200_000,
                        n_observations: int = 500_000) -> dict:
    """Large synthetic ledger: streaming manifest + direct batched facts.

    When ``n_observations`` exceeds ``n_items`` the surplus is split into two
    deterministic injections so every state is present at scale:

    * extra observations on the first ~2% of items (candidate competition ->
      ``pending_decision`` / ``candidate_unassigned``);
    * the same number of later items are left without any observation
      (``pending_receipt`` / ``overdue``);
    * the remaining items keep a clean unique 1:1 match and auto-settle.
    """
    recv_path = os.path.join(workdir, "receiver.db")
    manifest_dir = os.path.join(workdir, "manifest")
    os.makedirs(manifest_dir, exist_ok=True)
    store = new_receiver(recv_path)
    conn = store._conn

    # Manifest: n_work_orders work orders, items spread across them.  Each
    # item gets a globally unique (work_order, code) pair so a matching
    # observation forms a clean 1:1 relation; a small injected share of items
    # gets two competing observations, and a small share gets none, so every
    # disposition and item status is exercised at scale.
    n_wo = max(1, n_items // 100)
    items_per_wo = n_items // n_wo
    doc = {"shift_id": "SHIFT-SCALE", "shift_start": T0, "cutoff": T3,
           "senders": [SENDER]}
    with open(os.path.join(manifest_dir, "manifest.json"), "w",
              encoding="utf-8") as fh:
        json.dump(doc, fh)
    with open(os.path.join(manifest_dir, "work_orders.jsonl"), "w",
              encoding="utf-8") as fh:
        for w in range(n_wo):
            wid = f"WO{w:06d}"
            fh.write(json.dumps({
                "work_order_id": wid, "patient_domain": DOMAIN,
                "patient_id": f"P{w % 1000:04d}"}) + "\n")

    def code_for(w: int, k: int) -> str:
        # Unique per work order: high bits carry the work order, low the slot.
        return f"C{w:04d}K{k:03d}"

    with open(os.path.join(manifest_dir, "items.jsonl"), "w",
              encoding="utf-8") as fh:
        for i in range(n_items):
            w = i % n_wo
            k = (i // n_wo) % items_per_wo
            wid = f"WO{w:06d}"
            fh.write(json.dumps({
                "item_id": f"IT{i:07d}", "work_order_id": wid,
                "code_system": LOINC, "code": code_for(w, k),
                "sub_id": None, "due_at": DUE_EARLY}) + "\n")
    # One mapping per work order; each observation group carries that number.
    with open(os.path.join(manifest_dir, "mappings.jsonl"), "w",
              encoding="utf-8") as fh:
        for w in range(n_wo):
            fh.write(json.dumps({
                "sender": SENDER, "number_class": "placer",
                "local_number": f"L{w:06d}", "work_order_id": f"WO{w:06d}",
                "valid_from": T0, "valid_to": T5}) + "\n")

    # Facts: each observation is its own one-group message with a globally
    # unique control id.  We emit one observation per covered item, then all
    # surplus observations as duplicates on the earliest items (candidate
    # competition).  Items beyond the covered prefix receive nothing
    # (pending_receipt / overdue); everything covered exactly once is a clean
    # unique 1:1 match that auto-settles.
    n_covered = min(n_items, n_observations)
    n_extra = n_observations - n_covered
    conn.execute("BEGIN")
    _FLUSH = 5000
    msg_buf: list[tuple] = []
    row_buf: list[tuple] = []
    emitted = 0
    _msg_sql = (
        "INSERT INTO messages(sender, control_id, version, msg_type,"
        " sent_at, received_at, sha256, raw, status)"
        " VALUES (?,?,?,?,?,?,?,?,?)")
    _res_sql = (
        "INSERT INTO results(sender, control_id, obr_index, obx_index,"
        " patient_authority, patient_id, visit_id, placer_order,"
        " filler_order, test_code, test_system, test_name, obs_code,"
        " obs_system, obs_name, sub_id, value_type, value_text, units,"
        " ref_range, abnormal, rstatus, observed_at, reported_at,"
        " obr_seg, obx_seg, merged)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    def _flush() -> None:
        if not row_buf:
            return
        conn.executemany(_msg_sql, msg_buf)
        conn.executemany(_res_sql, row_buf)
        msg_buf.clear()
        row_buf.clear()

    def emit(msg_no: int, w: int, code: str) -> None:
        nonlocal emitted
        cid = f"M{msg_no:08d}"
        msg_buf.append((
            SENDER, cid, "2.5.1", "ORU^R01", T1, T1,
            f"sha-{cid}", b"SYNTHETIC", "committed"))
        row_buf.append((
            SENDER, cid, 1, 1, DOMAIN,
            f"P{w % 1000:04d}", None, f"L{w:06d}", None,
            code, LOINC, None, code, LOINC,
            None, None, "ST", "SYNTHETIC-VALUE", None, None, None, "F",
            T1, T1, 1, 1, 0,
        ))
        emitted += 1
        if len(row_buf) >= _FLUSH:
            _flush()

    msg_no = 0
    # one observation per covered item
    for i in range(n_covered):
        msg_no += 1
        w = i % n_wo
        k = (i // n_wo) % items_per_wo
        emit(msg_no, w, code_for(w, k))
    # all surplus observations become competing duplicates on early items
    for j in range(n_extra):
        msg_no += 1
        i = j % max(1, n_covered)
        w = i % n_wo
        k = (i // n_wo) % items_per_wo
        emit(msg_no, w, code_for(w, k))

    _flush()
    assert emitted == n_observations, (emitted, n_observations)
    conn.commit()
    store.close()
    return {
        "receiver_db": recv_path,
        "manifest_dir": manifest_dir,
        "ledger_db": os.path.join(workdir, "ledger.db"),
        "n_items": n_items, "n_observations": n_observations,
        "n_work_orders": n_wo,
    }
