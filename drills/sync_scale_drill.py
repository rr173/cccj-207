"""Runtime-generated long-history drill for the result subscription.

Generates a synthetic receiver history (default 200k committed messages,
~400k result rows) directly in SQL -- this is fixture generation, not the
code path under test -- then exercises the REAL pipeline on top of it:

1. the offline upgrade assigns durable positions to the whole history;
2. the analytics replica catches up from position 0 in bounded pages;
3. a few more messages are committed through the real ``Store`` (so new
   commits and their positions take effect together, at the tail);
4. a near-tail catch-up uses tiny pages (budget 1) right at the tail,
   including an all-non-matching page that must still advance.

Nothing is committed to the repository; everything is generated at run
time under the given workdir.  The drill reports scan-boundary evidence
(the query plan of the page scan and head-vs-tail fetch timings) and the
process peak RSS so callers can assert the 256 MiB ceiling.
"""

from __future__ import annotations

import json
import os
import resource
import sqlite3
import time

from hl7recv.builder import build_message
from hl7recv.store import Store
from hl7recv.sync import protocol, publish, replica

SENDERS = ["LAB1^HOSP", "MIC1^HOSP", "LAB2^HOSP", "IMG1^HOSP"]
SUBSCRIPTION = ["LAB1^HOSP", "LAB2^HOSP"]  # 2 of 4 senders are subscribed

_MSG_SQL = (
    "INSERT INTO messages(sender, control_id, version, msg_type, sent_at,"
    " received_at, sha256, raw, status) VALUES (?,?,?,?,?,?,?,?,?)"
)
_RES_SQL = (
    "INSERT INTO results(sender, control_id, obr_index, obx_index,"
    " patient_authority, patient_id, visit_id, placer_order, filler_order,"
    " test_code, test_system, test_name, obs_code, obs_system, obs_name,"
    " sub_id, value_type, value_text, units, ref_range, abnormal, rstatus,"
    " observed_at, reported_at, obr_seg, obx_seg, merged)"
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

# a real message committed through the Store at the tail (subscribed)
TAIL_MSG = (
    "MSH|^~\\&|LAB1|HOSP|RCVAPP|RCVFAC|20261102080000||ORU^R01|{cid}|P|2.5.1\r"
    "PID|1||P12345^^^HOSP^MR||Zhang^San\r"
    "PV1|1|O\r"
    "OBR|1|PL|FI|CBC^Complete Blood Count^LN\r"
    "OBX|1|NM|WBC^White Blood Cell Count^LN|1|{val}|10*9/L|4.0-10.0|N|||F\r"
)


def generate_history(db_path: str, n_messages: int, chunk: int = 5000) -> None:
    """Bulk-generate a committed-looking synthetic history (fixture only)."""
    store = Store(db_path)  # creates the receiver schema
    store.close()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA synchronous=OFF")  # fixture generation only
        msg_buf: list[tuple] = []
        res_buf: list[tuple] = []
        for i in range(1, n_messages + 1):
            sender = SENDERS[i % len(SENDERS)]
            cid = f"HIST{i:07d}"
            received = f"2026-11-01T08:{(i // 3600) % 24:02d}:{(i % 60):02d}.000+00:00"
            msg_buf.append((sender, cid, "2.5.1", "ORU^R01", None, received,
                            f"sha{i:064x}"[:64], b"synthetic", "committed"))
            for k in range(1, 1 + (i % 3) + 1):
                sub_id = (None, "", "1")[i % 3]
                units = ("mmol/L", None, "10*9/L")[i % 3]
                res_buf.append((
                    sender, cid, 1, k, "HOSP", "P12345", None, "PL", "FI",
                    "PANEL", "LN", None, f"AN{(i + k) % 50}", "LN", None,
                    sub_id, "NM", f"{i}.{k}", units, "1.0-9.9", "N", "F",
                    received, None, 4, 4 + k, 0,
                ))
            if len(msg_buf) >= chunk:
                conn.execute("BEGIN")
                conn.executemany(_MSG_SQL, msg_buf)
                conn.executemany(_RES_SQL, res_buf)
                conn.commit()
                msg_buf, res_buf = [], []
        if msg_buf:
            conn.execute("BEGIN")
            conn.executemany(_MSG_SQL, msg_buf)
            conn.executemany(_RES_SQL, res_buf)
            conn.commit()
    finally:
        conn.close()


def _page_scan_plan(db_path: str) -> str:
    conn = sqlite3.connect("file:" + db_path + "?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "EXPLAIN QUERY PLAN"
            " SELECT sp.seq, m.sender, m.control_id, m.received_at"
            " FROM sync_positions sp"
            " JOIN messages m ON m.sender=sp.sender AND m.control_id=sp.control_id"
            " WHERE sp.seq > 199990 AND sp.seq <= 200000"
            " ORDER BY sp.seq LIMIT 10"
        ).fetchall()
        return " | ".join(row[3] for row in rows)
    finally:
        conn.close()


def run_scale_drill(workdir: str, n_messages: int = 200_000,
                    page_budget: int = 5000) -> dict:
    os.makedirs(workdir, exist_ok=True)
    recv_db = os.path.join(workdir, "recv.db")
    rep_db = os.path.join(workdir, "replica.db")

    t0 = time.monotonic()
    generate_history(recv_db, n_messages)
    t_gen = time.monotonic() - t0

    # 1. offline upgrade over the whole history
    t0 = time.monotonic()
    meta = publish.upgrade(recv_db)
    t_upgrade = time.monotonic() - t0
    assert meta["bound"] == n_messages, meta

    # 2. full catch-up from 0 in bounded pages, up to a fixed watermark
    replica.init_replica(rep_db, source_id=meta["source_id"],
                         senders=SUBSCRIPTION)
    wm1 = publish.watermark(recv_db, SUBSCRIPTION)["bound"]
    t0 = time.monotonic()
    pages = 0
    cursor = 0
    while cursor < wm1:
        data = publish.fetch_page(recv_db, SUBSCRIPTION, start=cursor,
                                  bound=wm1, budget=page_budget)
        out = replica.import_page(rep_db, data)
        assert out["status"] == "applied"
        cursor = out["cursor"]
        pages += 1
    t_catchup = time.monotonic() - t0
    assert cursor == wm1

    # 3. real Store commits land at the tail WHILE the replica is caught up
    store = Store(recv_db)
    for i, cid in enumerate(("TAIL001", "TAIL002"), 1):
        payload = TAIL_MSG.format(cid=cid, val=f"9.{i}").encode("utf-8")
        store.commit_message(1000 + i, build_message(payload), payload)
    other = TAIL_MSG.replace("LAB1|HOSP", "MIC1|HOSP")
    other = other.replace("TAIL001", "TAIL003").replace("9.1", "8.8")
    store.commit_message(1003, build_message(other.encode("utf-8")), other.encode("utf-8"))
    store.close()
    bound = publish.watermark(recv_db, SUBSCRIPTION)["bound"]
    assert bound == wm1 + 3, bound

    # 4. near-tail catch-up with tiny pages (budget 1), incl. one
    #    all-non-matching page (TAIL003) that must still advance
    t0 = time.monotonic()
    tail_pages = 0
    while cursor < bound:
        data = publish.fetch_page(recv_db, SUBSCRIPTION, start=cursor,
                                  bound=bound, budget=1)
        out = replica.import_page(rep_db, data)
        assert out["status"] == "applied"
        cursor = out["cursor"]
        tail_pages += 1
    assert tail_pages == 3, tail_pages
    t_tail = time.monotonic() - t0

    # scan-boundary evidence: index range scan + head/tail fetch timings
    plan = _page_scan_plan(recv_db)
    t0 = time.monotonic()
    publish.fetch_page(recv_db, SUBSCRIPTION, start=10, bound=bound, budget=10)
    t_head = time.monotonic() - t0
    t0 = time.monotonic()
    publish.fetch_page(recv_db, SUBSCRIPTION, start=bound - 10, bound=bound,
                       budget=10)
    t_tailpage = time.monotonic() - t0

    # independent cross-check (plain SQL in this drill, not product code):
    # replica must hold exactly the subscribed prefix up to the cursor
    conn = sqlite3.connect("file:" + rep_db + "?mode=ro", uri=True)
    try:
        n_rep_msg = conn.execute("SELECT COUNT(*) FROM sub_messages").fetchone()[0]
        n_rep_res = conn.execute("SELECT COUNT(*) FROM sub_results").fetchone()[0]
        cur = int(conn.execute(
            "SELECT value FROM meta WHERE key='cursor'").fetchone()[0])
    finally:
        conn.close()
    conn = sqlite3.connect("file:" + recv_db + "?mode=ro", uri=True)
    try:
        n_src_msg = conn.execute(
            "SELECT COUNT(*) FROM sync_positions sp JOIN messages m"
            " ON m.sender=sp.sender AND m.control_id=sp.control_id"
            " WHERE sp.seq <= ? AND m.sender IN ('LAB1^HOSP','LAB2^HOSP')",
            (cur,),
        ).fetchone()[0]
        n_src_res = conn.execute(
            "SELECT COUNT(*) FROM results r JOIN sync_positions sp"
            " ON sp.sender=r.sender AND sp.control_id=r.control_id"
            " WHERE sp.seq <= ? AND r.sender IN ('LAB1^HOSP','LAB2^HOSP')",
            (cur,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert n_rep_msg == n_src_msg, (n_rep_msg, n_src_msg)
    assert n_rep_res == n_src_res, (n_rep_res, n_src_res)

    return {
        "messages": n_messages,
        "bound": bound,
        "pages": pages,
        "replica_messages": n_rep_msg,
        "replica_results": n_rep_res,
        "cursor": cur,
        "scan_plan": plan,
        "seconds": {
            "generate": round(t_gen, 2),
            "upgrade": round(t_upgrade, 2),
            "catchup": round(t_catchup, 2),
            "tail_drill": round(t_tail, 2),
            "head_page_fetch": round(t_head, 4),
            "tail_page_fetch": round(t_tailpage, 4),
        },
        "peak_rss_bytes":
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }


if __name__ == "__main__":
    import sys

    wd = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sync-scale"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
    print(json.dumps(run_scale_drill(wd, n), indent=2, sort_keys=True))
