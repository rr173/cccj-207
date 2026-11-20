"""Independent reference checker for the handover ledger.

This module deliberately imports **no** ``shiftledger`` reconciliation code.
Given the same three inputs the ledger saw -- the manifest directory, the
receiver database (read-only) and a cut-off -- it independently re-derives,
by explicit row-by-row enumeration:

* every group's attribution and its exact reason;
* every candidate / potential matching edge;
* every item's status (including on-time vs late write-off);
* every observation's single disposition;

then compares them, item by item and observation by observation, against the
stored immutable version.  It is built for small hand-derived data sets and
holds them in plain Python sets, so a mismatch report names the exact id and
the differing field rather than only an aggregate count.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone


def _instant(text: str) -> int:
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    assert dt.tzinfo is not None
    dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp()) * 1_000_000 + dt.microsecond


def _read_jsonl(path: str) -> list[dict]:
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def derive(manifest_dir: str, receiver_db: str, cutoff: str,
           active_assign: dict | None = None,
           active_locks: dict | None = None) -> dict:
    """Independently derive the expected reconciliation from raw inputs."""
    active_assign = dict(active_assign or {})
    active_locks = dict(active_locks or {})

    meta = json.load(open(os.path.join(manifest_dir, "manifest.json"),
                          encoding="utf-8"))
    shift_start = _instant(meta["shift_start"])
    cutoff_us = _instant(cutoff)
    senders = set(meta["senders"])

    wos = {w["work_order_id"]: (w["patient_domain"], w["patient_id"])
           for w in _read_jsonl(os.path.join(manifest_dir, "work_orders.jsonl"))}
    items = {}
    for it in _read_jsonl(os.path.join(manifest_dir, "items.jsonl")):
        items[it["item_id"]] = {
            "wo": it["work_order_id"], "sys": it["code_system"],
            "code": it["code"], "sub": it.get("sub_id"),
            "due": _instant(it["due_at"])}
    mappings = _read_jsonl(os.path.join(manifest_dir, "mappings.jsonl"))

    # Read receiver facts directly (read-only via query_only), no product or
    # ledger reconciliation code.
    rc = sqlite3.connect(receiver_db)
    rc.row_factory = sqlite3.Row
    rc.execute("PRAGMA query_only=ON")
    msgs = rc.execute(
        "SELECT sender, control_id, received_at FROM messages"
    ).fetchall()
    window = {(m["sender"], m["control_id"]): _instant(m["received_at"])
              for m in msgs if m["sender"] in senders
              and shift_start <= _instant(m["received_at"]) < cutoff_us}

    # Build groups with their orders, patient and observations.
    groups: dict[str, dict] = {}
    rrows = rc.execute(
        "SELECT sender, control_id, obr_index, obx_index, patient_authority,"
        " patient_id, placer_order, filler_order, obs_system, obs_code, sub_id"
        " FROM results ORDER BY sender, control_id, obr_index, obx_index"
    ).fetchall()
    for r in rrows:
        key = (r["sender"], r["control_id"])
        if key not in window:
            continue
        gk = "\x1f".join(str(x) for x in (r["sender"], r["control_id"],
                                          r["obr_index"]))
        g = groups.setdefault(gk, {
            "received": window[key], "patient": None, "numbers": {},
            "obs": []})
        if r["patient_authority"] and r["patient_id"]:
            g["patient"] = (r["patient_authority"], r["patient_id"])
        if r["placer_order"]:
            g["numbers"]["placer"] = r["placer_order"]
        if r["filler_order"]:
            g["numbers"]["filler"] = r["filler_order"]
        g["obs"].append({
            "obs_key": f"{gk}\x1f{r['obx_index']}", "sys": r["obs_system"],
            "code": r["obs_code"], "sub": r["sub_id"]})
    rc.close()

    def effective(sender, nclass, number, at):
        out = set()
        for mp in mappings:
            if (mp["sender"] == sender and mp["number_class"] == nclass
                    and mp["local_number"] == number
                    and _instant(mp["valid_from"]) <= at
                    < _instant(mp["valid_to"])):
                out.add(mp["work_order_id"])
        return out

    # ---- attribution, enumerated explicitly ---------------------------
    verdicts: dict[str, dict] = {}
    for gk, g in groups.items():
        sender = gk.split("\x1f")[0]
        if g["patient"] is None:
            verdicts[gk] = {"status": "no_patient_identity", "wo": None}
            continue
        if not g["numbers"]:
            verdicts[gk] = {"status": "no_order_number", "wo": None}
            continue
        per_number = []
        for nclass, number in sorted(g["numbers"].items()):
            woset = effective(sender, nclass, number, g["received"])
            per_number.append((nclass, number, woset))
        if any(not w for _, _, w in per_number):
            if all(w or True for _, _, w in per_number):
                # distinguish not-effective vs never-registered
                status = "unregistered_order"
                for nclass, number, w in per_number:
                    if not w:
                        ever = any(mp["sender"] == sender
                                   and mp["number_class"] == nclass
                                   and mp["local_number"] == number
                                   for mp in mappings)
                        if ever:
                            status = "out_of_window"
                verdicts[gk] = {"status": status, "wo": None}
                continue
        cand = set(per_number[0][2])
        for _, _, w in per_number[1:]:
            cand &= w
        if not cand:
            verdicts[gk] = {"status": "mapping_conflict", "wo": None}
            continue
        matching = {w for w in cand if wos.get(w) == g["patient"]}
        if not matching:
            verdicts[gk] = {"status": "patient_mismatch", "wo": None}
            continue
        if gk in active_assign:
            assert active_assign[gk] in matching
            verdicts[gk] = {"status": "assigned", "wo": active_assign[gk],
                            "decided": True, "candidates": matching}
        elif len(matching) == 1:
            verdicts[gk] = {"status": "assigned",
                            "wo": next(iter(matching)), "decided": False,
                            "candidates": matching}
        else:
            verdicts[gk] = {"status": "ambiguous", "wo": None,
                            "candidates": matching}

    # ---- edges ----------------------------------------------------------
    def item_matches(wo, ob):
        return [iid for iid, it in items.items()
                if it["wo"] == wo and it["sys"] == ob["sys"]
                and it["code"] == ob["code"] and it["sub"] == ob["sub"]]

    cand = set()          # (obs_key, item_id)
    potential = set()     # (obs_key, item_id)
    for gk, v in verdicts.items():
        for ob in groups[gk]["obs"]:
            if v["status"] == "assigned":
                for iid in item_matches(v["wo"], ob):
                    cand.add((ob["obs_key"], iid))
            elif v["status"] == "ambiguous":
                for wo in v["candidates"]:
                    for iid in item_matches(wo, ob):
                        potential.add((ob["obs_key"], iid))

    # ---- locks then unique 1:1 auto ------------------------------------
    adopted = {}  # obs -> (item, kind)
    item_owner = {}
    for obs, iid in active_locks.items():
        assert (obs, iid) in cand
        adopted[obs] = (iid, "lock")
        item_owner[iid] = obs

    def free_cand_lists():
        obs_items: dict[str, set] = {}
        item_obs: dict[str, set] = {}
        for o, i in cand:
            if o in adopted or i in item_owner:
                continue
            obs_items.setdefault(o, set()).add(i)
            item_obs.setdefault(i, set()).add(o)
        return obs_items, item_obs

    blocked_items = {i for _, i in potential if i not in item_owner}
    while True:
        obs_items, item_obs = free_cand_lists()
        pair = None
        for o, ilist in sorted(obs_items.items()):
            ilist = {i for i in ilist if i in item_obs}
            if len(ilist) == 1:
                (i,) = ilist
                if i in blocked_items:
                    continue
                if item_obs.get(i) == {o}:
                    pair = (o, i)
                    break
        if pair is None:
            break
        o, i = pair
        adopted[o] = (i, "auto")
        item_owner[i] = o

    # ---- item statuses --------------------------------------------------
    item_status = {}
    item_obs = {}
    timeliness = {}
    grp_received = {gk: g["received"] for gk, g in groups.items()}
    for iid, it in items.items():
        if iid in item_owner:
            o = item_owner[iid]
            kind = adopted[o][1]
            item_status[iid] = "lock_settled" if kind == "lock" else "auto_settled"
            item_obs[iid] = o
            gk = "\x1f".join(o.split("\x1f")[:3])
            timeliness[iid] = ("on_time" if grp_received[gk] <= it["due"]
                               else "late")
            continue
        has_cand = any(i == iid and o not in adopted
                       for o, i in cand)
        pots = [o for o, i in potential if i == iid]
        if has_cand or pots:
            item_status[iid] = "pending_decision"
        elif cutoff_us <= it["due"]:
            item_status[iid] = "pending_receipt"
        else:
            item_status[iid] = "overdue"

    # ---- observation dispositions --------------------------------------
    disp = {}
    for gk, g in groups.items():
        v = verdicts[gk]
        for ob in g["obs"]:
            o = ob["obs_key"]
            if o in adopted:
                disp[o] = "adopted"
            elif v["status"] == "ambiguous":
                disp[o] = "group_pending"
            elif v["status"] != "assigned":
                disp[o] = "unattributable"
            else:
                matched = [i for oo, i in cand if oo == o]
                if not matched:
                    disp[o] = "no_matching_item"
                elif all(i in item_owner for i in matched):
                    disp[o] = "matching_items_occupied"
                else:
                    disp[o] = "candidate_unassigned"

    return {
        "groups": verdicts, "items": item_status, "item_obs": item_obs,
        "timeliness": timeliness, "dispositions": disp,
        "candidate_edges": sorted(cand), "potential_edges": sorted(potential),
        "adopted": {o: i for o, (i, _) in adopted.items()},
    }


def compare_version(ledger_db: str, ledger_id: str, version_id: int,
                    manifest_dir: str, receiver_db: str, cutoff: str,
                    active_assign: dict | None = None,
                    active_locks: dict | None = None) -> list[str]:
    """Return a list of human-readable mismatches (empty == exact agreement)."""
    expected = derive(manifest_dir, receiver_db, cutoff,
                      active_assign, active_locks)
    lc = sqlite3.connect(ledger_db)
    lc.row_factory = sqlite3.Row
    lc.execute("PRAGMA query_only=ON")
    problems: list[str] = []

    # groups
    got_groups = {r["group_key"]: r for r in lc.execute(
        "SELECT group_key, status, work_order_id FROM version_groups"
        " WHERE version_id=?", (version_id,))}
    if set(got_groups) != set(expected["groups"]):
        problems.append(
            f"group set mismatch: missing={set(expected['groups']) - set(got_groups)}"
            f" extra={set(got_groups) - set(expected['groups'])}")
    for gk, ev in expected["groups"].items():
        if gk not in got_groups:
            continue
        if got_groups[gk]["status"] != ev["status"]:
            problems.append(
                f"group {gk}: status {got_groups[gk]['status']!r} !="
                f" {ev['status']!r}")
        if got_groups[gk]["work_order_id"] != ev["wo"]:
            problems.append(
                f"group {gk}: wo {got_groups[gk]['work_order_id']!r} !="
                f" {ev['wo']!r}")

    # items
    got_items = {r["item_id"]: r for r in lc.execute(
        "SELECT item_id, status, obs_key, timeliness FROM version_items"
        " WHERE version_id=?", (version_id,))}
    if set(got_items) != set(expected["items"]):
        problems.append("item set mismatch")
    for iid, estatus in expected["items"].items():
        if iid not in got_items:
            continue
        g = got_items[iid]
        if g["status"] != estatus:
            problems.append(
                f"item {iid}: status {g['status']!r} != {estatus!r}")
        if estatus in ("auto_settled", "lock_settled"):
            if g["obs_key"] != expected["item_obs"][iid]:
                problems.append(f"item {iid}: adopted obs mismatch")
            if g["timeliness"] != expected["timeliness"][iid]:
                problems.append(f"item {iid}: timeliness mismatch")

    # observations: exactly one disposition each, exact match
    got_disp = {r["obs_key"]: r["disposition"] for r in lc.execute(
        "SELECT obs_key, disposition FROM version_obs WHERE version_id=?",
        (version_id,))}
    if set(got_disp) != set(expected["dispositions"]):
        problems.append("observation set mismatch")
    for ok, ed in expected["dispositions"].items():
        if got_disp.get(ok) != ed:
            problems.append(f"obs {ok}: {got_disp.get(ok)!r} != {ed!r}")

    # every observation has exactly one row
    n = lc.execute(
        "SELECT COUNT(*) FROM version_obs WHERE version_id=?",
        (version_id,)).fetchone()[0]
    n_distinct = lc.execute(
        "SELECT COUNT(DISTINCT obs_key) FROM version_obs WHERE version_id=?",
        (version_id,)).fetchone()[0]
    if n != n_distinct:
        problems.append("an observation has more than one disposition row")
    lc.close()
    return problems
