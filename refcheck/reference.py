"""Independent reference checker.

This program is intentionally written **from scratch**: it does not import
or call any of the receiver's parsing or persistence code.  Given one or
more raw ORU^R01 message files it derives the expected fact rows
field-by-field and compares them against the receiver's SQLite database.

Usage:
    python -m refcheck.reference --db PATH FILE.hl7 [FILE2.hl7 ...]

Exit code 0 means every expected row, note and patient key matched the
database exactly (and no extra rows exist for those messages).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Independent minimal parser (reference implementation, stdlib only)
# ---------------------------------------------------------------------------
@dataclass
class RefNote:
    seg: int
    text: str


@dataclass
class RefResult:
    obr_index: int
    obx_index: int
    obr_seg: int
    obx_seg: int
    test_code: str
    test_system: str
    obs_code: str
    obs_system: str
    sub_id: str | None
    value_type: str
    value_text: str
    units: str | None
    ref_range: str | None
    abnormal: str | None
    rstatus: str
    observed_at: str | None
    reported_at: str | None
    placer_order: str | None
    filler_order: str | None
    notes: list[RefNote] = field(default_factory=list)


@dataclass
class RefMessage:
    sender: str
    control_id: str
    version: str
    patients: list[tuple[str, str]]
    visit_id: str | None
    results: list[RefResult]
    obr_notes: list[tuple[int, RefNote]]  # (obr_index, note)


def _unesc(s: str, fs: str, cmp: str, rep: str, esc: str, sub: str) -> str:
    if esc not in s:
        return s
    out, i = [], 0
    while i < len(s):
        if s[i] != esc:
            out.append(s[i])
            i += 1
            continue
        j = s.find(esc, i + 1)
        if j < 0:
            out.append(s[i:])
            break
        seq = s[i + 1 : j]
        out.append({"F": fs, "S": cmp, "R": rep, "E": esc, "T": sub}.get(seq, s[i : j + 1]))
        i = j + 1
    return "".join(out)


def _fld(seg: str, pos: int, fs: str) -> str | None:
    """1-based field text of a segment, None when missing."""
    parts = seg.split(fs)
    if seg.startswith("MSH"):
        if pos == 1:
            return fs
        idx = pos - 1  # MSH-2 is parts[1], MSH-n is parts[n-1]
    else:
        idx = pos
    if idx < len(parts):
        return parts[idx]
    return None


def _comp(field_text: str | None, n: int, cmp: str) -> str | None:
    if field_text is None:
        return None
    parts = field_text.split(cmp)
    if n - 1 < len(parts):
        return parts[n - 1]
    return None


def parse_reference(raw: bytes) -> RefMessage:
    text = raw.decode("utf-8")
    segs = [s.lstrip("\n") for s in text.split("\r") if s.strip("\n")]
    fs = segs[0][3]
    enc = _fld(segs[0], 2, fs) or "^~\\&"
    cmp, rep, esc, sub = enc[0], enc[1], enc[2], enc[3]

    def unesc(s: str) -> str:
        return _unesc(s, fs, cmp, rep, esc, sub)

    msh = segs[0]
    sender = f"{_fld(msh, 3, fs)}^{_fld(msh, 4, fs)}"
    control_id = _fld(msh, 10, fs) or ""
    version = _fld(msh, 12, fs) or ""

    patients: list[tuple[str, str]] = []
    visit_id = None
    results: list[RefResult] = []
    obr_notes: list[tuple[int, RefNote]] = []

    obr_idx = 0
    obx_idx = 0
    cur: dict = {}
    last_obx: RefResult | None = None
    last_kind = ""

    for idx, seg in enumerate(segs[1:], start=2):
        name = seg[:3]
        if name == "PID":
            pid3 = _fld(seg, 3, fs)
            for rep_text in (pid3 or "").split(rep):
                comps = rep_text.split(cmp)
                ident = comps[0] if comps else ""
                auth = comps[3] if len(comps) > 3 else ""
                patients.append((unesc(auth), unesc(ident)))
        elif name == "PV1":
            visit = _fld(seg, 19, fs)
            if visit:
                visit_id = unesc(visit.split(cmp)[0])
        elif name == "ORC":
            cur["orc_placer"] = _comp(_fld(seg, 2, fs), 1, cmp)
            cur["orc_filler"] = _comp(_fld(seg, 3, fs), 1, cmp)
        elif name == "OBR":
            obr_idx += 1
            obx_idx = 0
            last_obx = None
            svc = _fld(seg, 4, fs) or ""
            cur = {
                "obr_seg": idx,
                "test_code": unesc(_comp(svc, 1, cmp) or ""),
                "test_system": unesc(_comp(svc, 3, cmp) or ""),
                "placer": _comp(_fld(seg, 2, fs), 1, cmp) or cur.get("orc_placer"),
                "filler": _comp(_fld(seg, 3, fs), 1, cmp) or cur.get("orc_filler"),
                "obr7": _fld(seg, 7, fs),
                "obr22": _fld(seg, 22, fs),
            }
            last_kind = "OBR"
        elif name == "OBX":
            obx_idx += 1
            obs = _fld(seg, 3, fs) or ""
            units = _fld(seg, 6, fs)
            value = _fld(seg, 5, fs) or ""
            # re-join hierarchy with declared separators after unescaping
            value_text = rep.join(
                cmp.join(sub.join(unesc(s) for s in c.split(sub)) for c in r.split(cmp))
                for r in value.split(rep)
            )
            res = RefResult(
                obr_index=obr_idx,
                obx_index=obx_idx,
                obr_seg=cur["obr_seg"],
                obx_seg=idx,
                test_code=cur["test_code"],
                test_system=cur["test_system"],
                obs_code=unesc(_comp(obs, 1, cmp) or ""),
                obs_system=unesc(_comp(obs, 3, cmp) or ""),
                sub_id=_fld(seg, 4, fs),
                value_type=_fld(seg, 2, fs) or "",
                value_text=value_text,
                units=unesc(units) if units else None,
                ref_range=_fld(seg, 7, fs),
                abnormal=_fld(seg, 8, fs),
                rstatus=_fld(seg, 11, fs) or "",
                observed_at=_fld(seg, 14, fs) or cur.get("obr7"),
                reported_at=cur.get("obr22"),
                placer_order=cur.get("placer"),
                filler_order=cur.get("filler"),
            )
            results.append(res)
            last_obx = res
            last_kind = "OBX"
        elif name == "NTE":
            comment = _fld(seg, 3, fs) or ""
            note = RefNote(seg=idx, text=unesc(comment))
            if last_kind == "OBX" and last_obx is not None:
                last_obx.notes.append(note)
            else:
                obr_notes.append((obr_idx, note))
                last_kind = "OBR"

    return RefMessage(
        sender=sender,
        control_id=control_id,
        version=version,
        patients=patients,
        visit_id=visit_id,
        results=results,
        obr_notes=obr_notes,
    )


# ---------------------------------------------------------------------------
# Comparison against the receiver's database
# ---------------------------------------------------------------------------
COMPARE_COLS = (
    "obr_index", "obx_index", "obr_seg", "obx_seg",
    "placer_order", "filler_order", "test_code", "test_system",
    "obs_code", "obs_system", "sub_id", "value_type", "value_text",
    "units", "ref_range", "abnormal", "rstatus", "observed_at", "reported_at",
)


def check_message(db: sqlite3.Connection, ref: RefMessage) -> list[str]:
    problems: list[str] = []
    where = "WHERE sender=? AND control_id=?"
    args = (ref.sender, ref.control_id)

    msg = db.execute(
        f"SELECT * FROM messages {where}", args
    ).fetchone()
    if msg is None:
        return [f"{ref.control_id}: message not found in database"]

    rows = db.execute(
        f"SELECT * FROM results {where} ORDER BY obr_index, obx_index", args
    ).fetchall()
    if len(rows) != len(ref.results):
        problems.append(
            f"{ref.control_id}: expected {len(ref.results)} result rows, "
            f"found {len(rows)}"
        )
    for expected, actual in zip(ref.results, rows):
        for col in COMPARE_COLS:
            want = getattr(expected, col)
            got = actual[col]
            if want != got:
                problems.append(
                    f"{ref.control_id} OBR{expected.obr_index}/OBX{expected.obx_index}"
                    f" {col}: expected {want!r}, got {got!r}"
                )

    # patient keys
    pats = db.execute(
        f"SELECT authority, identifier FROM patients {where} ORDER BY seq", args
    ).fetchall()
    if [tuple(p) for p in pats] != ref.patients:
        problems.append(
            f"{ref.control_id}: patients expected {ref.patients}, "
            f"got {[tuple(p) for p in pats]}"
        )

    # notes
    notes = db.execute(
        f"SELECT obr_index, obx_index, seg, text FROM notes {where}"
        " ORDER BY obr_index, COALESCE(obx_index,0), seq",
        args,
    ).fetchall()
    expected_notes = [
        (r.obr_index, r.obx_index, n.seg, n.text)
        for r in ref.results
        for n in r.notes
    ] + [(obr, None, n.seg, n.text) for obr, n in ref.obr_notes]
    got_notes = [(n[0], n[1], n[2], n[3]) for n in notes]

    def _norm(note):
        obr, obx, seg, text = note
        return (obr, -1 if obx is None else obx, seg, text)

    if sorted(expected_notes, key=_norm) != sorted(got_notes, key=_norm):
        problems.append(
            f"{ref.control_id}: notes expected {sorted(expected_notes, key=_norm)}, "
            f"got {sorted(got_notes, key=_norm)}"
        )
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="refcheck")
    ap.add_argument("--db", required=True)
    ap.add_argument("files", nargs="+", help="raw ORU message files")
    args = ap.parse_args(argv)

    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    all_problems: list[str] = []
    for path in args.files:
        with open(path, "rb") as fh:
            raw = fh.read()
        ref = parse_reference(raw)
        problems = check_message(db, ref)
        if problems:
            all_problems.extend(problems)
            print(f"FAIL {path} ({ref.control_id}): {len(problems)} problem(s)")
            for p in problems:
                print(f"  - {p}")
        else:
            print(
                f"OK   {path} ({ref.control_id}): {len(ref.results)} result "
                f"rows, {len(ref.patients)} patient key(s) match"
            )
    db.close()
    if all_problems:
        print(f"REFERENCE CHECK FAILED: {len(all_problems)} problem(s)")
        return 1
    print("REFERENCE CHECK PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
