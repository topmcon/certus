#!/usr/bin/env python3
"""Report ambiguous symbols where multiple distinct canonical_ids map to the same normalized symbol.

Writes `mappings/ambiguous_symbols.csv` with columns: symbol, canonical_ids (pipe-separated), sample_names
"""
import csv
from pathlib import Path
import json
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "api_catalog.duckdb"
OUT = ROOT / "mappings"
OUT.mkdir(parents=True, exist_ok=True)


def normalize_symbol(s: str) -> str:
    if not s:
        return ""
    s = s.strip().upper()
    for suf in ("-USD", ".B", "-B"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s


def main():
    conn = duckdb.connect(str(DB))
    # prefer api_canonical
    try:
        rows = conn.execute("SELECT canonical_id, canonical_symbol, canonical_name FROM api_canonical").fetchall()
    except Exception:
        rows = conn.execute("SELECT source, item_id, name, raw_json FROM api_catalog").fetchall()

    by_sym = {}
    for r in rows:
        cid, sym, name = r
        sym_norm = normalize_symbol(sym or "")
        by_sym.setdefault(sym_norm, set()).add(cid)

    out_path = OUT / "ambiguous_symbols.csv"
    with out_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "canonical_ids", "count"])
        for s, cids in sorted(by_sym.items()):
            if len(cids) > 1:
                w.writerow([s, "|".join(sorted(cids)), len(cids)])

    print("Wrote ambiguous report:", out_path)


if __name__ == "__main__":
    main()
