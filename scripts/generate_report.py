#!/usr/bin/env python3
"""Generate a simple report from `api_canonical` and write CSV + markdown summary.
"""
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import csv

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "api_catalog.duckdb"
OUT = ROOT / "data" / "reports"
SNAP = ROOT / "snapshots"
OUT.mkdir(parents=True, exist_ok=True)
SNAP.mkdir(parents=True, exist_ok=True)


def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    conn = duckdb.connect(str(DB))
    # Ensure api_canonical exists
    try:
        total = conn.execute("SELECT COUNT(*) FROM api_canonical").fetchone()[0]
    except Exception as e:
        print("api_canonical table missing:", e)
        return

    rows = conn.execute("SELECT canonical_id, canonical_symbol, canonical_name, latest_price_usd, market_cap_usd FROM api_canonical ORDER BY canonical_symbol LIMIT 10000").fetchall()

    csv_path = OUT / f"canonical_snapshot_{ts}.csv"
    with csv_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["canonical_id", "symbol", "name", "price_usd", "market_cap_usd"])
        for r in rows:
            w.writerow(r)

    md = ROOT / "data" / "report.md"
    with md.open("w") as fh:
        fh.write(f"# Canonical snapshot {ts}\n\n")
        fh.write(f"Total canonical rows: {total}\n\n")
        fh.write(f"CSV snapshot: {csv_path}\n")

    # copy to snapshots
    snap_path = SNAP / f"canonical_snapshot_{ts}.csv"
    csv_path.replace(snap_path)

    print("Wrote report:", md)
    print("Wrote snapshot:", snap_path)


if __name__ == "__main__":
    main()
