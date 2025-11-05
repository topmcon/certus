#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import duckdb

DB = Path("data/api_catalog.duckdb")


def main():
    conn = duckdb.connect(str(DB))
    q = lambda sql: conn.execute(sql).fetchall()

    counts = q("SELECT source, COUNT(*) FROM api_catalog GROUP BY source ORDER BY COUNT(*) DESC")
    total = q("SELECT COUNT(*) FROM api_catalog")[0][0]
    canonical = q("SELECT COUNT(*) FROM api_canonical")[0][0]
    earliest = q("SELECT MIN(fetched_at) FROM api_catalog")[0][0]
    latest = q("SELECT MAX(fetched_at) FROM api_catalog")[0][0]
    sample_api = q("SELECT source,item_id,name,substr(raw_json,1,200) FROM api_catalog ORDER BY fetched_at DESC LIMIT 10")
    sample_canonical = q("SELECT canonical_id,source,source_id,symbol,name,last_snapshot FROM api_canonical ORDER BY last_snapshot DESC LIMIT 10")

    lines = []
    lines.append("# API Catalog Report\n")
    lines.append(f"Total rows in api_catalog: {total}")
    lines.append(f"Total canonical rows: {canonical}")
    lines.append(f"Earliest fetched_at: {earliest}")
    lines.append(f"Latest fetched_at: {latest}\n")
    lines.append("Source counts:")
    for s, c in counts:
        lines.append(f"- {s}: {c}")

    lines.append("\nLatest 10 rows in api_catalog:")
    for r in sample_api:
        sample = (r[3] or "").replace('\n', ' ').replace('\r', ' ')
        lines.append(f"- {r[0]} | {r[1]} | {r[2]} | {sample}")

    lines.append('\nTop 10 canonical rows:')
    for r in sample_canonical:
        lines.append(f"- {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]}")

    out = "\n".join(lines)
    Path('data/report.md').write_text(out)
    print(out)
    conn.close()


if __name__ == '__main__':
    main()
