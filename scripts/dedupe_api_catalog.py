#!/usr/bin/env python3
"""Dedupe the api_catalog table in data/api_catalog.duckdb.

Keeps the most recent row per (source, item_id) based on fetched_at.
Creates a new table `api_catalog_dedup` and optionally replaces the original.
"""
from __future__ import annotations
from pathlib import Path
import duckdb

DB_PATH = Path("data/api_catalog.duckdb")


def dedupe(replace: bool = False):
    conn = duckdb.connect(str(DB_PATH))
    # Ensure table exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_catalog (source VARCHAR, item_id VARCHAR, name VARCHAR, raw_json VARCHAR, fetched_at TIMESTAMP)"
    )

    # Create deduped table using window function
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_catalog_dedup AS \n"
        "SELECT source, item_id, name, raw_json, fetched_at FROM (\n"
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY source, item_id ORDER BY fetched_at DESC) AS rn \n"
        "  FROM api_catalog\n"
        ") t WHERE rn = 1"
    )

    before = conn.execute("SELECT COUNT(*) FROM api_catalog").fetchone()[0]
    after = conn.execute("SELECT COUNT(*) FROM api_catalog_dedup").fetchone()[0]
    print(f"Rows before: {before}, after dedupe: {after}")

    if replace:
        conn.execute("DROP TABLE api_catalog")
        conn.execute("ALTER TABLE api_catalog_dedup RENAME TO api_catalog")
        print("Replaced api_catalog with deduped table")

    conn.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace api_catalog with deduped table")
    args = p.parse_args()
    dedupe(replace=args.replace)
