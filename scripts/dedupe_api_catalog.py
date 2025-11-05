#!/usr/bin/env python3
"""Dedupe rows in data/api_catalog.duckdb keeping the latest row per (source, item_id).

Creates/updates table `api_catalog_dedup` with columns matching `api_catalog`.
If `--replace` is passed to the CLI or replace=True in function, it will replace `api_catalog` with the deduped set.
"""
from __future__ import annotations
from typing import Optional
from pathlib import Path
import duckdb

DB_PATH = Path("data/api_catalog.duckdb")


def dedupe(replace: bool = False) -> None:
    conn = duckdb.connect(str(DB_PATH))
    # Ensure table exists
    conn.execute("CREATE TABLE IF NOT EXISTS api_catalog (source VARCHAR, item_id VARCHAR, name VARCHAR, raw_json VARCHAR, fetched_at TIMESTAMP)")
    # Create deduped table: keep the row with max(fetched_at) per source,item_id
    conn.execute("CREATE TABLE IF NOT EXISTS api_catalog_dedup AS SELECT a.* FROM api_catalog a JOIN (SELECT source, item_id, MAX(fetched_at) AS fetched_at FROM api_catalog GROUP BY source, item_id) mx ON a.source = mx.source AND a.item_id = mx.item_id AND a.fetched_at = mx.fetched_at")
    # Remove exact duplicates if any
    conn.execute("CREATE TABLE IF NOT EXISTS api_catalog_dedup2 AS SELECT DISTINCT source, item_id, name, raw_json, fetched_at FROM api_catalog_dedup")
    conn.execute("DROP TABLE IF EXISTS api_catalog_dedup")
    conn.execute("ALTER TABLE api_catalog_dedup2 RENAME TO api_catalog_dedup")

    if replace:
        # Replace original table
        conn.execute("CREATE TABLE IF NOT EXISTS api_catalog_new AS SELECT * FROM api_catalog_dedup")
        conn.execute("DROP TABLE IF EXISTS api_catalog")
        conn.execute("ALTER TABLE api_catalog_new RENAME TO api_catalog")
    conn.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace api_catalog with deduped rows")
    args = p.parse_args()
    dedupe(replace=args.replace)
