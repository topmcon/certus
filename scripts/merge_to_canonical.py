#!/usr/bin/env python3
"""Merge api_catalog into a canonical normalized table usable for analysis.

Canonical schema (api_canonical):
  - canonical_id INTEGER AUTOINCREMENT
  - source TEXT
  - source_id TEXT
  - symbol TEXT
  - name TEXT
  - first_seen TIMESTAMP
  - last_seen TIMESTAMP
  - payload JSON / TEXT (raw_json)
  - last_snapshot TIMESTAMP

This script reads the deduped `api_catalog_dedup` if present, otherwise `api_catalog`.
It populates `api_canonical` by grouping source+source_id.
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import json

DB_PATH = Path("data/api_catalog.duckdb")


def merge():
    conn = duckdb.connect(str(DB_PATH))
    # Choose the source table
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    src = "api_catalog_dedup" if "api_catalog_dedup" in tables else "api_catalog"

    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_canonical (canonical_id INTEGER, source VARCHAR, source_id VARCHAR, symbol VARCHAR, name VARCHAR, first_seen TIMESTAMP, last_seen TIMESTAMP, payload VARCHAR, last_snapshot TIMESTAMP)"
    )

    # Build canonical rows by grouping
    # For CoinGecko entries: raw_json contains fields id, symbol, name
    # For CoinMarketCap entries: raw_json contains id (numeric), name, symbol
    # We'll parse raw_json and extract common fields where possible.
    rows = []
    q = f"SELECT source, item_id, name, raw_json, fetched_at FROM {src}"
    for source, item_id, name, raw_json, fetched_at in conn.execute(q).fetchall():
        symbol = None
        parsed = None
        try:
            parsed = json.loads(raw_json)
        except Exception:
            parsed = None

        if parsed:
            # Try common patterns
            if source == "coingecko":
                symbol = parsed.get("symbol")
                name = parsed.get("name") or name
            elif source == "coinmarketcap":
                # API returns numeric id and symbol/name inside object
                symbol = parsed.get("symbol")
                name = parsed.get("name") or name
        rows.append((source, item_id, symbol or "", name or "", fetched_at, fetched_at, raw_json, fetched_at))

    # Insert into a staging canonical table then upsert-like replace duplicates by source+source_id keeping last_snapshot
    conn.execute("CREATE TABLE IF NOT EXISTS api_canonical_staging AS SELECT * FROM api_canonical WHERE 1=0")
    conn.executemany(
        "INSERT INTO api_canonical_staging (source, source_id, symbol, name, first_seen, last_seen, payload, last_snapshot) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    # Merge: for simplicity, append then dedupe into api_canonical_final
    conn.execute("CREATE TABLE IF NOT EXISTS api_canonical_all AS SELECT * FROM api_canonical UNION ALL SELECT * FROM api_canonical_staging")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_canonical_agg AS \n"
        "SELECT source, source_id, symbol, name, MIN(first_seen) AS first_seen, MAX(last_seen) AS last_seen, MAX(last_snapshot) AS last_snapshot, ANY_VALUE(payload) AS payload\n"
        "FROM api_canonical_all GROUP BY source, source_id, symbol, name"
    )

    # Assign canonical_id based on latest snapshot
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_canonical_final AS \n"
        "SELECT ROW_NUMBER() OVER (ORDER BY last_snapshot DESC) AS canonical_id, source, source_id, symbol, name, first_seen, last_seen, payload, last_snapshot\n"
        "FROM api_canonical_agg"
    )

    # Replace api_canonical with final
    conn.execute("DROP TABLE IF EXISTS api_canonical")
    conn.execute("ALTER TABLE api_canonical_final RENAME TO api_canonical")
    # Clean staging
    conn.execute("DROP TABLE IF EXISTS api_canonical_staging")
    conn.execute("DROP TABLE IF EXISTS api_canonical_all")

    count = conn.execute("SELECT COUNT(*) FROM api_canonical").fetchone()[0]
    print(f"Canonical rows: {count}")
    conn.close()


if __name__ == "__main__":
    merge()
