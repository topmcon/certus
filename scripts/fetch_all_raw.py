#!/usr/bin/env python3
"""Fetch raw data from CoinGecko and CoinMarketCal and catalog into DuckDB.

Saves raw JSON files under `data/raw/` and writes per-item rows into
`data/api_catalog.duckdb` table `api_catalog` with columns:
  - source TEXT
  - item_id TEXT
  - name TEXT
  - raw_json TEXT
  - fetched_at TIMESTAMP

Run: set -a; source .env; set +a; python3 scripts/fetch_all_raw.py
"""
from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from certus.data.coingecko_client import CoinGeckoClient
from certus.data.coinmarketcap_client import CoinMarketCapClient


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path("data/api_catalog.duckdb")


async def fetch_coingecko_all(client: CoinGeckoClient, max_pages: int = 4) -> list:
    items = []
    for page in range(1, max_pages + 1):
        try:
            page_items = await client.coins_markets(per_page=250, page=page)
        except Exception as e:
            print("CoinGecko page", page, "failed:", e)
            break
        if not page_items:
            break
        items.extend(page_items)
        # stop early if a small page returned
        if len(page_items) < 250:
            break
    return items


async def fetch_coinmarketcap_all(client: CoinMarketCapClient, max_pages: int = 10) -> list:
    items = []
    # CoinMarketCap listings use start & limit; we'll page by `limit` offset
    limit = 100
    for page in range(0, max_pages):
        start = page * limit + 1
        try:
            page_items = await client.listings(start=start, limit=limit)
        except Exception as e:
            print("CoinMarketCap page", page + 1, "failed:", e)
            break
        if not page_items:
            break
        items.extend(page_items)
        # if fewer than requested, assume last page
        if len(page_items) < limit:
            break
    return items


def catalog_to_duckdb(rows: list[tuple[str, str, str, str, datetime]]):
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_catalog (source VARCHAR, item_id VARCHAR, name VARCHAR, raw_json VARCHAR, fetched_at TIMESTAMP)"
    )
    # Use a staging table and insert only rows that don't already exist by (source, item_id)
    conn.execute("CREATE TABLE IF NOT EXISTS api_catalog_staging (source VARCHAR, item_id VARCHAR, name VARCHAR, raw_json VARCHAR, fetched_at TIMESTAMP)")
    conn.executemany("INSERT INTO api_catalog_staging VALUES (?, ?, ?, ?, ?)", rows)
    conn.execute(
        "INSERT INTO api_catalog SELECT s.* FROM api_catalog_staging s LEFT JOIN api_catalog a ON s.source = a.source AND s.item_id = a.item_id WHERE a.source IS NULL"
    )
    conn.execute("DROP TABLE IF EXISTS api_catalog_staging")
    conn.close()


async def main(max_pages_cg: int = 4, max_pages_cmc: int = 10, do_insert: bool = True, dedupe_after: bool = False):
    # Use timezone-aware UTC timestamps
    fetched_at = datetime.now(timezone.utc)

    cg = CoinGeckoClient()
    cmc = CoinMarketCapClient()
    try:
        print("Fetching CoinGecko markets...")
        cg_items = await fetch_coingecko_all(cg, max_pages=max_pages_cg)
        print(f"Fetched {len(cg_items)} CoinGecko items")

        print("Fetching CoinMarketCap listings...")
        cmc_items = await fetch_coinmarketcap_all(cmc, max_pages=max_pages_cmc)
        print(f"Fetched {len(cmc_items)} CoinMarketCap items")
    finally:
        await cg.close()
        await cmc.close()

    # Save raw files
    cg_path = RAW_DIR / f"coingecko_markets_{int(fetched_at.timestamp())}.json"
    cmc_path = RAW_DIR / f"coinmarketcap_listings_{int(fetched_at.timestamp())}.json"
    cg_path.write_text(json.dumps(cg_items, indent=2))
    cmc_path.write_text(json.dumps(cmc_items, indent=2))
    print("Wrote raw files:", cg_path, cmc_path)

    # Prepare rows for duckdb
    rows = []
    for m in cg_items:
        item_id = str(m.get("id"))
        name = m.get("name") or ""
        rows.append(("coingecko", item_id, name, json.dumps(m), fetched_at))

    for e in cmc_items:
        item_id = str(e.get("id"))
        name = e.get("name") or e.get("slug") or ""
        rows.append(("coinmarketcap", item_id, name, json.dumps(e), fetched_at))

    if do_insert and rows:
        catalog_to_duckdb(rows)
        print(f"Inserted {len(rows)} rows into {DB_PATH}")
    else:
        print(f"Skipped inserting {len(rows)} rows (do_insert={do_insert})")

    # Print counts and a sample
    conn = duckdb.connect(str(DB_PATH))
    res = conn.execute("SELECT source, COUNT(*) AS cnt FROM api_catalog GROUP BY source").fetchall()
    print("Counts:")
    for r in res:
        print(r)
    sample = conn.execute("SELECT source, item_id, name, substr(raw_json,1,200) as sample FROM api_catalog ORDER BY fetched_at DESC LIMIT 10").fetchall()
    print("Sample rows:")
    for s in sample:
        print(s)
    conn.close()

    # Optionally run dedupe/merge steps
    if dedupe_after:
        try:
            from scripts.dedupe_api_catalog import dedupe

            print("Running dedupe to collapse duplicates (replace)...")
            dedupe(replace=True)
        except Exception as e:
            print("Dedupe failed:", e)


if __name__ == "__main__":
    asyncio.run(main())
