#!/usr/bin/env python3
"""Small sample ingestion: fetch a tiny markets sample and write to DuckDB.

Writes a table `sample_markets` into `data/markets.duckdb`.
"""
from __future__ import annotations
import asyncio
import duckdb
from pathlib import Path

from certus.data.coingecko_client import CoinGeckoClient


DB_PATH = Path("data/markets.duckdb")


async def main():
    client = CoinGeckoClient()
    try:
        markets = await client.coins_markets(per_page=5, page=1)
    finally:
        await client.close()

    # Normalize simple table: id, symbol, name, current_price
    rows = []
    for m in markets:
        rows.append((m.get("id"), m.get("symbol"), m.get("name"), m.get("current_price")))

    conn = duckdb.connect(str(DB_PATH))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sample_markets (id VARCHAR, symbol VARCHAR, name VARCHAR, current_price DOUBLE)"
    )
    conn.executemany("INSERT INTO sample_markets VALUES (?, ?, ?, ?)", rows)
    conn.close()
    print(f"Wrote {len(rows)} rows to {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
