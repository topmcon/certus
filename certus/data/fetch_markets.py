"""
certus/data/fetch_markets.py
Fetch CoinGecko markets and save to Parquet + DuckDB.
"""

import os
import time
import asyncio
import duckdb
import pandas as pd
from typing import List, Dict, Any

from certus.data.coingecko_client import CoinGeckoClient
from certus.utils.logging import setup_logger

log = setup_logger("fetch_markets")

# ============== 1) Fetch ==============

async def fetch_markets_df(per_page: int = 25, page: int = 1) -> pd.DataFrame:
    client = CoinGeckoClient()
    try:
        raw: List[Dict[str, Any]] = await client.coins_markets(
            vs_currency="usd",
            per_page=per_page,
            page=page,
            price_change_percentage="1h,24h,7d",
        )
    finally:
        await client.close()

    ts = int(time.time() * 1000)
    rows = []
    for m in raw:
        rows.append({
            "ts": ts,
            "id": m.get("id"),
            "symbol": (m.get("symbol") or "").upper(),
            "name": m.get("name"),
            "vs_currency": "USD",
            "price": m.get("current_price"),
            "market_cap": m.get("market_cap"),
            "volume_24h": m.get("total_volume"),
            "pct_change_1h": m.get("price_change_percentage_1h_in_currency"),
            "pct_change_24h": m.get("price_change_percentage_24h_in_currency"),
            "pct_change_7d": m.get("price_change_percentage_7d_in_currency"),
        })

    df = pd.DataFrame.from_records(rows)
    log.info(f"Fetched {len(df)} rows from CoinGecko.")
    return df

# ============== 2) Save (no helper deps) ==============

def _ensure_dir(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def save_markets(
    df: pd.DataFrame,
    out_dir: str = "data",
    parquet_name: str = "markets_latest",
    duckdb_path: str = "data/markets.duckdb",
    duckdb_table: str = "markets",
):
    # Parquet
    _ensure_dir(out_dir)
    parquet_path = os.path.join(out_dir, f"{parquet_name}.parquet")
    df.to_parquet(parquet_path, index=False)
    log.info(f"[✔] Saved Parquet: {parquet_path}")

    # DuckDB
    _ensure_dir(os.path.dirname(duckdb_path))
    con = duckdb.connect(duckdb_path)
    try:
        con.register("df", df)
        con.execute(f"CREATE OR REPLACE TABLE {duckdb_table} AS SELECT * FROM df")
    finally:
        con.close()
    log.info(f"[✔] Updated DuckDB table '{duckdb_table}' → {duckdb_path}")

# ============== 3) Runner ==============

async def _run_once(per_page: int, page: int):
    df = await fetch_markets_df(per_page=per_page, page=page)
    if df.empty:
        log.warning("No data returned from CoinGecko.")
        return
    save_markets(df)
    log.info("✅ Market data saved successfully.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch CoinGecko markets and save locally.")
    parser.add_argument("--per-page", type=int, default=25, help="Number of records per page.")
    parser.add_argument("--page", type=int, default=1, help="Page number to fetch.")
    args = parser.parse_args()
    asyncio.run(_run_once(args.per_page, args.page))

if __name__ == "__main__":
    main()
