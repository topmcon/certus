#!/usr/bin/env python3
import os
import sys
import time
import argparse
import logging
from typing import List, Tuple

import duckdb
import pandas as pd
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s"
)

DB_PATH = "data/markets.duckdb"

def get_base_url() -> str:
    # Prefer Pro if key present; allow override
    base = os.getenv("COINGECKO_BASE_URL")
    if base:
        return base.rstrip("/")
    key = os.getenv("COINGECKO_API_KEY")
    if key:
        return "https://pro-api.coingecko.com/api/v3"
    return "https://api.coingecko.com/api/v3"

def get_headers() -> dict:
    headers = {"Accept": "application/json"}
    key = os.getenv("COINGECKO_API_KEY")
    if key:
        headers["x-cg-pro-api-key"] = key
    return headers

def get_target_universe(limit: int) -> List[Tuple[str, str]]:
    """
    Return up to `limit` coins as (id, symbol), using the latest snapshot rows in markets.
    """
    con = duckdb.connect(DB_PATH)
    q = """
    WITH latest AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
      FROM markets
    )
    SELECT id, symbol
    FROM latest
    WHERE rn = 1
    ORDER BY COALESCE(market_cap, 0) DESC
    LIMIT $limit
    """
    df = con.execute(q, {"limit": limit}).fetchdf()
    con.close()
    if df.empty:
        logging.error("No rows found in 'markets'. Run scripts/fetch_markets.py first.")
        sys.exit(1)
    # normalize symbol to upper for consistency
    return [(str(r["id"]), str(r["symbol"]).upper()) for _, r in df.iterrows()]

def fetch_market_chart(coin_id: str, vs_currency: str, days: str, interval: str) -> dict:
    base = get_base_url()
    url = f"{base}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,          # e.g., "90" or "max"
        "interval": interval,  # "daily" or "hourly"
    }
    headers = get_headers()

    # Simple retry loop
    for attempt in range(5):
        try:
            r = httpx.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                wait = 2 ** attempt
                logging.warning(f"429 rate limited for {coin_id}. Sleeping {wait}s and retrying...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            wait = 1.5 ** attempt
            logging.warning(f"HTTP error for {coin_id}: {e}. Retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch market_chart for {coin_id} after retries.")

def chart_to_df(coin_id: str, symbol: str, vs_currency: str, chart: dict) -> pd.DataFrame:
    """
    chart['prices'] is list of [ms, price]
    chart['market_caps'] is list of [ms, cap]
    chart['total_volumes'] is list of [ms, vol]
    """
    prices = chart.get("prices", [])
    caps = {ts: cap for ts, cap in chart.get("market_caps", [])}
    vols = {ts: vol for ts, vol in chart.get("total_volumes", [])}

    rows = []
    for ts, px in prices:
        rows.append({
            "ts": int(ts),             # ms
            "id": coin_id,
            "symbol": symbol,
            "vs_currency": vs_currency.upper(),
            "price": float(px) if px is not None else None,
            "market_cap": float(caps.get(ts)) if caps.get(ts) is not None else None,
            "total_volume": float(vols.get(ts)) if vols.get(ts) is not None else None,
            "source": "coingecko_market_chart"
        })
    return pd.DataFrame(rows)

def upsert_markets(df: pd.DataFrame):
    """
    Append-only insert skipping duplicates on (id, ts).
    """
    if df.empty:
        return

    con = duckdb.connect(DB_PATH)
    # Ensure table exists with expected schema
    con.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            ts BIGINT,
            id VARCHAR,
            symbol VARCHAR,
            vs_currency VARCHAR,
            price DOUBLE,
            market_cap DOUBLE,
            total_volume DOUBLE,
            pct_change_1h DOUBLE,
            pct_change_24h DOUBLE,
            pct_change_7d DOUBLE,
            source VARCHAR
        )
    """)
    # Bring df to same columns (missing pct_change_* will be NULL)
    for col in ["pct_change_1h", "pct_change_24h", "pct_change_7d"]:
        if col not in df.columns:
            df[col] = None

    con.register("staging_df", df)

    # Insert rows where (id, ts) doesn't already exist
    con.execute("""
        INSERT INTO markets
        SELECT s.*
        FROM staging_df s
        LEFT ANTI JOIN (SELECT id, ts FROM markets) m
        ON s.id = m.id AND s.ts = m.ts
    """)

    # Optional: compact/optimize
    con.close()

def main():
    parser = argparse.ArgumentParser(description="Backfill historical prices into DuckDB markets.")
    parser.add_argument("--days", default="90", help='Number of days (e.g. "30","90","365","max")')
    parser.add_argument("--interval", default="daily", choices=["daily","hourly"], help="Aggregation interval")
    parser.add_argument("--vs", default="usd", help="Quote currency (default: usd)")
    parser.add_argument("--limit", type=int, default=25, help="Max # of coins from existing markets universe")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests")
    args = parser.parse_args()

    universe = get_target_universe(args.limit)
    logging.info(f"Backfilling {len(universe)} coins — days={args.days}, interval={args.interval}, vs={args.vs}")

    all_rows = 0
    for i, (coin_id, symbol) in enumerate(universe, start=1):
        try:
            chart = fetch_market_chart(coin_id, args.vs, args.days, args.interval)
            df = chart_to_df(coin_id, symbol, args.vs, chart)
            upsert_markets(df)
            all_rows += len(df)
            logging.info(f"[{i}/{len(universe)}] {symbol:<8} ({coin_id}) → rows: {len(df)}")
        except Exception as e:
            logging.error(f"Failed {symbol} ({coin_id}): {e}")
        time.sleep(args.sleep)

    logging.info(f"[✔] Backfill complete. Total inserted rows (attempted): {all_rows}")

if __name__ == "__main__":
    main()
