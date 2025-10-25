#!/usr/bin/env python3
"""
Backfill historical CoinGecko prices into DuckDB `markets` table.
This version uses the current MARKETS_SCHEMA and logs every major step.
"""

import os
import time
import argparse
import logging
from typing import List, Tuple

import duckdb
import pandas as pd
import httpx
from certus.storage.schema import MARKETS_SCHEMA  # unified schema

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
DB_PATH = "data/markets.duckdb"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def get_base_url() -> str:
    base = os.getenv("COINGECKO_BASE_URL")
    if base:
        return base.rstrip("/")
    key = os.getenv("COINGECKO_API_KEY")
    return "https://pro-api.coingecko.com/api/v3" if key else "https://api.coingecko.com/api/v3"


def get_headers() -> dict:
    headers = {"Accept": "application/json"}
    key = os.getenv("COINGECKO_API_KEY")
    if key:
        headers["x-cg-pro-api-key"] = key
    return headers


# ---------------------------------------------------------------------
# Coin selection and data fetch
# ---------------------------------------------------------------------
def get_target_universe(limit: int) -> List[Tuple[str, str]]:
    con = duckdb.connect(DB_PATH)
    con.execute(MARKETS_SCHEMA)
    q = """
    SELECT DISTINCT id, symbol
    FROM markets
    ORDER BY id
    LIMIT $limit
    """
    df = con.execute(q, {"limit": limit}).fetchdf()
    con.close()

    if df.empty:
        logging.warning("No existing IDs found in markets. Defaulting to top coins.")
        return [("bitcoin", "BTC"), ("ethereum", "ETH"), ("tether", "USDT")]

    return [(str(r["id"]), str(r["symbol"]).upper()) for _, r in df.iterrows()]


def fetch_market_chart(coin_id: str, vs_currency: str, days: str, interval: str) -> dict:
    base = get_base_url()
    url = f"{base}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": interval}
    headers = get_headers()
    r = httpx.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def chart_to_df(coin_id: str, symbol: str, vs_currency: str, chart: dict) -> pd.DataFrame:
    prices = chart.get("prices", [])
    caps = {ts: cap for ts, cap in chart.get("market_caps", [])}
    vols = {ts: vol for ts, vol in chart.get("total_volumes", [])}

    rows = []
    for ts, px in prices:
        rows.append({
            "ts": int(ts),
            "id": coin_id,
            "symbol": symbol,
            "vs_currency": vs_currency.upper(),
            "price": float(px),
            "market_cap": float(caps.get(ts)) if ts in caps else None,
            "total_volume": float(vols.get(ts)) if ts in vols else None,
            "source": "coingecko_market_chart",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Insert logic
# ---------------------------------------------------------------------
def upsert_markets(df: pd.DataFrame):
    if df.empty:
        logging.warning("[SKIP] Empty DataFrame — nothing to insert.")
        return
    con = duckdb.connect(DB_PATH)
    con.execute(MARKETS_SCHEMA)
    con.register("staging_df", df)
    con.execute("""
        INSERT INTO markets
        SELECT s.*
        FROM staging_df s
        WHERE NOT EXISTS (
            SELECT 1 FROM markets m
            WHERE s.id = m.id AND s.ts = m.ts
        )
    """)
    inserted = len(df)
    con.close()
    logging.info(f"[✔] Inserted {inserted} rows for {df['id'].iloc[0]}.")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Backfill CoinGecko historical prices.")
    parser.add_argument("--days", default="90", help='Number of days ("30","90","365","max")')
    parser.add_argument("--interval", default="daily", choices=["daily", "hourly"])
    parser.add_argument("--vs", default="usd")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=0.4)
    args = parser.parse_args()

    base = get_base_url()
    key_set = bool(os.getenv("COINGECKO_API_KEY"))
    logging.info(f"Using base: {base} | key_set={key_set}")

    universe = get_target_universe(args.limit)
    logging.info(f"Backfilling {len(universe)} coins — days={args.days}, interval={args.interval}")

    total_inserted = 0
    for i, (coin_id, symbol) in enumerate(universe, start=1):
        try:
            chart = fetch_market_chart(coin_id, args.vs, args.days, args.interval)
            df = chart_to_df(coin_id, symbol, args.vs, chart)
            logging.info(f"[{i}/{len(universe)}] {symbol:<6} → fetched {len(df)} rows.")
            upsert_markets(df)
            total_inserted += len(df)
        except Exception as e:
            logging.error(f"[{i}/{len(universe)}] Failed {symbol} ({coin_id}): {e}")
        time.sleep(args.sleep)

    logging.info(f"[✔] Backfill complete. Total rows attempted: {total_inserted}")


if __name__ == "__main__":
    main()
