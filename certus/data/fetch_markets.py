# certus/data/fetch_markets.py
import time, asyncio
import pandas as pd
from .coingecko_client import CoinGeckoClient
from ..storage.io import to_parquet, to_duckdb

def validate_markets(df: pd.DataFrame) -> pd.DataFrame:
    need = ["id", "symbol", "name", "price", "market_cap", "volume_24h"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"Missing required columns: {miss}")
    return df

async def fetch_markets_df(per_page: int = 25, page: int = 1) -> pd.DataFrame:
    client = CoinGeckoClient()
    try:
        mkts = await client.coins_markets(
            vs_currency="usd", per_page=per_page, page=page,
            price_change_percentage="1h,24h,7d",
        )
    finally:
        await client.close()

    ts = int(time.time() * 1000)
    rows = []
    for m in mkts:
        rows.append({
            "ts": ts,
            "id": m["id"],
            "symbol": m["symbol"].upper(),
            "name": m["name"],
            "vs_currency": "USD",
            "price": m.get("current_price"),
            "market_cap": m.get("market_cap"),
            "volume_24h": m.get("total_volume"),
            "pct_change_1h": m.get("price_change_percentage_1h_in_currency"),
            "pct_change_24h": m.get("price_change_percentage_24h_in_currency"),
            "pct_change_7d": m.get("price_change_percentage_7d_in_currency"),
        })
    return pd.DataFrame(rows)

def save_markets(df: pd.DataFrame,
                 parquet_path: str = "data/markets.parquet",
                 duckdb_path: str = "data/markets.duckdb",
                 table: str = "markets") -> None:
    validate_markets(df)
    to_parquet(df, parquet_path)
    to_duckdb(df, duckdb_path, table)

def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Fetch CoinGecko markets and store them.")
    p.add_argument("--per-page", type=int, default=25)
    p.add_argument("--page", type=int, default=1)
    p.add_argument("--every", type=int, default=0, help="Seconds; 0 = run once.")
    return p.parse_args()

def _run_once(per_page: int, page: int):
    df = asyncio.run(fetch_markets_df(per_page=per_page, page=page))
    save_markets(df)
    print(f"Saved {len(df)} rows.")

def main():
    args = _parse_args()
    if args.every <= 0:
        _run_once(args.per_page, args.page); return
    import time as _t
    while True:
        _run_once(args.per_page, args.page)
        _t.sleep(args.every)
