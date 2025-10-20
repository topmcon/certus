import time
import pandas as pd
import asyncio

from certus.data.coingecko_client import CoinGeckoClient
from certus.storage.io import to_duckdb, to_parquet
from certus.config import SETTINGS  # if you have it; otherwise hardcode paths

async def fetch_markets_df(per_page: int = 25, page: int = 1) -> pd.DataFrame:
    client = CoinGeckoClient()
    try:
        mkts = await client.coins_markets(vs_currency="usd", per_page=per_page, page=page)
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
            "pct_change_24h": m.get("price_change_percentage_24h"),
            "pct_change_7d": m.get("price_change_percentage_7d_in_currency"),
        })
    return pd.DataFrame(rows)

def save_markets(df):
    to_duckdb(df, SETTINGS.duckdb_path, "cg_quotes")
    to_parquet(df, "data", "cg_quotes")  # <-- inside the function
    print(f"✅ Saved {len(df)} rows to DuckDB + Parquet")
