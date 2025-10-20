from __future__ import annotations
import argparse, asyncio, time
import pandas as pd
from typing import List
from ..data.coingecko_client import CoinGeckoClient
from ..models.market import MarketQuote
from ..storage.io import to_parquet, to_duckdb
from ..config import SETTINGS

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, default="BTC,ETH,SOL", help="comma list of symbols to include (case-insensitive)")
    ap.add_argument("--vs", type=str, default="USD", help="vs currency (e.g., USD)")
    ap.add_argument("--top", type=int, default=50, help="fallback: number of top coins by mcap if ids not found")
    ap.add_argument("--dest", type=str, default="duckdb,parquet", help="destinations: duckdb,parquet")
    ap.add_argument("--table", type=str, default="cg_quotes", help="duckdb table name")
    return ap.parse_args()

async def _resolve_ids(client: CoinGeckoClient, symbols: List[str]) -> List[str]:
    listed = await client.coins_list(include_platform=True)
    sym_map = {}
    for c in listed:
        sym_map.setdefault(c['symbol'].upper(), []).append(c['id'])
    ids = []
    for s in symbols:
        if s.upper() in sym_map:
            # prefer the first, CoinGecko symbols are unique-ish but duplicates exist
            ids.append(sym_map[s.upper()][0])
    return ids

async def main():
    args = parse_args()
    client = CoinGeckoClient()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    ids = await _resolve_ids(client, symbols)
    vs = args.vs.upper()

    if not ids:
        # Fallback: take top markets by cap
        markets = await client.coins_markets(vs_currency=vs.lower(), per_page=args.top, page=1)
        ids = [m['id'] for m in markets]

    markets = await client.coins_markets(vs_currency=vs.lower(), ids=ids, per_page=min(len(ids), 250), page=1)
    ts_ms = int(time.time() * 1000)
    rows = []
    for m in markets:
        rows.append(MarketQuote(
            ts=ts_ms,
            id=m.get("id"),
            symbol=m.get("symbol", "").upper(),
            name=m.get("name"),
            vs_currency=vs,
            price=m.get("current_price"),
            market_cap=m.get("market_cap"),
            volume_24h=m.get("total_volume"),
            pct_change_24h=m.get("price_change_percentage_24h"),
        ).model_dump())

    df = pd.DataFrame(rows)
    if df.empty:
        print("No data returned."); return

    if "parquet" in args.dest:
        p = to_parquet(df, SETTINGS.data_dir, "coingecko_quotes")
        print(f"Wrote Parquet: {p}")
    if "duckdb" in args.dest:
        to_duckdb(df, SETTINGS.duckdb_path, args.table)
        print(f"Inserted into DuckDB: {SETTINGS.duckdb_path}::{args.table} ({len(df)} rows)")

if __name__ == "__main__":
    asyncio.run(main())
