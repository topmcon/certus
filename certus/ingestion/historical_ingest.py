from __future__ import annotations
import argparse, asyncio, time, math
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd
from ..data.coingecko_client import CoinGeckoClient
from ..storage.io import to_parquet, to_duckdb
from ..config import SETTINGS

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, default="BTC,ETH,SOL", help="comma list of symbols to include")
    ap.add_argument("--vs", type=str, default="USD", help="vs currency (e.g., USD)")
    ap.add_argument("--start", type=str, required=True, help="start datetime ISO (e.g., 2024-01-01T00:00:00Z)")
    ap.add_argument("--end", type=str, required=True, help="end datetime ISO (e.g., 2024-12-31T23:59:59Z)")
    ap.add_argument("--granularity", type=str, default="auto", help="auto (CoinGecko decides)")
    ap.add_argument("--dest", type=str, default="duckdb,parquet", help="destinations: duckdb,parquet")
    ap.add_argument("--table", type=str, default="cg_prices", help="duckdb table name")
    return ap.parse_args()

async def _resolve_ids(client: CoinGeckoClient, symbols: List[str]) -> Dict[str,str]:
    listed = await client.coins_list(include_platform=True)
    sym_map = {}
    for c in listed:
        sym_map.setdefault(c['symbol'].upper(), []).append(c['id'])
    out = {}
    for s in symbols:
        if s.upper() in sym_map:
            out[s.upper()] = sym_map[s.upper()][0]
    return out

def _to_unix(s: str) -> int:
    dt = datetime.fromisoformat(s.replace("Z","+00:00"))
    return int(dt.timestamp())

async def fetch_range(symbols: List[str], vs: str, start: str, end: str):
    client = CoinGeckoClient()
    id_map = await _resolve_ids(client, symbols)
    frm = _to_unix(start); to = _to_unix(end)
    rows = []
    for sym, cid in id_map.items():
        data = await client.market_chart_range(cid, vs_currency=vs.lower(), frm_unix=frm, to_unix=to)
        # data['prices'] is list of [ts_ms, price]; market_caps, total_volumes similar
        caps = dict(data.get("market_caps", []))
        vols = dict(data.get("total_volumes", []))
        for ts_ms, price in data.get("prices", []):
            rows.append({
                "ts": int(ts_ms),
                "id": cid,
                "symbol": sym.upper(),
                "vs_currency": vs.upper(),
                "price": float(price) if price is not None else None,
                "market_cap": float(caps.get(ts_ms)) if caps.get(ts_ms) is not None else None,
                "volume": float(vols.get(ts_ms)) if vols.get(ts_ms) is not None else None
            })
    return pd.DataFrame(rows)

def main():
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    df = asyncio.run(fetch_range(symbols, args.vs, args.start, args.end))
    if df.empty:
        print("No data returned for the range."); return
    if "parquet" in args.dest:
        p = to_parquet(df, SETTINGS.data_dir, "coingecko_prices")
        print(f"Wrote Parquet: {p}")
    if "duckdb" in args.dest:
        to_duckdb(df, SETTINGS.duckdb_path, args.table)
        print(f"Inserted into DuckDB: {SETTINGS.duckdb_path}::{args.table} ({len(df)} rows)")

if __name__ == "__main__":
    main()
