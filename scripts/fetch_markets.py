#!/usr/bin/env python
from certus.utils.pause_guard import guard_pause
guard_pause()

import os
import logging
# ... rest of your imports and logic
#!/usr/bin/env python3
"""
Certus Market Fetcher — Pro-ready, parallel, schema-safe, rich fields.
Pulls ~10k markets with extended columns.
"""
import time, asyncio
from typing import List, Tuple
import pandas as pd, duckdb
from certus.data.coingecko_client import CoinGeckoClient, CoinGeckoHTTPError

DB_PATH = "data/markets.duckdb"
TABLE   = "markets"

MAX_PAGES = 40      # 40 * 250 = ~10k
PER_PAGE  = 250
CONCURRENCY_PRO   = 20
CONCURRENCY_FREE  = 2

CANON_COLS: List[Tuple[str, str]] = [
    ("ts", "BIGINT"),
    ("id", "VARCHAR"),
    ("symbol", "VARCHAR"),
    ("name", "VARCHAR"),
    ("vs_currency", "VARCHAR"),
    ("price", "DOUBLE"),
    ("market_cap", "DOUBLE"),
    ("total_volume", "DOUBLE"),
    ("high_24h", "DOUBLE"),
    ("low_24h", "DOUBLE"),
    ("price_change_24h", "DOUBLE"),
    ("price_change_percentage_1h", "DOUBLE"),
    ("price_change_percentage_24h", "DOUBLE"),
    ("price_change_percentage_7d", "DOUBLE"),
    ("market_cap_change_24h", "DOUBLE"),
    ("market_cap_change_percentage_24h", "DOUBLE"),
    ("circulating_supply", "DOUBLE"),
    ("total_supply", "DOUBLE"),
    ("max_supply", "DOUBLE"),
    ("ath", "DOUBLE"),
    ("ath_change_percentage", "DOUBLE"),
    ("ath_date", "TIMESTAMP"),
    ("atl", "DOUBLE"),
    ("atl_change_percentage", "DOUBLE"),
    ("atl_date", "TIMESTAMP"),
    ("last_updated", "TIMESTAMP"),
    ("image", "VARCHAR"),
    ("roi_times", "DOUBLE"),
    ("roi_currency", "VARCHAR"),
    ("roi_percentage", "DOUBLE"),
]

def _rows_to_df(rows: List[dict]) -> pd.DataFrame:
    cols = [c for c, _ in CANON_COLS]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return df[cols]

async def fetch_page(c: CoinGeckoClient, page: int, ts_ms: int, vs_currency="usd") -> List[dict]:
    mkts = await c.coins_markets(
        vs_currency=vs_currency,
        per_page=PER_PAGE,
        page=page,
        price_change_percentage="1h,24h,7d",
        sparkline=False,
    )
    out = []
    for m in mkts:
        roi = m.get("roi") or {}
        out.append({
            "ts": ts_ms,
            "id": m.get("id"),
            "symbol": (m.get("symbol") or "").upper(),
            "name": m.get("name"),
            "vs_currency": vs_currency.upper(),
            "price": m.get("current_price"),
            "market_cap": m.get("market_cap"),
            "total_volume": m.get("total_volume"),
            "high_24h": m.get("high_24h"),
            "low_24h": m.get("low_24h"),
            "price_change_24h": m.get("price_change_24h"),
            "price_change_percentage_1h": m.get("price_change_percentage_1h_in_currency"),
            "price_change_percentage_24h": m.get("price_change_percentage_24h_in_currency") or m.get("price_change_percentage_24h"),
            "price_change_percentage_7d": m.get("price_change_percentage_7d_in_currency"),
            "market_cap_change_24h": m.get("market_cap_change_24h"),
            "market_cap_change_percentage_24h": m.get("market_cap_change_percentage_24h"),
            "circulating_supply": m.get("circulating_supply"),
            "total_supply": m.get("total_supply"),
            "max_supply": m.get("max_supply"),
            "ath": m.get("ath"),
            "ath_change_percentage": m.get("ath_change_percentage"),
            "ath_date": m.get("ath_date"),
            "atl": m.get("atl"),
            "atl_change_percentage": m.get("atl_change_percentage"),
            "atl_date": m.get("atl_date"),
            "last_updated": m.get("last_updated"),
            "image": m.get("image"),
            "roi_times": (roi.get("times") if isinstance(roi, dict) else None),
            "roi_currency": (roi.get("currency") if isinstance(roi, dict) else None),
            "roi_percentage": (roi.get("percentage") if isinstance(roi, dict) else None),
        })
    return out

def _ensure_table_schema(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT * FROM df LIMIT 0")
    existing = con.sql(f"PRAGMA table_info('{TABLE}')").fetchdf()
    existing_cols = set(existing["name"].tolist())
    for col, typ in CANON_COLS:
        if col not in existing_cols:
            con.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {typ}")

def _insert_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        print("[i] Nothing to insert.")
        return
    table_cols = con.sql(f"PRAGMA table_info('{TABLE}')").fetchdf()["name"].tolist()
    insert_cols = [c for c, _ in CANON_COLS if c in table_cols and c in df.columns]
    col_list = ", ".join(insert_cols)
    con.register("df", df[insert_cols])
    con.execute(f"INSERT INTO {TABLE} ({col_list}) SELECT {col_list} FROM df")

async def main():
    ts = int(time.time() * 1000)
    client = CoinGeckoClient()
    try:
        _ = await client.ping()
        _ = await client.coins_markets(vs_currency="usd", per_page=2, page=1)
        print(f"[OK] Connectivity via {'PRO' if client.is_pro else 'PUBLIC'} API.")
    except CoinGeckoHTTPError as e:
        print("[FATAL] Connectivity failed:", e)
        await client.close()
        return

    concurrency = 20 if client.is_pro else 2
    print(f"[Certus] Fetching {MAX_PAGES} pages x {PER_PAGE} (concurrency={concurrency})…")

    sem = asyncio.Semaphore(concurrency)
    results: List[List[dict]] = []

    async def worker(p: int):
        async with sem:
            try:
                rows = await fetch_page(client, p, ts)
                print(f"[+] Page {p}: {len(rows)} rows")
                results.append(rows)
            except CoinGeckoHTTPError as e:
                print(f"[ERR] Page {p}: {e}")

    tasks = [asyncio.create_task(worker(p)) for p in range(1, MAX_PAGES + 1)]
    await asyncio.gather(*tasks)
    await client.close()

    flat = [r for batch in results for r in batch]
    df = _rows_to_df(flat)
    print(f"[✔] Retrieved {len(df)} rows total.")

    con = duckdb.connect(DB_PATH)
    _ensure_table_schema(con, df)
    _insert_df(con, df)
    con.close()
    print("[✅] Market data saved successfully.")

if __name__ == "__main__":
    asyncio.run(main())
