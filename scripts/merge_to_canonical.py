#!/usr/bin/env python3
"""Merge deduped api_catalog rows into an `api_canonical` table in DuckDB.

Logic:
- Read `api_catalog_dedup` (fall back to `api_catalog` if dedup table missing).
- Use `mappings/symbol_to_cg.csv` to map symbols -> coingecko_id.
- For rows from CoinGecko, canonical_id = `cg:{coingecko_id}`.
- For CoinMarketCap rows, try to map by symbol to CoinGecko id; else canonical_id = `cmc:{item_id}`.
- Insert/update `api_canonical` with fields: canonical_id, canonical_symbol, canonical_name, latest_price_usd, market_cap_usd, last_seen_utc, source_ids (JSON), sources_meta (JSON), extra (JSON).
"""
from __future__ import annotations
import csv
import json
from pathlib import Path
from datetime import datetime
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "api_catalog.duckdb"
MAP_PATH = ROOT / "mappings" / "symbol_to_cg.csv"


def load_symbol_map() -> dict:
    m = {}
    if MAP_PATH.exists():
        with MAP_PATH.open() as fh:
            r = csv.DictReader(fh)
            for row in r:
                sym = (row.get("symbol") or "").upper()
                cid = row.get("coingecko_id")
                if sym and cid:
                    m[sym] = cid
    return m


def _extract_price(row_json: str) -> float | None:
    try:
        d = json.loads(row_json)
        # CoinGecko uses current_price
        if isinstance(d, dict) and "current_price" in d:
            return float(d.get("current_price") or 0.0)
        # CoinMarketCap nested in quote->USD->price
        if isinstance(d, dict) and "quote" in d:
            usd = d.get("quote", {}).get("USD")
            if usd and "price" in usd:
                return float(usd.get("price") or 0.0)
        # fallback
        for k in ("price", "last_price"):
            if k in d:
                return float(d.get(k) or 0.0)
    except Exception:
        pass
    return None


def _extract_market_cap(row_json: str) -> float | None:
    try:
        d = json.loads(row_json)
        if isinstance(d, dict) and "market_cap" in d:
            return float(d.get("market_cap") or 0.0)
        if isinstance(d, dict) and "quote" in d:
            usd = d.get("quote", {}).get("USD")
            if usd and "market_cap" in usd:
                return float(usd.get("market_cap") or 0.0)
    except Exception:
        pass
    return None


def merge():
    conn = duckdb.connect(str(DB_PATH))
    # prefer dedup table
    tables = {t[0] for t in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
    src_table = "api_catalog_dedup" if "api_catalog_dedup" in tables else "api_catalog"

    # create canonical table if missing
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_canonical (canonical_id VARCHAR, canonical_symbol VARCHAR, canonical_name VARCHAR, latest_price_usd DOUBLE, market_cap_usd DOUBLE, last_seen_utc TIMESTAMP, source_ids VARCHAR, sources_meta VARCHAR, extra VARCHAR)"
    )

    symbol_map = load_symbol_map()

    rows = conn.execute(f"SELECT source, item_id, name, raw_json, fetched_at FROM {src_table}").fetchall()

    to_upsert = []
    for r in rows:
        source, item_id, name, raw_json, fetched_at = r
        fetched_at = fetched_at if isinstance(fetched_at, datetime) else None
        # parse raw_json for symbol
        sym = None
        try:
            data = json.loads(raw_json)
            sym = (data.get("symbol") or data.get("symbol", "") or data.get("slug") or "")
        except Exception:
            data = None
        sym_norm = (sym or "").upper()

        canonical_id = None
        canonical_symbol = None
        canonical_name = name or (data.get("name") if data else None)

        if source.lower() == "coingecko" and data and data.get("id"):
            canonical_id = f"cg:{data.get('id')}"
            canonical_symbol = (data.get("symbol") or "").upper()
        elif source.lower() == "coinmarketcap":
            # try map by symbol
            mapped = symbol_map.get(sym_norm)
            if mapped:
                canonical_id = f"cg:{mapped}"
                canonical_symbol = sym_norm
            else:
                canonical_id = f"cmc:{item_id}"
                canonical_symbol = sym_norm or (name or "")[:10].upper()
        else:
            # fallback
            canonical_id = f"raw:{source}:{item_id}"
            canonical_symbol = sym_norm or (name or "")[:10].upper()

        price = _extract_price(raw_json)
        mcap = _extract_market_cap(raw_json)

        source_ids = {source: item_id}
        sources_meta = {source: {"fetched_at": fetched_at.isoformat() if fetched_at else None}}

        extra = {}

        to_upsert.append((canonical_id, canonical_symbol, canonical_name, price, mcap, fetched_at, json.dumps(source_ids), json.dumps(sources_meta), json.dumps(extra)))

    # Upsert logic: for simplicity, we'll DROP existing rows with same canonical_id and insert latest (idempotent for this run)
    for row in to_upsert:
        cid = row[0]
        conn.execute("DELETE FROM api_canonical WHERE canonical_id = ?", [cid])
        conn.execute("INSERT INTO api_canonical VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

    # show counts
    total = conn.execute("SELECT COUNT(*) FROM api_canonical").fetchone()[0]
    print(f"Merged {len(to_upsert)} source rows into api_canonical (total rows now: {total})")
    conn.close()


if __name__ == "__main__":
    merge()
