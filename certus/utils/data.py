# certus/utils/data.py
from __future__ import annotations
import os, httpx, pandas as pd
from datetime import datetime, timezone

COINGECKO_API = os.getenv("COINGECKO_API", "https://pro-api.coingecko.com/api/v3")
CG_API_KEY = os.getenv("COINGECKO_API_KEY")  # set in env

HEADERS = {"accept": "application/json", **({"x-cg-pro-api-key": CG_API_KEY} if CG_API_KEY else {})}

async def fetch_markets(per_page: int = 25, page: int = 1) -> pd.DataFrame:
    url = f"{COINGECKO_API}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "price_change_percentage": "24h"
    }
    async with httpx.AsyncClient(timeout=30.0, headers=HEADERS) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    rows = []
    for d in data:
        rows.append({
            "id": d.get("id"),
            "symbol": (d.get("symbol") or "").upper(),
            "name": d.get("name"),
            "price": d.get("current_price"),
            "pct_change_24h": d.get("price_change_percentage_24h"),
            "market_cap": d.get("market_cap"),
            "volume_24h": d.get("total_volume"),
            "high_24h": d.get("high_24h"),
            "low_24h": d.get("low_24h"),
            "last_updated": pd.to_datetime(d.get("last_updated"))
        })
    return pd.DataFrame(rows)

async def fetch_ohlcv_minute(coin_id: str, days: int = 1) -> pd.DataFrame:
    """
    Pulls minute data for 'days' (1 or 7 typical for minute resolution from CG).
    We then resample to 3m/5m/15m/30m/1h in the UI.
    """
    url = f"{COINGECKO_API}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "minute"}
    async with httpx.AsyncClient(timeout=30.0, headers=HEADERS) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        payload = r.json()

    # payload has 'prices', 'market_caps', 'total_volumes' as [ [ms, value], ... ]
    px = payload.get("prices", [])
    vol = payload.get("total_volumes", [])
    if not px:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    dfp = pd.DataFrame(px, columns=["ms", "price"])
    dfp["ts"] = pd.to_datetime(dfp["ms"], unit="ms", utc=True).dt.tz_convert("UTC")
    dfp.drop(columns=["ms"], inplace=True)

    dv = pd.DataFrame(vol, columns=["ms", "volume"])
    dv["ts"] = pd.to_datetime(dv["ms"], unit="ms", utc=True).dt.tz_convert("UTC")
    dv.drop(columns=["ms"], inplace=True)

    d = pd.merge_asof(dfp.sort_values("ts"), dv.sort_values("ts"), on="ts")
    # Approximate OHLC from minute ticks by using minute bars
    d.set_index("ts", inplace=True)
    ohlc = d["price"].resample("1min").ohlc()
    volm = d["volume"].resample("1min").last().fillna(0)
    ohlc["volume"] = volm
    ohlc.reset_index(inplace=True)
    ohlc.rename(columns=str, inplace=True)
    return ohlc[["ts", "open", "high", "low", "close", "volume"]]
