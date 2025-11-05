from __future__ import annotations
from fastapi import FastAPI
import asyncio

from certus.data.coingecko_client import CoinGeckoClient
from certus.data.coinmarketcal_client import CoinMarketCalClient

app = FastAPI()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/markets_sample")
async def markets_sample():
    cg = CoinGeckoClient()
    cmc = CoinMarketCalClient()
    try:
        markets = await cg.coins_markets(per_page=5, page=1)
        events = await cmc.events(max=3, page=1)
    finally:
        await cg.close()
        await cmc.close()

    return {"markets_count": len(markets), "events_count": len(events), "markets": markets[:3], "events": events[:3]}
