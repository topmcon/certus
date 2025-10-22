#!/usr/bin/env python3
import asyncio
from certus.data.coingecko_client import CoinGeckoClient, CoinGeckoHTTPError

async def main():
    c = CoinGeckoClient()
    try:
        pong = await c.ping()
        print("[OK] Ping:", pong)
        data = await c.coins_markets(vs_currency="usd", per_page=5, page=1)
        ids = [d.get("id") for d in data]
        print(f"[OK] Sample page via {'PRO' if c.is_pro else 'PUBLIC'} API. First IDs: {ids}")
    except CoinGeckoHTTPError as e:
        print("[ERR] CoinGecko call failed.")
        print(e)
    finally:
        await c.close()

if __name__ == "__main__":
    asyncio.run(main())
