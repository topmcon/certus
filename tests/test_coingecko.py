import asyncio

from certus.data.coingecko_client import CoinGeckoClient


class DummyResp:
    def __init__(self, json_data):
        self._json = json_data
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._json


class DummyAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def get(self, endpoint, params=None):
        if endpoint == "/ping":
            return DummyResp({"gecko_says": "pong"})
        if endpoint == "/coins/markets":
            return DummyResp([{"id": "bitcoin", "symbol": "btc"}, {"id": "ethereum", "symbol": "eth"}])
        return DummyResp({})

    async def aclose(self):
        return None


def test_coingecko_ping_and_markets(monkeypatch):
    monkeypatch.setattr("certus.data.coingecko_client.httpx.AsyncClient", DummyAsyncClient)

    async def _run():
        client = CoinGeckoClient()
        pong = await client.ping()
        assert isinstance(pong, dict)
        markets = await client.coins_markets(per_page=2, page=1)
        assert isinstance(markets, list) and len(markets) == 2
        await client.close()

    asyncio.run(_run())
