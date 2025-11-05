import asyncio

from certus.data.coinmarketcal_client import CoinMarketCalClient


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

    async def get(self, url, params=None, headers=None, timeout=None):
        return DummyResp({"body": [{"id": 1, "title": {"en": "test"}}]})

    async def aclose(self):
        return None


def test_ping_and_events(monkeypatch):
    monkeypatch.setattr("certus.data.coinmarketcal_client.httpx.AsyncClient", DummyAsyncClient)

    async def _run():
        client = CoinMarketCalClient()
        res = await client.ping()
        assert isinstance(res, dict)
        events = await client.events(max=1)
        assert isinstance(events, list) and len(events) == 1
        await client.close()

    asyncio.run(_run())
