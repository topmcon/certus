import pytest, asyncio
from certus.data.coingecko_client import CoinGeckoClient

@pytest.mark.asyncio
async def test_ping():
    c = CoinGeckoClient()
    r = await c.ping()
    assert isinstance(r, dict)
