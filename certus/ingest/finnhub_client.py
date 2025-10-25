from __future__ import annotations
from typing import Any, Dict
from certus.utils.env import get_env
from certus.utils.http import get_json

BASE = "https://finnhub.io/api/v1"

def quote(symbol: str = "AAPL") -> Dict[str, Any]:
    token = get_env("FINNHUB_API_KEY")
    params = {"symbol": symbol, "token": token}
    return get_json(f"{BASE}/quote", params=params)
