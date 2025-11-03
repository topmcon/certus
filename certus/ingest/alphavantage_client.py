from __future__ import annotations
from typing import Any, Dict
from certus.utils.env import get_env
from certus.utils.http import get_json

BASE = "https://www.alphavantage.co/query"

def global_quote(symbol: str = "IBM") -> Dict[str, Any]:
    # API key should be provided via env var ALPHAVANTAGE_API_KEY
    key = get_env("ALPHAVANTAGE_API_KEY")
    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": key}
    return get_json(BASE, params=params)
