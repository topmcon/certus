from __future__ import annotations
from typing import Any, Dict
from certus.utils.env import get_env
from certus.utils.http import get_json

BASE = "https://cryptopanic.com/api/developer/v2"

def latest_posts(public: bool = True, currencies: str | None = None, page: int = 1) -> Dict[str, Any]:
    token = get_env("CRYPTOPANIC_API_KEY")
    params = {"auth_token": token, "public": str(public).lower(), "page": page}
    if currencies:
        params["currencies"] = currencies
    return get_json(f"{BASE}/posts/", params=params)
