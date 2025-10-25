from __future__ import annotations
import os
from dotenv import load_dotenv

# Load .env from project root explicitly
load_dotenv(dotenv_path=".env", override=False)

def get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required env: {name}")
    return v

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
