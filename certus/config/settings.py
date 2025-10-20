# certus/config/settings.py
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

# --- .env support ---
# Requires: python-dotenv
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
except Exception:  # keep imports lightweight if package not installed yet
    load_dotenv = None
    find_dotenv = None

def _load_env() -> Path:
    """
    Load .env (if present) and return the project root directory.
    We prefer the directory containing the discovered .env. If not found,
    fall back to repo root inferred from this file's path.
    """
    dotenv_path_str = None
    if find_dotenv is not None:
        dotenv_path_str = find_dotenv(usecwd=True)
    if dotenv_path_str:
        dotenv_path = Path(dotenv_path_str).resolve()
        if load_dotenv is not None:
            load_dotenv(dotenv_path, override=False)
        return dotenv_path.parent
    # Fallback: repo root = two levels above this file (certus/config/)
    return Path(__file__).resolve().parents[2]

ROOT = _load_env()

# ---------- helpers ----------
def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip())
    except Exception:
        return default

# ---------- settings ----------
@dataclass(frozen=True)
class _Settings:
    # Storage
    data_dir: str = os.getenv("DATA_DIR", str(ROOT / "data"))
    duckdb_path: str = os.getenv("DUCKDB_PATH", str(Path(data_dir) / "certus.duckdb"))

    # CoinGecko
    COINGECKO_API_KEY: str | None = os.getenv("COINGECKO_API_KEY")
    COINGECKO_BASE_URL: str = os.getenv(
        "COINGECKO_BASE_URL",
        "https://pro-api.coingecko.com/api/v3" if os.getenv("COINGECKO_API_KEY") else "https://api.coingecko.com/api/v3"
    )

    # Default fetch params
    cg_order: str = os.getenv("CG_ORDER", "market_cap_desc")
    cg_vs_currency: str = os.getenv("CG_VS_CURRENCY", "usd")
    cg_price_change_pct: str = os.getenv("CG_PRICE_CHANGE_PCT", "1h,24h,7d")
    cg_per_page: int = env_int("CG_PER_PAGE", 50)
    cg_page: int = env_int("CG_PAGE", 1)

# Ensure data dir exists
Path(_Settings().data_dir).mkdir(parents=True, exist_ok=True)

SETTINGS = _Settings()
