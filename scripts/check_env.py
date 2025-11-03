#!/usr/bin/env python3
"""Pre-flight environment checker for required API keys.

Usage: python scripts/check_env.py
Exits with code 0 when all required env vars are present, else exits 2 and prints missing names.
"""
from __future__ import annotations
import os
import sys

REQUIRED = [
    "ALPHAVANTAGE_API_KEY",
    "FINNHUB_API_KEY",
    "CRYPTOPANIC_API_KEY",
    "COINMARKETCAL_API_KEY",
]

def main() -> int:
    missing = [n for n in REQUIRED if not os.getenv(n)]
    if missing:
        print("Missing required environment variables:")
        for m in missing:
            print(f" - {m}")
        print("\nSet them in your shell or in a .env file (do NOT commit .env).")
        return 2
    print("All required environment variables are set.")
    return 0

if __name__ == '__main__':
    sys.exit(main())
