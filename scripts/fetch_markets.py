#!/usr/bin/env python
import asyncio
import logging
import pandas as pd
import time
from certus.utils.data import fetch_markets_df, save_markets

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

async def main():
    logging.info("Fetching markets…")
    df = await fetch_markets_df(per_page=25, page=1)
    if df is None or df.empty:
        raise RuntimeError("fetch_markets_df() returned no data.")

    logging.info(f"Fetched {len(df)} rows.")
    save_markets(df)
    logging.info("✅ Market data saved successfully.")

if __name__ == "__main__":
    asyncio.run(main())
