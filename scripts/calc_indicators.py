if grep -q '^CERTUS_PAUSED=true' .env 2>/dev/null; then
  echo "[Certus] ⚠️ System paused — skipping smoke check."
  exit 0
fi
#!/usr/bin/env python
from certus.utils.pause_guard import guard_pause
guard_pause()

import duckdb
import logging
# scripts/calc_indicators.py
from __future__ import annotations
import duckdb, logging
from certus.storage.schema import ensure_db
from certus.analytics.indicators import compute_indicators

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

def main():
    con = ensure_db()
    logging.info("Loading markets to compute indicators…")
    df = con.sql("""
        SELECT id, symbol, price, last_updated
        FROM markets
        WHERE price IS NOT NULL
        ORDER BY symbol, last_updated
    """).fetchdf()
    if df.empty:
        logging.warning("No market rows to compute.")
        return
    ind = compute_indicators(df)
    con.register("ind", ind)
    con.execute("INSERT INTO indicators SELECT * FROM ind")
    logging.info("Indicators inserted: %d", len(ind))

if __name__ == "__main__":
    main()
