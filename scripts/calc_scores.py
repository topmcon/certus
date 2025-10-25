if grep -q '^CERTUS_PAUSED=true' .env 2>/dev/null; then
  echo "[Certus] ⚠️ System paused — skipping smoke check."
  exit 0
fi
#!/usr/bin/env python
from certus.utils.pause_guard import guard_pause
guard_pause()

import duckdb
import pandas as pd
# scripts/calc_scores.py
from __future__ import annotations
import duckdb, logging
from certus.storage.schema import ensure_db
from certus.analytics.scores import build_scores

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

def main():
    con = ensure_db()
    ind = con.sql("""
        SELECT id, symbol, price, rsi_14, macd_hist, trend, ts
        FROM indicators
        ORDER BY ts
    """).fetchdf()
    if ind.empty:
        logging.warning("No indicators to score.")
        return
    scores = build_scores(ind)
    con.register("scores_df", scores)
    con.execute("INSERT INTO scores SELECT * FROM scores_df")
    logging.info("Scores inserted: %d", len(scores))

if __name__ == "__main__":
    main()
