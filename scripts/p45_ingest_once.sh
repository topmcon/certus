#!/usr/bin/env bash
set -euo pipefail
echo "== ingest: CryptoPanic =="
python scripts/ingest_news.py
echo "== ingest: CoinMarketCal =="
python scripts/ingest_events.py
echo "== ingest: Quotes =="
python scripts/ingest_quotes.py
echo "== verify counts =="
python - <<'PY'
import duckdb
con = duckdb.connect("data/markets.duckdb")
for t in ["news_cryptopanic","events_coinmarketcal","quotes_finnhub","quotes_av"]:
    n = con.sql(f"select count(*) from {t}").fetchone()[0]
    print(t, "rows=", n)
con.close()
PY
