#!/usr/bin/env bash
set -e
echo "== Certus auto-update =="
python scripts/fetch_markets.py
python scripts/build_top_markets.py
echo "== Done. Database + Top 500 Trending refreshed =="
