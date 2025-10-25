#!/usr/bin/env bash
set -euo pipefail
echo "== P4.5 smokes =="
python scripts/smoke_cryptopanic.py
python scripts/smoke_coinmarketcal.py
python scripts/smoke_finnhub.py
python scripts/smoke_alphavantage.py
echo "== All smokes done =="
