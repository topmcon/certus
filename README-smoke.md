Run smoke checks and API connectivity

This project includes two quick checks useful during development:

- `run_smoke.py` — a lightweight script that uses the in-repo CoinGecko client to ping the service and fetch a tiny markets sample. Writes `results/smoke.json`.
- `scripts/action_check_apis.py` — CI-friendly checker which validates connectivity for CoinGecko and CoinMarketCal (reads keys from `.env`).

Usage:

Run the smoke script (no secrets required for public CoinGecko):

```bash
python3 run_smoke.py
```

Run the full API checker (requires `COINGECKO_API_KEY` and `COINMARKETCAL_API_KEY` in `.env`):

```bash
set -a; source .env; set +a; python3 scripts/action_check_apis.py
```

You can also use the Makefile targets:

```bash
make smoke
make check-apis
```
