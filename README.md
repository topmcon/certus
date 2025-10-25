# Certus | Phase 2 — CoinGecko Data Pipeline (Python)

[![Certus Verify](https://github.com/topmcon/certus/actions/workflows/certus-verify.yml/badge.svg?branch=main)](https://github.com/topmcon/certus/actions/workflows/certus-verify.yml)


Quick-start pipeline to pull quotes & market data from CoinGecko and store them to DuckDB + Parquet.

## 1) Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # put your CoinGecko API key
```

## 2) Run

```bash
python -m certus.ingestion.ingest_coingecko --symbols BTC,ETH,SOL --vs USD --top 50 --interval 1m --dest duckdb,parquet
```

## 3) Notes

- Respects CoinGecko rate limits via client-side limiter + retries.
- Switch to Pro by exporting `CG_BASE_URL=https://pro-api.coingecko.com/api/v3` or editing `.env`.
- Data lands in `./data/` (change via env). DuckDB DB = `certus.duckdb`.

[![Certus Verify](https://github.com/topmcon/certus/actions/workflows/certus-verify.yml/badge.svg?branch=main)](https://github.com/topmcon/certus/actions/workflows/certus-verify.yml)
