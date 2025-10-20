#!/usr/bin/env bash
set -euo pipefail
python -m certus.ingestion.ingest_coingecko --symbols BTC,ETH,SOL --vs USD --top 50 --interval 1m --dest duckdb,parquet
