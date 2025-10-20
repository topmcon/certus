# certus/storage/schema.py
from __future__ import annotations
import duckdb
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS cg_quotes (
    ts               BIGINT,
    id               VARCHAR,
    symbol           VARCHAR,
    name             VARCHAR,
    vs_currency      VARCHAR,
    rank             INTEGER,
    price            DOUBLE,
    market_cap       DOUBLE,
    volume_24h       DOUBLE,
    pct_change_1h    DOUBLE,
    pct_change_24h   DOUBLE,
    pct_change_7d    DOUBLE
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS cg_quotes_ts_idx   ON cg_quotes(ts);
CREATE INDEX IF NOT EXISTS cg_quotes_id_ts    ON cg_quotes(id, ts);
"""

def ensure_schema(duckdb_path: str) -> None:
    Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(duckdb_path)
    try:
        con.execute(DDL)
    finally:
        con.close()
