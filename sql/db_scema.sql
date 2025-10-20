-- sql/db_schema.sql
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

CREATE INDEX IF NOT EXISTS cg_quotes_ts_idx ON cg_quotes(ts);
CREATE INDEX IF NOT EXISTS cg_quotes_id_ts  ON cg_quotes(id, ts);
