-- Create quotes table if missing (idempotent insert pattern is handled in Python)
CREATE TABLE IF NOT EXISTS cg_quotes (
  ts BIGINT,
  id TEXT,
  symbol TEXT,
  name TEXT,
  vs_currency TEXT,
  price DOUBLE,
  market_cap DOUBLE,
  volume_24h DOUBLE,
  pct_change_24h DOUBLE
);

CREATE TABLE IF NOT EXISTS cg_prices (
  ts BIGINT,
  id TEXT,
  symbol TEXT,
  vs_currency TEXT,
  price DOUBLE,
  market_cap DOUBLE,
  volume DOUBLE
);

-- Latest snapshot per symbol
CREATE OR REPLACE VIEW v_latest_quotes AS
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY ts DESC) rn
  FROM cg_quotes
)
SELECT ts, id, symbol, name, vs_currency, price, market_cap, volume_24h, pct_change_24h
FROM ranked WHERE rn = 1;

-- Top by market cap (uses latest view)
CREATE OR REPLACE VIEW v_top_mcap AS
SELECT * FROM v_latest_quotes ORDER BY market_cap DESC NULLS LAST;

-- Daily bars synthesized from cg_prices (close = last price of day)
CREATE OR REPLACE VIEW v_daily_close AS
WITH s AS (
  SELECT
    date_trunc('day', to_timestamp(ts/1000)) AS d,
    symbol,
    vs_currency,
    last(price) AS close
  FROM cg_prices
  GROUP BY 1,2,3
)
SELECT * FROM s ORDER BY d DESC;
