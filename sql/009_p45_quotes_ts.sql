-- Append-only time series for prices (can hold both stocks & crypto)
CREATE TABLE IF NOT EXISTS quotes_ts (
  symbol       VARCHAR,
  provider     VARCHAR,        -- 'finnhub' | 'alphavantage' | other
  ts_recorded  TIMESTAMP,      -- when we captured it
  price        DOUBLE,
  high         DOUBLE,
  low          DOUBLE,
  open         DOUBLE,
  prev_close   DOUBLE,
  volume       DOUBLE,         -- may be null if not provided
  PRIMARY KEY (symbol, provider, ts_recorded)
);

-- Helper view: the latest snapshot (union from both sources)
CREATE OR REPLACE VIEW quote_latest AS
WITH q AS (
  SELECT symbol, price, high, low, open, prev_close, ingested_at, 'finnhub' AS provider
  FROM quotes_finnhub
  UNION ALL
  SELECT symbol, price, high, low, open, prev_close, ingested_at, 'alphavantage' AS provider
  FROM quotes_av
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ingested_at DESC) rn
  FROM q
)
SELECT symbol, price, high, low, open, prev_close, ingested_at, provider
FROM r WHERE rn = 1;
