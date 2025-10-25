-- Clean a single symbol (trim, uppercase)
CREATE OR REPLACE VIEW trend_feed_symbols AS
SELECT
  kind, source_id, ts, title, description, symbols_raw,
  CASE
    WHEN symbol_primary IS NOT NULL AND length(trim(symbol_primary)) > 0
      THEN upper(trim(symbol_primary))
    WHEN symbols_raw IS NOT NULL AND length(trim(symbols_raw)) > 0
      THEN upper(trim(REGEXP_EXTRACT(symbols_raw, '^[^,]+')))
    ELSE NULL
  END AS symbol_clean,
  url, domain, source, votes,
  last_price, prev_close, quote_provider, quote_time,
  trend_score
FROM trend_feed_scored;

-- Explode all symbols from a row (one row per symbol)
-- Handles comma-separated lists like "BTC,ETH,SOL"
CREATE OR REPLACE VIEW trend_feed_exploded AS
WITH base AS (
  SELECT
    kind, source_id, ts, title, description, symbols_raw,
    COALESCE(symbol_primary, '') AS symbol_primary,
    url, domain, source, votes,
    last_price, prev_close, quote_provider, quote_time, trend_score
  FROM trend_feed_scored
),
sym_list AS (
  SELECT
    b.*,
    -- split by comma into a list (empty -> [])
    CASE
      WHEN length(trim(b.symbols_raw)) > 0 THEN str_split(b.symbols_raw, ',')
      ELSE []::VARCHAR[]
    END AS syms
  FROM base b
),
exploded AS (
  SELECT
    kind, source_id, ts, title, description, symbols_raw,
    upper(trim(CASE
      WHEN length(trim(symbol_primary)) > 0 THEN symbol_primary
      ELSE s
    END)) AS symbol_clean,
    url, domain, source, votes,
    last_price, prev_close, quote_provider, quote_time, trend_score
  FROM sym_list, UNNEST(syms) AS t(s)
)
SELECT * FROM exploded;
