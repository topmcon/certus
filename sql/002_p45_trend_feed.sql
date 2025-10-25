-- Latest quote per symbol from either source
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

-- Normalize CryptoPanic + CoinMarketCal into one stream
CREATE OR REPLACE VIEW trend_feed AS
SELECT
  'news'                  AS kind,
  n.id                    AS source_id,
  n.published_at          AS ts,
  COALESCE(json_extract_string(n.raw, '$.title'), n.title) AS title,
  NULL                    AS description,
  n.currencies            AS symbols_raw,
  REGEXP_EXTRACT(n.currencies, '^[^,]+') AS symbol_primary,
  n.url,
  n.domain,
  n.source,
  CAST(n.votes AS DOUBLE) AS votes,
  n.raw                   AS raw
FROM news_cryptopanic n

UNION ALL
SELECT
  'event'                 AS kind,
  e.id                    AS source_id,
  e.date_event            AS ts,
  COALESCE(json_extract_string(e.raw, '$.title.en'), e.title) AS title,
  COALESCE(json_extract_string(e.raw, '$.description.en'), e.description) AS description,
  e.coin_symbol           AS symbols_raw,
  e.coin_symbol           AS symbol_primary,
  e.url,
  NULL                    AS domain,
  e.source,
  NULL                    AS votes,
  e.raw                   AS raw
FROM events_coinmarketcal e;

-- Convenience: enriched with latest quote
CREATE OR REPLACE VIEW trend_feed_enriched AS
SELECT
  f.*,
  q.price    AS last_price,
  q.prev_close,
  q.provider AS quote_provider,
  q.ingested_at AS quote_time
FROM trend_feed f
LEFT JOIN quote_latest q
  ON q.symbol = f.symbol_primary;
