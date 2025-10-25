-- Rolling windows
CREATE OR REPLACE VIEW trend_feed_last72 AS
SELECT * FROM trend_feed_enriched
WHERE ts >= now() - INTERVAL '72 hours';

CREATE OR REPLACE VIEW trend_feed_today AS
SELECT * FROM trend_feed_enriched
WHERE CAST(ts AS DATE) = CAST(now() AS DATE);

-- Simple recency score (0..1) inside last 72h
CREATE OR REPLACE VIEW trend_feed_scored AS
WITH base AS (
  SELECT *,
         1.0 - (DATE_DIFF('minute', ts, now()) / 4320.0) AS recency_raw,
         COALESCE(votes, 0)                              AS votes_raw
  FROM trend_feed_last72
),
norm AS (
  SELECT *,
         GREATEST(0.0, LEAST(1.0, recency_raw)) AS recency,
         CASE
           WHEN votes_raw >= 100 THEN 1.0
           WHEN votes_raw <=   0 THEN 0.0
           ELSE votes_raw / 100.0
         END AS votes_norm
  FROM base
)
SELECT
  kind, source_id, ts, title, description, symbols_raw, symbol_primary,
  url, domain, source, votes,
  last_price, prev_close, quote_provider, quote_time,
  ROUND(0.7*recency + 0.3*votes_norm, 4) AS trend_score
FROM norm;
