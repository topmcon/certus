CREATE OR REPLACE VIEW symbol_rollup AS
WITH m AS (
  SELECT
    upper(symbol_clean) AS symbol,
    count(*)                           AS total_mentions,
    count(DISTINCT domain)             AS unique_sources,
    max(ts)                            AS last_mention,
    string_agg(DISTINCT category, ', ') AS categories
  FROM trend_feed_categorized
  GROUP BY 1
),
p AS (SELECT * FROM price_windows)
SELECT
  coalesce(m.symbol,p.symbol) AS symbol,
  coalesce(total_mentions,0)  AS total_mentions,
  coalesce(unique_sources,0)  AS unique_sources,
  last_mention,
  categories,
  p.price_now,
  p.pct_1h, p.pct_24h, p.pct_48h, p.pct_72h, p.pct_7d,
  ROUND(
      (least(coalesce(p.pct_24h,0),100)/100.0 * 0.4) +
      (least(coalesce(p.pct_72h,0),100)/100.0 * 0.3) +
      (least(coalesce(total_mentions,0),50)/50.0 * 0.3)
  ,4) AS momentum_score
FROM m
FULL OUTER JOIN p ON p.symbol = m.symbol;
