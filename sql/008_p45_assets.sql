CREATE TABLE IF NOT EXISTS assets (
  symbol VARCHAR PRIMARY KEY,
  name   VARCHAR,
  market VARCHAR              -- 'crypto' | 'stock' | NULL (unknown)
);

-- Discover symbols from current data (exploded feed + quotes)
CREATE OR REPLACE VIEW discovered_symbols AS
SELECT DISTINCT UPPER(symbol_clean) AS symbol
FROM trend_feed_exploded
UNION
SELECT DISTINCT UPPER(symbol) FROM quotes_finnhub
UNION
SELECT DISTINCT UPPER(symbol) FROM quotes_av;
