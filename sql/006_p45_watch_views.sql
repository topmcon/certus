CREATE OR REPLACE VIEW trend_feed_watch AS
SELECT f.*
FROM trend_feed_exploded f
JOIN watchlist w ON w.symbol = f.symbol_clean;

CREATE OR REPLACE VIEW trend_feed_nonwatch AS
SELECT f.*
FROM trend_feed_exploded f
LEFT JOIN watchlist w ON w.symbol = f.symbol_clean
WHERE w.symbol IS NULL;
