-- Coarse time “anchors” to join against
CREATE OR REPLACE VIEW price_window_anchors AS
SELECT 
  symbol,
  MAX(CASE WHEN ts_recorded <= now() - INTERVAL '1 hour' THEN ts_recorded END)  AS ts_1h,
  MAX(CASE WHEN ts_recorded <= now() - INTERVAL '24 hours' THEN ts_recorded END) AS ts_24h,
  MAX(CASE WHEN ts_recorded <= now() - INTERVAL '48 hours' THEN ts_recorded END) AS ts_48h,
  MAX(CASE WHEN ts_recorded <= now() - INTERVAL '72 hours' THEN ts_recorded END) AS ts_72h,
  MAX(CASE WHEN ts_recorded <= now() - INTERVAL '7 days' THEN ts_recorded END)   AS ts_7d
FROM quotes_ts
GROUP BY symbol;

-- Current price per symbol (most recent point)
CREATE OR REPLACE VIEW price_now AS
SELECT q.symbol,
       FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts_recorded DESC) AS price_now
FROM quotes_ts q
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_recorded DESC) = 1;

-- Pull back reference prices at anchors and compute % changes
CREATE OR REPLACE VIEW price_windows AS
WITH a AS (SELECT * FROM price_window_anchors),
n AS (SELECT * FROM price_now)
SELECT
  n.symbol,
  n.price_now,
  ref_1h.price   AS price_1h,
  ref_24h.price  AS price_24h,
  ref_48h.price  AS price_48h,
  ref_72h.price  AS price_72h,
  ref_7d.price   AS price_7d,
  CASE WHEN ref_1h.price  IS NOT NULL AND ref_1h.price  != 0 THEN (n.price_now / ref_1h.price  - 1)*100 END AS pct_1h,
  CASE WHEN ref_24h.price IS NOT NULL AND ref_24h.price != 0 THEN (n.price_now / ref_24h.price - 1)*100 END AS pct_24h,
  CASE WHEN ref_48h.price IS NOT NULL AND ref_48h.price != 0 THEN (n.price_now / ref_48h.price - 1)*100 END AS pct_48h,
  CASE WHEN ref_72h.price IS NOT NULL AND ref_72h.price != 0 THEN (n.price_now / ref_72h.price - 1)*100 END AS pct_72h,
  CASE WHEN ref_7d.price  IS NOT NULL AND ref_7d.price  != 0 THEN (n.price_now / ref_7d.price  - 1)*100 END AS pct_7d
FROM n
LEFT JOIN a ON a.symbol = n.symbol
LEFT JOIN quotes_ts AS ref_1h  ON ref_1h.symbol  = a.symbol AND ref_1h.ts_recorded  = a.ts_1h
LEFT JOIN quotes_ts AS ref_24h ON ref_24h.symbol = a.symbol AND ref_24h.ts_recorded = a.ts_24h
LEFT JOIN quotes_ts AS ref_48h ON ref_48h.symbol = a.symbol AND ref_48h.ts_recorded = a.ts_48h
LEFT JOIN quotes_ts AS ref_72h ON ref_72h.symbol = a.symbol AND ref_72h.ts_recorded = a.ts_72h
LEFT JOIN quotes_ts AS ref_7d  ON ref_7d.symbol  = a.symbol AND ref_7d.ts_recorded  = a.ts_7d;
