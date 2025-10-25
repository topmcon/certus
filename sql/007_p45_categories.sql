-- Categorize titles/descriptions and rank priority for dashboard sections
CREATE OR REPLACE VIEW trend_feed_categorized AS
WITH base AS (
  SELECT
    f.kind,
    f.source_id,
    f.ts,
    UPPER(f.symbol_clean)                            AS symbol,
    COALESCE(CAST(f.title AS VARCHAR), '')           AS title,
    COALESCE(CAST(f.description AS VARCHAR), '')     AS description,
    f.trend_score,
    f.last_price,
    f.quote_provider,
    f.url,
    f.source,
    f.domain
  FROM trend_feed_watch f
),
tagged AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_MATCHES(title, '(?i)listing|listed|kraken|binance|coinbase|okx|bybit') THEN 'Listing'
      WHEN REGEXP_MATCHES(title, '(?i)delist|delisted')                                   THEN 'Delist'
      WHEN REGEXP_MATCHES(title, '(?i)airdrop|claim|reward')                              THEN 'Airdrop'
      WHEN REGEXP_MATCHES(title, '(?i)unlock|token unlock|vesting')                       THEN 'Token Unlock'
      WHEN REGEXP_MATCHES(title, '(?i)burn|buyback')                                      THEN 'Burn/Buyback'
      WHEN REGEXP_MATCHES(title, '(?i)partnership|partners with|integrat(es|ion)')        THEN 'Partnership'
      WHEN REGEXP_MATCHES(title, '(?i)governance|vote|proposal|snapshot|dao')             THEN 'Governance/Vote'
      WHEN REGEXP_MATCHES(title, '(?i)mainnet|launch|release|upgrade|hard fork|v\\d+\\.?\\d*') THEN 'Launch/Upgrade'
      WHEN REGEXP_MATCHES(title, '(?i)hack|exploit|vuln|phish|rug')                       THEN 'Security'
      WHEN REGEXP_MATCHES(title, '(?i)etf|sec|regulat|approval|denied|court|lawsuit')     THEN 'Regulatory/Legal'
      WHEN REGEXP_MATCHES(title, '(?i)funding|raises|seed|series [ab]|grant')             THEN 'Funding'
      WHEN REGEXP_MATCHES(title, '(?i)macro|fed|inflation|rates|jobs')                    THEN 'Macro'
      ELSE 'General'
    END AS category
  FROM base
),
scored AS (
  SELECT
    *,
    CASE category
      WHEN 'Listing'          THEN 0.25
      WHEN 'Token Unlock'     THEN 0.20
      WHEN 'Burn/Buyback'     THEN 0.18
      WHEN 'Airdrop'          THEN 0.15
      WHEN 'Launch/Upgrade'   THEN 0.15
      WHEN 'Governance/Vote'  THEN 0.10
      WHEN 'Security'         THEN 0.22
      WHEN 'Regulatory/Legal' THEN 0.22
      WHEN 'Funding'          THEN 0.12
      WHEN 'Macro'            THEN 0.10
      ELSE 0.05
    END AS cat_weight
  FROM tagged
)
SELECT
  kind, source_id, ts, symbol, title, description, category,
  trend_score,
  ROUND(trend_score + cat_weight, 4) AS priority_score,
  last_price, quote_provider, url, source, domain
FROM scored;
