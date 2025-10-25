-- CryptoPanic news
CREATE TABLE IF NOT EXISTS news_cryptopanic (
  id            VARCHAR PRIMARY KEY, -- provider id
  published_at  TIMESTAMP,
  title         VARCHAR,
  url           VARCHAR,
  domain        VARCHAR,
  source        VARCHAR,
  currencies    VARCHAR,             -- comma list
  kind          VARCHAR,             -- news, media, etc.
  votes         BIGINT,
  raw           JSON,
  ingested_at   TIMESTAMP DEFAULT now()
);

-- CoinMarketCal events
CREATE TABLE IF NOT EXISTS events_coinmarketcal (
  id            VARCHAR PRIMARY KEY, -- provider id
  title         VARCHAR,
  description   VARCHAR,
  coin_symbol   VARCHAR,
  coin_name     VARCHAR,
  date_event    TIMESTAMP,
  is_hot        BOOLEAN,
  source        VARCHAR,
  proof         VARCHAR,
  url           VARCHAR,
  raw           JSON,
  ingested_at   TIMESTAMP DEFAULT now()
);

-- Finnhub quotes (latest tick snapshot)
CREATE TABLE IF NOT EXISTS quotes_finnhub (
  symbol        VARCHAR,
  price         DOUBLE,
  high          DOUBLE,
  low           DOUBLE,
  open          DOUBLE,
  prev_close    DOUBLE,
  t_unix_ms     BIGINT,
  raw           JSON,
  ingested_at   TIMESTAMP DEFAULT now()
);

-- AlphaVantage global quotes (latest snapshot)
CREATE TABLE IF NOT EXISTS quotes_av (
  symbol        VARCHAR,
  price         DOUBLE,
  high          DOUBLE,
  low           DOUBLE,
  open          DOUBLE,
  prev_close    DOUBLE,
  volume        DOUBLE,
  ts            TIMESTAMP,           -- provider timestamp if present
  raw           JSON,
  ingested_at   TIMESTAMP DEFAULT now()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_news_time ON news_cryptopanic(published_at);
CREATE INDEX IF NOT EXISTS idx_events_time ON events_coinmarketcal(date_event);
CREATE INDEX IF NOT EXISTS idx_finnhub_sym ON quotes_finnhub(symbol);
CREATE INDEX IF NOT EXISTS idx_av_sym ON quotes_av(symbol);
