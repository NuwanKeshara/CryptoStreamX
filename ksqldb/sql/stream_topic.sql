
CREATE STREAM BINANCE_TRADE_STREAM (
  event_type VARCHAR,
  event_time BIGINT,
  symbol VARCHAR,
  trade_id INTEGER,
  price DOUBLE,
  quantity DOUBLE,
  trade_time BIGINT,
  market_maker BOOLEAN,
  ignore BOOLEAN
) WITH (
  KAFKA_TOPIC='binance_test',
  VALUE_FORMAT='JSON'
);



CREATE TABLE binance_trade_agg_1m AS
SELECT
  symbol,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS trade_count,
  SUM(quantity) AS total_quantity,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM BINANCE_TRADE_STREAM
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 10 SECONDS)
GROUP BY symbol;



CREATE TABLE binance_trade_agg_5m AS
SELECT
  symbol,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS trade_count,
  SUM(quantity) AS total_quantity,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM BINANCE_TRADE_STREAM
WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 10 SECONDS)
GROUP BY symbol;
