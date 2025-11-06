
CREATE STREAM BINANCE_TRADE_RAW (
  event_type VARCHAR,
  event_time BIGINT,
  symbol VARCHAR,
  trade_id BIGINT,
  price DOUBLE,
  quantity DOUBLE,
  trade_time BIGINT,
  market_maker BOOLEAN,
  ignore BOOLEAN
) WITH (
  KAFKA_TOPIC='binance_test',
  VALUE_FORMAT='JSON',
  TIMESTAMP='event_time'
);



CREATE TABLE BINANCE_TRADE_AGG_1M
WITH (KAFKA_TOPIC='binance_trade_agg_1m') AS
SELECT
  'BTCUSDT' AS symbol,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS trade_count,
  SUM(quantity) AS total_quantity,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM BINANCE_TRADE_RAW
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 10 SECONDS)
GROUP BY 'BTCUSDT'
EMIT FINAL;



CREATE TABLE BINANCE_TRADE_AGG_5M
WITH (KAFKA_TOPIC='binance_trade_agg_5m') AS
SELECT
  'BTCUSDT' AS symbol,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS trade_count,
  SUM(quantity) AS total_quantity,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM BINANCE_TRADE_RAW
WINDOW TUMBLING (SIZE 5 MINUTE, GRACE PERIOD 10 SECONDS)
GROUP BY 'BTCUSDT'
EMIT FINAL;
