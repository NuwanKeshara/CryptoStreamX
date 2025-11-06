
CREATE STREAM BINANCE_TRADE_RAW (
  "event_type" VARCHAR,
  "event_time" BIGINT,
  "symbol" VARCHAR,
  "trade_id" BIGINT,
  "price" DOUBLE,
  "quantity" DOUBLE,
  "trade_time" BIGINT,
  "market_maker" BOOLEAN,
  "ignore" BOOLEAN
) WITH (
  KAFKA_TOPIC='binance_trade',
  VALUE_FORMAT='AVRO',
  TIMESTAMP='"event_time"'
);




CREATE TABLE BINANCE_TRADE_AGG_1M
WITH (
  KAFKA_TOPIC='binance_trade_agg_1m',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  "symbol" AS symbol,
  CAST(WINDOWSTART AS BIGINT) AS "window_start",
  CAST(WINDOWEND AS BIGINT) AS "window_end",
  CAST(COUNT(*) AS BIGINT) AS "trade_count",
  CAST(SUM("quantity") AS DOUBLE) AS "total_quantity",
  CAST(AVG("price") AS DOUBLE) AS "avg_price",
  CAST(MIN("price") AS DOUBLE) AS "min_price",
  CAST(MAX("price") AS DOUBLE) AS "max_price"
FROM BINANCE_TRADE_RAW
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 10 SECONDS)
GROUP BY "symbol"
EMIT FINAL;





CREATE TABLE BINANCE_TRADE_AGG_5M
WITH (
  KAFKA_TOPIC='binance_trade_agg_5m',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  "symbol" AS symbol,
  CAST(WINDOWSTART AS BIGINT) AS "window_start",
  CAST(WINDOWEND AS BIGINT) AS "window_end",
  CAST(COUNT(*) AS BIGINT) AS "trade_count",
  CAST(SUM("quantity") AS DOUBLE) AS "total_quantity",
  CAST(AVG("price") AS DOUBLE) AS "avg_price",
  CAST(MIN("price") AS DOUBLE) AS "min_price",
  CAST(MAX("price") AS DOUBLE) AS "max_price"
FROM BINANCE_TRADE_RAW
WINDOW TUMBLING (SIZE 5 MINUTE, GRACE PERIOD 10 SECONDS)
GROUP BY "symbol"
EMIT FINAL;

