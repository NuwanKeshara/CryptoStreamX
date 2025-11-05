
--Stream with renamed + typed fields
CREATE STREAM IF NOT EXISTS BINANCE_TRADES_CLEANED WITH (
    KAFKA_TOPIC = 'binance.trades',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
) AS SELECT
    "data"->"e"                        AS event_type,
    "data"->"E"                        AS event_time_ms,
    "data"->"s"                        AS symbol,
    "data"->"t"                        AS trade_id,
    CAST("data"->"p" AS DECIMAL(38,8)) AS price,
    CAST("data"->"q" AS DECIMAL(38,8)) AS quantity,
    "data"->"T"                        AS trade_time,
    CASE WHEN "data"->"m" THEN 1 ELSE 0 END AS buyer_market_maker,
    CASE WHEN "data"->"M" THEN 1 ELSE 0 END AS ignore_flag
FROM binance_trades_raw
EMIT CHANGES;
