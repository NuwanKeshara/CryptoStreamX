
CREATE DATABASE IF NOT EXISTS crypto;
USE crypto;

-- CREATE TABLE IF NOT EXISTS binance_test (
--     event_type String,
--     event_time UInt32,
--     symbol String,
--     trade_id UInt32,
--     price Float32,
--     quantity Float32,
--     trade_time UInt32,
--     market_maker Bool,
--     ignore Bool
-- )
-- ENGINE = MergeTree()
-- ORDER BY (symbol, trade_id);



-- CREATE TABLE binance_trade_agg_1m (
--     window_start UInt64,
--     window_end   UInt64,
--     trade_count  UInt64,
--     total_quantity Float64,
--     avg_price    Float64,
--     min_price    Float64,
--     max_price    Float64,
--     symbol       String
-- ) 
-- ENGINE = MergeTree()
-- ORDER BY (window_start);



-- CREATE TABLE binance_trade_agg_5m (
--     window_start UInt64,
--     window_end   UInt64,
--     trade_count  UInt64,
--     total_quantity Float64,
--     avg_price    Float64,
--     min_price    Float64,
--     max_price    Float64,
--     symbol       String
-- ) 
-- ENGINE = MergeTree()
-- ORDER BY (window_start);

CREATE TABLE binance_test (
    event_type     String,
    event_time     UInt64, 
    trade_time     UInt64,
    symbol         String,
    trade_id       UInt64,
    price          Float64,
    quantity       Float64,
    market_maker   UInt8,
    ignore         UInt8,
    event_time_dt  DateTime64(3, 'UTC') MATERIALIZED toDateTime(event_time / 1000, 'UTC'),
    trade_time_dt  DateTime64(3, 'UTC') MATERIALIZED toDateTime(trade_time / 1000, 'UTC')
) 
ENGINE = MergeTree()
ORDER BY (event_time);


CREATE TABLE binance_trade_agg_1m (
    window_start   UInt64,
    window_end     UInt64,
    trade_count    UInt64,
    total_quantity Float64,
    avg_price      Float64,
    min_price      Float64,
    max_price      Float64,
    symbol         String DEFAULT 'BTCUSDT',
    window_start_dt DateTime64(3, 'UTC') MATERIALIZED toDateTime(window_start / 1000, 'UTC'),
    window_end_dt   DateTime64(3, 'UTC') MATERIALIZED toDateTime(window_end / 1000, 'UTC')
) 
ENGINE = MergeTree()
ORDER BY (symbol, window_start);


CREATE TABLE binance_trade_agg_5m (
    window_start   UInt64,
    window_end     UInt64,
    trade_count    UInt64,
    total_quantity Float64,
    avg_price      Float64,
    min_price      Float64,
    max_price      Float64,
    symbol         String DEFAULT 'BTCUSDT',
    window_start_dt DateTime64(3, 'UTC') MATERIALIZED toDateTime(window_start / 1000, 'UTC'),
    window_end_dt   DateTime64(3, 'UTC') MATERIALIZED toDateTime(window_end / 1000, 'UTC')
) ENGINE = MergeTree()
ORDER BY (symbol, window_start);


