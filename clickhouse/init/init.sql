
CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto."binance_test" (
    event_type String,
    event_time UInt64,
    symbol String,
    trade_id UInt32,
    price Float32,
    quantity Float32,
    trade_time UInt64,
    market_maker Bool,
    ignore Bool
)
ENGINE = MergeTree()
ORDER BY (symbol, trade_id);


CREATE TABLE IF NOT EXISTS binance_trade_agg_1m
(
    symbol String,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    trade_count UInt32,
    total_quantity Float64,
    avg_price Float64,
    min_price Float64,
    max_price Float64
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS binance_trade_agg_5m
(
    symbol String,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    trade_count UInt32,
    total_quantity Float64,
    avg_price Float64,
    min_price Float64,
    max_price Float64
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;
