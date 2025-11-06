
CREATE DATABASE IF NOT EXISTS crypto;
USE crypto;

CREATE TABLE binance_trade (
    event_type     String,
    event_time     UInt64, 
    trade_time     UInt64,
    symbol         String,
    trade_id       UInt64,
    price          Float64,
    quantity       Float64,
    market_maker   Bool,
    ignore         Bool,
    event_time_dt  DateTime64(3, 'UTC') ALIAS toDateTime(event_time / 1000, 'UTC'),
    trade_time_dt  DateTime64(3, 'UTC') ALIAS toDateTime(trade_time / 1000, 'UTC')
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
    window_start_dt DateTime64(3, 'UTC') ALIAS toDateTime(window_start / 1000, 'UTC'),
    window_end_dt   DateTime64(3, 'UTC') ALIAS toDateTime(window_end / 1000, 'UTC')
) 
ENGINE = MergeTree()
ORDER BY (window_start);


CREATE TABLE binance_trade_agg_5m (
    window_start   UInt64,
    window_end     UInt64,
    trade_count    UInt64,
    total_quantity Float64,
    avg_price      Float64,
    min_price      Float64,
    max_price      Float64,
    window_start_dt DateTime64(3, 'UTC') ALIAS toDateTime(window_start / 1000, 'UTC'),
    window_end_dt   DateTime64(3, 'UTC') ALIAS toDateTime(window_end / 1000, 'UTC')
) 
ENGINE = MergeTree()
ORDER BY (window_start);


