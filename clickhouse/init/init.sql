CREATE DATABASE IF NOT EXISTS crypto;
USE crypto;


CREATE TABLE btcusdt_trade_agg (
    id UInt64 DEFAULT rowNumberInAllBlocks(),
    symbol String,
    window_start DateTime,
    window_end DateTime,
    total_volume Float64,
    trade_count UInt64,
    insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id);
