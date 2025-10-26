CREATE DATABASE IF NOT EXISTS crypto;
USE crypto;


CREATE TABLE btcusdt_trade_agg_1 (
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


CREATE TABLE btcusdt_trade_agg_2 (
  id UInt64 DEFAULT rowNumberInAllBlocks(),
  symbol String,
  avg_price Float64,
  total_qty Float64,
  trade_count UInt64,
  window_start DateTime64(3),
  window_end DateTime64(3),
  insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, window_start);


CREATE TABLE binance_trades_raw (
  id UInt64 DEFAULT rowNumberInAllBlocks(),
  event_type String,
  event_time_ms UInt64,
  symbol String,
  trade_id UInt64,
  price Float64,
  quantity Float64,
  trade_time UInt64,
  buyer_market_maker UInt8,
  ignore_flag UInt8,
  insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, event_time_ms);