--sample data
-- {
-- "stream": "stream data",
-- "data" :{
--   "e": "aggTrade",    // Event type
--   "E": 1672515782136, // Event time
--   "s": "BNBBTC",      // Symbol
--   "a": 12345,         // Aggregate trade ID
--   "p": "0.001",       // Price
--   "q": "100",         // Quantity
--   "f": 100,           // First trade ID
--   "l": 105,           // Last trade ID
--   "T": 1672515782136, // Trade time
--   "m": true,          // Is the buyer the market maker?
--   "M": true           // Ignore
--     }
-- }

-- Source stream from raw Binance topic
CREATE STREAM binance_trades_raw (                                                                                                                                                                  
  "stream" STRING,
  "data" STRUCT <                                                                                                                                                                             
  "e" STRING,                                                                                                                                                                          
  "E" BIGINT,                                                                                                                                                                       
  "s" STRING,                                                                                                                                                                              
  "t" BIGINT,                                                                                                                                                                            
  "p" DOUBLE,                                                                                                                                                                               
  "q" DOUBLE,                                                                                                                                                                            
  "T" BIGINT,                                                                                                                                                                          
  "m" BOOLEAN,                                                                                                                                                                 
  "M" BOOLEAN                                                                                                                                                                         
  >                                                                                                                                                                                                       
) WITH (                                                                                                                                                                                                 
  KAFKA_TOPIC='binance.test',                                                                                                                                                                            
  VALUE_FORMAT='JSON'
); 

-- Stream with renamed + typed fields
CREATE STREAM IF NOT EXISTS BINANCE_TRADES_CLEANED WITH (
    KAFKA_TOPIC = 'binance.trades.cleand',
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
