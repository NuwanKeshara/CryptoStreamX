# CryptoStreamX — Real-Time Crypto Trade Analytics Pipeline

CryptoStreamX is a fully streaming data pipeline that captures and processes live cryptocurrency trade data from Binance in real time. Built with Apache Kafka, the system ingests, buffers, and distributes high-throughput event streams. Kafka Streams and ksqlDB power in-flight transformations, aggregations, and real-time analytics without external compute layers. Kafka Connect integrates seamlessly with ClickHouse, a high-performance columnar database, for ultra-fast storage and query execution on time-series trade data. This architecture delivers a scalable, fault-tolerant data flow from ingestion to analytics, enabling instant insights into trading activity, price movements, and market patterns.

## Tech Stack

- Data Ingestion: Apache Kafka
- Stream Processing: ksqlDB, Kafka Streams
- Data Integration: Kafka Connect
- Data Storage: ClickHouse
- Programming Language: Python
- Serialization Format: JSON
- Deployment: Docker


### in alpha stage...
