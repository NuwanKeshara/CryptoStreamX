## To create and run the sink connector on kafka connect

docker exec -it kafka-connect /bin/bash -c "curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/mnt/configs/clickhouse-sink-config.json"


## ksql db run
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

## check connectors, streams, topics
show connectors;
show streams;
show topics;


## Delete Streams
DROP STREAMS BINANCE_TRADE DELETE TOPIC;


# How To Run:

1. docker compose up -d
2. python ./scripts/topic_config.py
3. docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
4. execute stream creation queries (stream_topics.sql)
5. create kafka sink connector (docker exec -it kafka-connect /bin/bash -c "curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/mnt/configs/clickhouse-sink-config.json")
6. python ./scripts/producer.py