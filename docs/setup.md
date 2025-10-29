## To create and run the sink connector on kafka connect

docker exec -it kafka-connect /bin/bash -c "curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @/mnt/configs/clickhouse-sink-config.json"


## ksql db run
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

## check connectors, streams, topics
show connectors;
show streams;
show topics;
