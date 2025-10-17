import json
import logging
from kafka import KafkaConsumer


# configs
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'test-topic'
GROUP_ID = 'test-group'
AUTO_OFFSET_RESET = 'earliest'

# kafka consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


# print value
for message in consumer:
    logging.info(f"Received from Kafka: {message.value}")
    print(f"Received from Kafka: {message.value}")
