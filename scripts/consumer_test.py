import json
import logging
import os
import sys
from dotenv import load_dotenv
from kafka import KafkaConsumer


# load environment
load_dotenv("../.env")

# configs
TEST_BOOTSTRAP_SERVERS = os.getenv('TEST_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('TEST_TOPIC')
TEST_AUTO_OFFSET_RESET = os.getenv('TEST_AUTO_OFFSET_RESET')
TEST_GROUP_ID = os.getenv('TEST_GROUP_ID')
TEST_CONSUMER_LOG_FILE_PATH = os.getenv('TEST_CONSUMER_LOG_FILE_PATH')

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(TEST_CONSUMER_LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)


# kafka consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=TEST_BOOTSTRAP_SERVERS,
    auto_offset_reset=TEST_AUTO_OFFSET_RESET,
    enable_auto_commit=True,
    group_id=TEST_GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


# print value
for message in consumer:
    logging.info(f"Received from Kafka: {message.value}")
    print(f"Received from Kafka: {message.value}")
