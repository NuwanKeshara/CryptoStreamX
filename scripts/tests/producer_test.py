import json
import logging
import random
import time
import sys
import os
from kafka import KafkaProducer
from dotenv import load_dotenv


# load environment
load_dotenv("../.env")

# configs
TEST_BOOTSTRAP_SERVERS = os.getenv('TEST_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('TEST_TOPIC')
TEST_PRODUCER_LOG_FILE_PATH = os.getenv('TEST_PRODUCER_LOG_FILE_PATH')
TEST_ACK = int(os.getenv('TEST_ACK'))


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(TEST_PRODUCER_LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)


# kafka producer
producer = KafkaProducer(
    bootstrap_servers=TEST_BOOTSTRAP_SERVERS,
    acks=TEST_ACK,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_random_data() -> dict:
    return {
        'id': random.randint(1000, 9999),
        'value': random.random(),
        'status': random.choice(['OK', 'FAIL']),
        'timestamp': int(time.time())
    }


def send_random_data_forever():
    while True:
        data = generate_random_data()
        # key = data['id']
        logging.info(f"Random data generated: {data}")

        producer.send(TOPIC, value=data)

        print(f"Sent to Kafka: {data}")
        logging.info(f"Sent to Kafka: {data}")
        time.sleep(3) 



if __name__ == "__main__":
    logging.info(f"Started Kafka producer sending to topic '{TOPIC}' on '{TEST_BOOTSTRAP_SERVERS}'")
    send_random_data_forever()
