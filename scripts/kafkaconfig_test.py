import logging
import os
import sys
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic


# load environment
load_dotenv("../.env")

# configs
TEST_BOOTSTRAP_SERVERS = os.getenv('TEST_BOOTSTRAP_SERVERS')
TEST_CONFIG_LOG_FILE_PATH = os.getenv('TEST_CONFIG_LOG_FILE_PATH')
TEST_CLIENT_ID = os.getenv('TEST_CLIENT_ID')
TOPIC = os.getenv('TEST_TOPIC')
TEST_NUM_PARTITIONS = int(os.getenv('TEST_NUM_PARTITIONS'))
TOPIC = os.getenv('TEST_TOPIC')
TEST_REPLICATION_FACTOR = int(os.getenv('TEST_REPLICATION_FACTOR'))
TEST_CLEANUP_POLICY = os.getenv('TEST_CLEANUP_POLICY')
TEST_RETENTION_MS = os.getenv('TEST_RETENTION_MS')

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(TEST_CONFIG_LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)


# admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=TEST_BOOTSTRAP_SERVERS,
    client_id=TEST_CLIENT_ID
)



# define topics
topic_list = [
    NewTopic(
    name=TOPIC,
    num_partitions=TEST_NUM_PARTITIONS,
    replication_factor=TEST_REPLICATION_FACTOR,
    topic_configs={
        'cleanup.policy': TEST_CLEANUP_POLICY,
        'retention.ms': TEST_RETENTION_MS
    }
)
]


# create topic
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    logging.info(f"Topics created successfully on '{TEST_BOOTSTRAP_SERVERS}': {topic_list}")
    print("Topics created successfully")
except Exception as e:
    logging.info(f"Topic creation failed: {e}")
    print(f"Topic creation failed: {e}")
finally:
    admin_client.close()
    logging.info(f"Closed admin client connection to '{TEST_BOOTSTRAP_SERVERS}'")
    print(f"Closed admin client connection to '{TEST_BOOTSTRAP_SERVERS}'")