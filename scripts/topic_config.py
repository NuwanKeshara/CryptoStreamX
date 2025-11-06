import logging
import os
import sys
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/topic_config.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


try:
    # load environment
    load_dotenv("./.env")

    # config variables
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
    CLIENT_ID = os.getenv('CLIENT_ID')
    TOPIC = os.getenv('TOPIC')
    TOPIC1 = os.getenv('TOPIC1')
    TOPIC2 = os.getenv('TOPIC2')
    TOPIC3 = os.getenv('TOPIC3')
    NUM_PARTITIONS = int(os.getenv('NUM_PARTITIONS'))
    REPLICATION_FACTOR = int(os.getenv('REPLICATION_FACTOR'))
    CLEANUP_POLICY = os.getenv('CLEANUP_POLICY')
    RETENTION_MS = os.getenv('RETENTION_MS')

except Exception as e:
    print(f"Error loading environment variables: {e}")
    exit(1)
else:
    logging.info("Environment variables loaded successfully")



try:
    # initialize a admin client session
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=CLIENT_ID
    )

except Exception as e:
    logging.info(f"Error setting up admin client: {e}")
    print(f"Error setting up admin client: {e}")
    exit(1)

else:
    logging.info("Admin client setup successfully")


    # define topic settings
    topic_list = [
        NewTopic(
        name=TOPIC,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs={
            'cleanup.policy': CLEANUP_POLICY,
            'retention.ms': RETENTION_MS
            }
        ),
        NewTopic(
        name=TOPIC1,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs={
            'cleanup.policy': CLEANUP_POLICY,
            'retention.ms': RETENTION_MS
            }
        ),
        NewTopic(
        name=TOPIC2,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs={
            'cleanup.policy': CLEANUP_POLICY,
            'retention.ms': RETENTION_MS
            }
        ),
        NewTopic(
        name=TOPIC3,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs={
            'cleanup.policy': CLEANUP_POLICY,
            'retention.ms': RETENTION_MS
            }
        )
    ]



# create topic
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    logging.info(f"Topics created successfully on '{BOOTSTRAP_SERVERS}': {topic_list}")
    print("Topics created successfully")

except Exception as e:
    logging.info(f"Topic creation failed: {e}")
    print(f"Topic creation failed: {e}")

finally:
    admin_client.close()
    logging.info(f"Closed admin client connection to '{BOOTSTRAP_SERVERS}'")
    print(f"Closed admin client connection to '{BOOTSTRAP_SERVERS}'")