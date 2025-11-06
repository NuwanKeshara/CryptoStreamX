import logging
import os
import sys
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema


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
    # initialize admin client
    admin_client = AdminClient({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': CLIENT_ID
    })
except Exception as e:
    logging.error(f"Error setting up admin client: {e}")
    sys.exit(1)
else:
    logging.info("Admin client setup successfully")



# define topic settings
topic_list = [
    NewTopic(
        topic=TOPIC,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        config={
            'cleanup.policy': CLEANUP_POLICY,
            'retention.ms': RETENTION_MS
        }
    )
]


# create topic
futures = admin_client.create_topics(new_topics=topic_list, validate_only=False)
for topic, future in futures.items():
    try:
        future.result()

    except Exception as e:
        logging.error(f"Failed to create topic {topic}: {e}")
        print(f"Failed to create topic {topic}: {e}")
else:

    logging.info(f"Topic '{TOPIC}' created successfully on '{BOOTSTRAP_SERVERS}'")
    print(f"Topic '{TOPIC}' created successfully.")
