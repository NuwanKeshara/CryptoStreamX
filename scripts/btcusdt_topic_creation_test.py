from kafka.admin import KafkaAdminClient, NewTopic


# admin client
admin_client = KafkaAdminClient(
    bootstrap_servers="host.docker.internal:9092",
    client_id="btcusdt"
)



# define topics
topic_list = [
    NewTopic(
    name="btcusdt_trades",
    num_partitions=1,
    replication_factor=2,
    topic_configs={
        'cleanup.policy': "delete",
        'retention.ms': 300000
    }
)
]


# create topic
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    # logging.info(f"Topics created successfully on '{TEST_BOOTSTRAP_SERVERS}': {topic_list}")
    print("Topics created successfully")
except Exception as e:
    # logging.info(f"Topic creation failed: {e}")
    print(f"Topic creation failed: {e}")
finally:
    admin_client.close()
    # logging.info(f"Closed admin client connection to '{TEST_BOOTSTRAP_SERVERS}'")
    print(f"Closed admin client connection")