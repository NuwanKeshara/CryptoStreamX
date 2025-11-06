import json
import os
import signal
import sys
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from websocket import WebSocketApp
from dotenv import load_dotenv

# Load environment
load_dotenv("./.env")

BINANCE_WS_URL = os.getenv('BINANCE_WS_URL')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
TOPIC = os.getenv('TOPIC')


# Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/producer.log"),
        logging.StreamHandler(sys.stdout)
        ]
)


# Schema Registry & Avro setup
schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})


# Fetch the latest schema registered for the topic
try:
    latest_schema = schema_registry_client.get_latest_version(f"{TOPIC}-value")
    value_schema_str = latest_schema.schema.schema_str
    logging.info(f"Using schema id={latest_schema.schema_id} from Schema Registry for topic '{TOPIC}'")
except Exception as e:
    logging.error(f"Failed to fetch schema for topic '{TOPIC}': {e}")
    sys.exit(1)

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=value_schema_str
)


# Kafka Producer
producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer,
    'compression.type': 'lz4',
    'linger.ms': 20,
    'batch.size': 32768,
    'acks': '1'
}


producer = SerializingProducer(producer_conf)


# Message formatter
def make_record(payload):
    """Convert Binance trade message to Avro record"""
    data = payload.get("data", payload)
    if not data:
        return None

    try:
        return {
            "event_type": data.get("e"),
            "event_time": int(data.get("E", 0)),
            "symbol": data.get("s"),
            "trade_id": int(data.get("t", 0)),
            "price": float(data.get("p", 0.0)),
            "quantity": float(data.get("q", 0.0)),
            "trade_time": int(data.get("T", 0)),
            "market_maker": bool(data.get("m", False)),
            "ignore": bool(data.get("M", False))
        }
    except Exception as e:
        logging.error(f"Failed to parse record: {e}")
        return None


# Kafka send wrapper
def send_to_kafka(msg):
    record = make_record(msg)
    if record is None:
        return

    try:
        producer.produce(
            topic=TOPIC,
            key=record["symbol"],
            value=record
        )
        producer.poll(0)
    except Exception as e:
        logging.error(f"Failed to produce message: {e}")



# WebSocket callbacks
def on_message(ws, message):
    try:
        data = json.loads(message)
        send_to_kafka(data)
    except json.JSONDecodeError:
        logging.warning("Received non-JSON message, ignoring.")
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, code, msg):
    logging.warning(f"WebSocket closed (code={code}, msg={msg})")

def on_open(ws):
    print("Connected to Binance WebSocket")
    logging.info("Connected to Binance WebSocket")


# Graceful shutdown
def signal_handler(sig, frame):
    print("Shutting down gracefully...")
    logging.info("Shutting down gracefully...")
    producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)




# Main
def main():
    ws_app = WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    # handle ping/pong
    ws_app.run_forever(ping_interval=15, ping_timeout=10)

if __name__ == "__main__":
    main()
