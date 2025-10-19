import json
import signal
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from websocket import WebSocketApp

# load environment
load_dotenv("../.env")

# config
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_BROKERS = "host.docker.internal:9092"
KAFKA_TOPIC = "btcusdt_trades"
ws_app = None


# kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8")
)


def send_to_kafka(msg):
    try:
        producer.send(
            KAFKA_TOPIC,
            key="BTCUSDT",
            value=msg
        )
        producer.flush()
    except Exception as e:
        print(f"[ERROR] Kafka send failed: {e}")


# websocket event handlers
def on_message(ws, message):
    try:
        data = json.loads(message)
        send_to_kafka(data)
    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")


def on_error(ws, error):
    print(f"[ERROR] WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[INFO] WebSocket closed. Code={close_status_code}, Msg={close_msg}")


def on_open(ws):
    print("[INFO] Connected to Binance WebSocket")


# graceful shutdown
def signal_handler(sig, frame):
    print("\n[INFO] Shutting down gracefully...")
    if ws_app:
        ws_app.close()
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)





def main():
    global ws_app
    ws_app = WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    print("[INFO] Starting WebSocket listener...")
    ws_app.run_forever(ping_interval=15, ping_timeout=10)

if __name__ == "__main__":
    main()
