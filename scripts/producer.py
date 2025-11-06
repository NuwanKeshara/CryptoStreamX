from datetime import datetime, timezone
import json
import os
import signal
import sys
from kafka import KafkaProducer
from websocket import WebSocketApp
from dotenv import load_dotenv

load_dotenv("./.env")

BINANCE_WS_URL = os.getenv('BINANCE_WS_URL')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
TOPIC = os.getenv('TOPIC')


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    batch_size=32*1024,
    compression_type="lz4",
    acks=1,
    max_in_flight_requests_per_connection=5
)

def make_connect_record(payload):
    data = payload.get("data")
    if not data or not data.get("e") or not data.get("s"):
        return None 
    
    return {
            "event_type": data["e"],
            "event_time": int(data["E"]),
            "symbol": data["s"],
            "trade_id": int(data.get("t", 0)),
            "price": float(data.get("p", 0.0)),
            "quantity": float(data.get("q", 0.0)),
            "trade_time": int(data["T"]),
            "market_maker": bool(data.get("m", False)),
            "ignore": bool(data.get("M", False))
    }

def send_to_kafka(msg):
    record = make_connect_record(msg)
    if record is None:
        return
    producer.send(TOPIC, value=record)

def on_message(ws, message):
    try:
        data = json.loads(message)
        send_to_kafka(data)
    except Exception as e:
        print(f"Failed to process message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, code, msg):
    print(f"WebSocket closed. Code={code}, Msg={msg}")

def on_open(ws):
    print("Connected to Binance WebSocket")

def signal_handler(sig, frame):
    print("Shutting down gracefully...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    ws_app = WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws_app.run_forever(ping_interval=15, ping_timeout=10)

if __name__ == "__main__":
    main()
