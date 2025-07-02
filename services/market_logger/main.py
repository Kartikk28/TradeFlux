from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json

app = FastAPI()

consumer = KafkaConsumer(
    "market_data",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="market-logger-group",
)

def consume():
    for message in consumer:
        print(f"Received message: {message.value}")

@app.get("/health")
async def health():
    return {"status": "MarketLogger is running"}

# Start the consumer in a background thread
threading.Thread(target=consume, daemon=True).start()
