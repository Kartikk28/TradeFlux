from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer
import threading
import json
import asyncio
from collections import deque
from threading import Lock

app = FastAPI()

consumer = KafkaConsumer(
    "market_data",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="market-logger-group",
)

# Store last 100 ticks in a thread-safe deque
recent_ticks = deque(maxlen=100)
tick_lock = Lock()

# For websocket broadcasting
websocket_clients = set()
ws_clients_lock = Lock()

def consume():
    for message in consumer:
        tick = message.value
        with tick_lock:
            recent_ticks.append(tick)
        # Broadcast to websocket clients
        with ws_clients_lock:
            for ws in list(websocket_clients):
                try:
                    asyncio.run_coroutine_threadsafe(ws.send_json(tick), ws.loop)
                except Exception:
                    pass  # Ignore errors for now
        print(f"Received message: {tick}")

@app.get("/health")
async def health():
    return {"status": "MarketLogger is running"}

@app.get("/market_data")
async def get_market_data():
    with tick_lock:
        return list(recent_ticks)

@app.websocket("/ws/market_data")
async def websocket_market_data(websocket: WebSocket):
    await websocket.accept()
    # Register client
    websocket.loop = asyncio.get_event_loop()
    with ws_clients_lock:
        websocket_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        with ws_clients_lock:
            websocket_clients.discard(websocket)

# Start the consumer in a background thread
threading.Thread(target=consume, daemon=True).start()
