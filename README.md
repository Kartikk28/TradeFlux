<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<title>TradeFlux – Real-time Market Data Pipeline</title>
<style>
  body {
    font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
    max-width: 900px;
    margin: 0 auto;
    padding: 2em;
    background: #f9f9f9;
    color: #333;
  }
  h1, h2, h3 {
    color: #2c3e50;
  }
  pre {
    background: #eee;
    padding: 1em;
    overflow: auto;
    border-radius: 4px;
  }
  code {
    background: #e1e1e1;
    padding: 0.2em 0.4em;
    border-radius: 3px;
  }
  .highlight {
    background: #dff0d8;
    border-left: 4px solid #3c763d;
    padding: 0.5em;
    margin: 1em 0;
  }
</style>
</head>
<body>

<h1>📈 TradeFlux</h1>

<p><strong>TradeFlux – Real-time Market Data Pipeline</strong></p>

<p>TradeFlux is a microservices-based project demonstrating how to simulate, stream, and consume real-time market data using Kafka, Docker, and Python.</p>

<hr/>

<h2>🚀 Overview</h2>
<ul>
  <li><strong>Produce</strong> fake market data with a Kafka producer (<code>producer.py</code>).</li>
  <li><strong>Stream</strong> data through Apache Kafka.</li>
  <li><strong>Consume</strong> and log data using a FastAPI microservice (<code>market_logger</code>).</li>
  <li><strong>Verify</strong> system health via a REST endpoint (<code>/health</code>).</li>
</ul>

<hr/>

<h2>🗂️ Project Structure</h2>
<pre>
TradeFlux/
├─ docker-compose.yml        # Kafka, Zookeeper, FastAPI
├─ README.md
├─ venv/
├─ producer.py               # Producer script
└─ services/
    └─ market_logger/
        ├─ main.py           # FastAPI consumer
        └─ Dockerfile
</pre>

<hr/>

<h2>⚙️ Prerequisites</h2>
<ul>
  <li>Docker & Docker Compose</li>
  <li>Python 3.9+</li>
</ul>
<p>(Optional: kubectl & minikube for Kubernetes deployments.)</p>

<hr/>

<h2>🐳 Docker Compose Configuration</h2>
<p>This <code>docker-compose.yml</code> sets up Kafka, Zookeeper, and the FastAPI consumer:</p>
<pre>
version: "3.8"

services:
  zookeeper:
    image: bitnami/zookeeper:latest
    environment:
      ALLOW_ANONYMOUS_LOGIN: "yes"
    ports:
      - "2181:2181"

  kafka:
    image: bitnami/kafka:3.4
    environment:
      KAFKA_BROKER_ID: "1"
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      ALLOW_PLAINTEXT_LISTENER: "yes"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    ports:
      - "9092:9092"

  market_logger:
    build:
      context: .
      dockerfile: services/market_logger/Dockerfile
    depends_on:
      - kafka
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    ports:
      - "8000:8000"
</pre>

<hr/>

<h2>🐍 Market Data Producer</h2>
<p><code>producer.py</code> generates fake price ticks every second:</p>
<pre>
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_tick():
    return {
        'symbol': random.choice(['AAPL', 'GOOG', 'MSFT', 'TSLA']),
        'price': round(random.uniform(100, 500), 2),
        'timestamp': time.time()
    }

while True:
    tick = generate_tick()
    producer.send('market_data', tick)
    print("Produced:", tick)
    time.sleep(1)
</pre>

<hr/>

<h2>📥 FastAPI Market Logger</h2>
<p><code>main.py</code> subscribes to <code>market_data</code> and logs messages:</p>
<pre>
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
        print(f"Received: {message.value}")

@app.get("/health")
async def health():
    return {"status": "MarketLogger is running"}

threading.Thread(target=consume, daemon=True).start()
</pre>

<hr/>

<h2>🔗 How It All Fits Together</h2>
<p><strong>Data Flow:</strong></p>
<ol>
  <li><code>producer.py</code> creates fake ticks and sends them to Kafka topic <code>market_data</code>.</li>
  <li><code>market_logger</code> listens and prints received ticks.</li>
  <li>Health check: <code>curl http://localhost:8000/health</code></li>
</ol>

<hr/>

<h2>✅ Quick Start</h2>
<ol>
  <li><strong>Build & start services:</strong>
    <pre>docker-compose up --build</pre>
  </li>
  <li><strong>In another terminal, run producer:</strong>
    <pre>source venv/bin/activate
python producer.py</pre>
  </li>
  <li><strong>Watch logs:</strong>
    <pre>docker-compose logs -f market_logger</pre>
  </li>
</ol>

<hr/>

<h2>🌟 Why This Matters</h2>
<p>
This project demonstrates:
<ul>
  <li>Event-driven microservices with Kafka</li>
  <li>Publish/Subscribe communication patterns</li>
  <li>How trading platforms ingest and process real-time data</li>
</ul>
</p>

<hr/>

<h2>🆕 New API Endpoints</h2>
<ul>
  <li><strong>GET <code>/market_data</code></strong>: Returns the last 100 market ticks as a JSON array.</li>
  <li><strong>WebSocket <code>/ws/market_data</code></strong>: Streams live market ticks to connected clients in real time.</li>
</ul>

<h2>🖥️ Frontend Dashboard</h2>
<ul>
  <li>Modern React-based dashboard (see <code>frontend/</code> directory).</li>
  <li>Displays live market data in a beautiful, responsive table.</li>
  <li>Health check indicator for backend status.</li>
  <li>Supports both historical (last 100 ticks) and real-time updates via WebSocket.</li>
  <li>Dark/light mode support and mobile-friendly design.</li>
</ul>

<h3>How to Use the API</h3>
<ul>
  <li>To fetch recent ticks: <code>curl http://localhost:8000/market_data</code></li>
  <li>To receive live updates: connect to <code>ws://localhost:8000/ws/market_data</code> using a WebSocket client.</li>
</ul>

<hr/>

<p align="center">🚀 Happy Coding!</p>

</body>
</html>
