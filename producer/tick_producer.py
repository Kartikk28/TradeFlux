
import os, json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "tradeflux.ticks")

SYMBOLS = ["ACME", "MSFT", "COG", "GOW", "TFLX"]  # demo universe

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

price_state = {s: 100.0 + 20*random.random() for s in SYMBOLS}

def next_tick(symbol):
    # simple random walk with volatility
    p = price_state[symbol]
    p *= (1 + random.uniform(-0.002, 0.002))
    p = max(p, 1.0)
    price_state[symbol] = p
    vol = random.randint(10, 500)
    return {
        "symbol": symbol,
        "ts": datetime.now(timezone.utc).isoformat(),
        "price": round(p, 4),
        "volume": vol
    }

def run():
    while True:
        for s in SYMBOLS:
            tick = next_tick(s)
            producer.send(TOPIC, tick)
        producer.flush()
        time.sleep(0.3)  # ~3 ticks/sec total

if __name__ == "__main__":
    print(f"Producing to {TOPIC} @ {BOOTSTRAP} ...")
    run()
