import json
import random
import time
from kafka import KafkaProducer

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

symbols = ["AAPL", "GOOG", "TSLA", "AMZN", "MSFT"]

print("Starting to produce fake market data...")

try:
    while True:
        # Randomly select a symbol
        symbol = random.choice(symbols)
        # Generate a fake price
        price = round(random.uniform(100, 500), 2)
        # Create a message
        message = {"symbol": symbol, "price": price}

        # Send message to Kafka topic "market_data"
        producer.send("market_data", message)

        print(f"Produced: {message}")

        # Wait 2 seconds before producing the next message
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopped producing.")

