from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'market_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in consumer:
    print("Received:", msg.value)
