from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_tick():
    return {
        'symbol': 'XYZ',
        'price': round(100 + random.uniform(-1, 1), 2),
        'timestamp': time.time()
    }

if __name__ == '__main__':
    while True:
        tick = generate_tick()
        producer.send('market_data', tick)
        print(f"Sent {tick}")
        time.sleep(1)  # 1 tick per second
