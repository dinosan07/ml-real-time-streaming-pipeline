import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    transaction = {
        "amount": random.randint(100, 5000),
        "user_id": random.randint(1, 100),
        "location": random.choice(["India", "USA", "UK"]),
        "device": random.choice(["Mobile", "Laptop"]),
    }

    producer.send("transactions", transaction)
    print("Sent:", transaction)
    time.sleep(2)
