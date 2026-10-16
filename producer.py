"""
producer.py
Simulates a stream of user activity events and sends them to a Kafka topic.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC = "user-events"
BOOTSTRAP_SERVERS = "localhost:9092"

# Simulated data pools
USERS = [f"user_{i}" for i in range(1, 21)] + ["bot_001", "bot_002"]
EVENT_TYPES = ["click", "page_view", "scroll", "purchase", "logout"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/about", "/contact"]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_event():
    return {
        "user_id": random.choice(USERS),
        "event_type": random.choice(EVENT_TYPES),
        "page": random.choice(PAGES),
        "session_duration_s": random.randint(1, 600),
        "timestamp": datetime.utcnow().isoformat(),
    }


def main():
    producer = create_producer()
    print(f"[Producer] Connected. Sending events to topic '{TOPIC}'...")

    for i in range(50):
        event = generate_event()
        producer.send(TOPIC, value=event)
        print(f"[Producer] Sent event {i + 1}/50: {event['user_id']} → {event['event_type']} on {event['page']}")
        time.sleep(0.5)  # Simulate real-time pacing

    producer.flush()
    producer.close()
    print("[Producer] Done. All events sent.")


if __name__ == "__main__":
    main()