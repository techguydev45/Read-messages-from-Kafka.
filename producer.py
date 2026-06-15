"""
Kafka Producer - sends sample user events for testing the consumer.
"""

import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "user-events"

SAMPLE_EVENTS = [
    {"user_id": 101, "event": "login", "timestamp": "2026-06-13T08:00:00Z"},
    {"user_id": 102, "event": "page_view", "page": "/home", "timestamp": "2026-06-13T08:01:00Z"},
    {"user_id": 101, "event": "add_to_cart", "product_id": "SKU-42", "timestamp": "2026-06-13T08:02:00Z"},
    {"user_id": 103, "event": "signup", "timestamp": "2026-06-13T08:03:00Z"},
    {"user_id": 102, "event": "purchase", "amount": 29.99, "timestamp": "2026-06-13T08:04:00Z"},
]


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS[0]}...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print(
            "Error: Could not connect to Kafka. "
            "Make sure Kafka is running (see README.md).",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Sending {len(SAMPLE_EVENTS)} messages to '{TOPIC}'...\n")

    for event in SAMPLE_EVENTS:
        producer.send(TOPIC, value=event)
        print(f"Sent: {event}")
        time.sleep(0.5)

    producer.flush()
    producer.close()
    print("\nAll messages sent.")


if __name__ == "__main__":
    main()
