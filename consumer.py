"""
Kafka Consumer - reads messages from the user-events topic.
Data-Study-06-13 (DAY 30) - Topic 5: Build a Simple Consumer
"""

import json
import sys

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "user-events"
GROUP_ID = "user-events-consumer-group"


def create_consumer() -> KafkaConsumer:
    """Connect to Kafka and subscribe to the user-events topic."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def display_message(message) -> None:
    """Print a received message and its metadata to the terminal."""
    print(f"Received: {message.value}")
    print(f"  Topic:     {message.topic}")
    print(f"  Partition: {message.partition}")
    print(f"  Offset:    {message.offset}")
    print(f"  Key:       {message.key}")
    print("-" * 40)


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS[0]}...")
    print(f"Subscribed to topic: '{TOPIC}'")
    print("Waiting for messages. Press Ctrl+C to stop.\n")

    try:
        consumer = create_consumer()
    except NoBrokersAvailable:
        print(
            "Error: Could not connect to Kafka. "
            "Make sure Kafka is running (see README.md).",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        for message in consumer:
            display_message(message)
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
