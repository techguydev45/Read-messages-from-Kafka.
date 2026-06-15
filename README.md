# Read Messages from Kafka

Kafka Producer & Consumer System — **Data-Study-06-13 (DAY 30), Topic 5: Build a Simple Consumer**

Read incoming messages from the `user-events` topic and display them in the terminal.

## Project Structure

```
.
├── consumer.py        # Kafka consumer (main deliverable)
├── producer.py        # Sample producer for testing
├── docker-compose.yml # Local Kafka + Zookeeper
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.8+
- Docker Desktop (for local Kafka)

## Setup

### 1. Start Kafka

```bash
docker compose up -d
```

Wait ~30 seconds for Kafka to be ready.

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Terminal 1 — Start the consumer

```bash
python consumer.py
```

The consumer connects to Kafka, subscribes to `user-events`, and prints each message.

### Terminal 2 — Send test messages

```bash
python producer.py
```

You should see messages appear in the consumer terminal.

### Stop and restart the consumer

1. Press `Ctrl+C` in the consumer terminal to stop.
2. Run `python consumer.py` again.

Because the consumer uses `auto_offset_reset="earliest"` and a consumer group, restarted consumers continue from the last committed offset. To re-read all messages, change the `group_id` in `consumer.py` or reset offsets.

## Example Output

```
Connecting to Kafka at localhost:9092...
Subscribed to topic: 'user-events'
Waiting for messages. Press Ctrl+C to stop.

Received: {'user_id': 101, 'event': 'login', 'timestamp': '2026-06-13T08:00:00Z'}
  Topic:     user-events
  Partition: 0
  Offset:    0
  Key:       None
----------------------------------------
Received: {'user_id': 102, 'event': 'page_view', 'page': '/home', 'timestamp': '2026-06-13T08:01:00Z'}
  Topic:     user-events
  Partition: 0
  Offset:    1
  Key:       None
----------------------------------------
```

## Stop Kafka

```bash
docker compose down
```
