# Real-Time Streaming Pipeline with Kafka

A hands-on demo of a real-time event streaming pipeline using **Apache Kafka** and **Python**. A producer simulates a stream of user activity events, and a consumer reads those events, transforms them (ETL), and writes the results to a local CSV file.

## Skills Demonstrated
- Python (kafka-python, json, csv, datetime)
- ETL (Extract → Transform → Load)
- Real-time data streaming concepts
- Apache Kafka producer/consumer pattern

## Project Structure

```
kafka-streaming-pipeline/
├── README.md
├── requirements.txt
├── producer.py        # Simulates and sends events to a Kafka topic
├── consumer.py        # Reads, transforms, and loads events to CSV
```

## Prerequisites

1. **Python 3.8+**
2. **Apache Kafka** running locally (with Zookeeper)

### Quick Kafka Setup (using the official binary)

```bash
# Download Kafka from https://kafka.apache.org/downloads
# Extract and start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# In a new terminal, start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Create the topic
bin/kafka-topics.sh --create --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

> On Windows, use `bin/windows/*.bat` scripts instead.

## Setup

```bash
git clone https://github.com/your-username/kafka-streaming-pipeline.git
cd kafka-streaming-pipeline
pip install -r requirements.txt
```

## Running the Pipeline

Open **two terminals**:

**Terminal 1 — Start the consumer first:**
```bash
python consumer.py
```

**Terminal 2 — Start the producer:**
```bash
python producer.py
```

The producer will send 50 simulated user events to the `user-events` Kafka topic. The consumer reads each event, applies transformations, and appends the results to `output/processed_events.csv`.

## ETL Overview

| Stage | What Happens |
|-------|-------------|
| **Extract** | Consumer reads raw JSON messages from the Kafka topic |
| **Transform** | Filters bot users, adds a `processed_at` timestamp, uppercases event type, calculates session duration bucket |
| **Load** | Writes cleaned records to a CSV file |

## Sample Output (`output/processed_events.csv`)

```
user_id,event_type,page,session_duration_s,duration_bucket,processed_at
user_42,CLICK,/home,45,short,2024-01-15 10:23:01
user_7,PAGE_VIEW,/products,182,medium,2024-01-15 10:23:02
```

## Notes
- The consumer creates the `output/` directory automatically.
- Run the consumer before the producer so no messages are missed.
- Kafka must be running on `localhost:9092` before starting either script.