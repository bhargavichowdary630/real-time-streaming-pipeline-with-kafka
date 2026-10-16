"""
consumer.py
Reads user activity events from a Kafka topic, transforms them (ETL),
and writes the processed records to a CSV file.
"""

import json
import csv
import os
from datetime import datetime
from kafka import KafkaConsumer

TOPIC = "user-events"
BOOTSTRAP_SERVERS = "localhost:9092"
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "processed_events.csv")

# Bot user IDs to filter out
BOT_USERS = {"bot_001", "bot_002"}

CSV_HEADERS = [
    "user_id",
    "event_type",
    "page",
    "session_duration_s",
    "duration_bucket",
    "processed_at",
]


def get_duration_bucket(seconds):
    """Categorize session duration into human-readable buckets."""
    if seconds < 60:
        return "short"
    elif seconds < 300:
        return "medium"
    else:
        return "long"


def transform(raw_event):
    """
    Transform stage of ETL:
    - Filter bot users (return None to skip)
    - Uppercase the event type
    - Add a processed_at timestamp
    - Bucket the session duration
    """
    user_id = raw_event.get("user_id", "")

    # Filter: drop bot traffic
    if user_id in BOT_USERS:
        return None

    return {
        "user_id": user_id,
        "event_type": raw_event.get("event_type", "").upper(),
        "page": raw_event.get("page", ""),
        "session_duration_s": raw_event.get("session_duration_s", 0),
        "duration_bucket": get_duration_bucket(raw_event.get("session_duration_s", 0)),
        "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="etl-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10000,  # Stop after 10s of no new messages
    )


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"[Consumer] Listening on topic '{TOPIC}'...")
    print(f"[Consumer] Writing output to '{OUTPUT_FILE}'")

    consumer = create_consumer()
    processed_count = 0
    skipped_count = 0

    with open(OUTPUT_FILE, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
        writer.writeheader()

        for message in consumer:
            raw_event = message.value

            # Transform
            record = transform(raw_event)

            if record is None:
                skipped_count += 1
                print(f"[Consumer] Skipped bot event from {raw_event.get('user_id')}")
                continue

            # Load
            writer.writerow(record)
            csvfile.flush()  # Ensure each record is written immediately
            processed_count += 1
            print(
                f"[Consumer] Processed: {record['user_id']} | "
                f"{record['event_type']} | {record['page']} | "
                f"bucket={record['duration_bucket']}"
            )

    consumer.close()
    print(f"\n[Consumer] Done.")
    print(f"  Events processed : {processed_count}")
    print(f"  Events skipped   : {skipped_count} (bots filtered)")
    print(f"  Output saved to  : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()