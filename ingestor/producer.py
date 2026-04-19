#!/usr/bin/env python3
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# Configuration - Using environment variables for Governance
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')
TOPIC_NAME = 'flight_data'

# Initialize the Kafka Producer
print(f"--- Attempting to connect to Kafka at {KAFKA_SERVER} ---", flush=True)

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    print("--- Successfully connected to Kafka! ---", flush=True)
except Exception as e:
    print(f"--- FAILED to connect to Kafka: {e} ---", flush=True)
    raise


def stream_flights():
    file_path = '/app/data/flights.csv'

    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}", flush=True)
        return

    print(f"Reading data from {file_path}...", flush=True)
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower()  # Standardize headers
    for _, row in df.iterrows():
        producer.send(TOPIC_NAME, value=row.to_dict())
    print(f"Loaded {len(df)} rows. Starting stream to Kafka...", flush=True)

    count = 0
    for _, row in df.iterrows():
        message = row.to_dict()

        # Send to Kafka
        producer.send(TOPIC_NAME, value=message)

        count += 1
        if count % 10 == 0:  # Print every 10 rows so the logs aren't too crazy
            print(f"Sent {count} rows to Kafka...", flush=True)

        time.sleep(0.1)

    # Ensure all messages are sent before exiting
    print("Finishing stream and flushing producer...", flush=True)
    producer.flush()
    print("Stream complete!", flush=True)


if __name__ == "__main__":
    try:
        stream_flights()
    except Exception as e:
        print(f"Error: {e}", flush=True)