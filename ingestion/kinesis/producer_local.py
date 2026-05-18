"""
ingestion/kinesis/producer_local.py

Sends simulated events to LocalStack Kinesis for local testing.
Runs 3 rounds of events across all streams then exits cleanly.
"""

import json
import os
import sys
import time

sys.path.insert(0, "/app")

import boto3
from ingestion.kinesis.producer import (
    generate_order_event,
    generate_transaction_event,
    generate_clickstream_event,
    KinesisProducer,
    STREAMS,
)

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION   = "us-east-1"

class LocalProducer(KinesisProducer):
    def __init__(self):
        self.client = boto3.client(
            "kinesis",
            region_name=REGION,
            endpoint_url=ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

def main():
    producer = LocalProducer()

    runs = [
        ("orders",       generate_order_event,       50),
        ("transactions", generate_transaction_event,  30),
        ("clickstream",  generate_clickstream_event,  80),
    ]

    for stream_type, generator, count in runs:
        stream_name = STREAMS[stream_type]
        events = [generator() for _ in range(count)]
        print(f"Sending {count} {stream_type} events to {stream_name}...")
        result = producer.send_events(stream_name, events)
        print(f"  Result: {result}")
        time.sleep(1)

    print("\nAll events sent. Producer done.")

if __name__ == "__main__":
    main()
