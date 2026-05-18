"""
ingestion/kinesis/producer.py

Sends e-commerce and financial events to Kinesis Streams.
This runs on the application side — every order placed, every
payment processed, every click gets published here in real time.

Two streams:
  - project2-orders-stream       (order & clickstream events)
  - project2-transactions-stream (payment & financial events)

Usage:
    python producer.py --stream orders --event-file sample_orders.json
    python producer.py --stream transactions --count 100 --simulate
"""

import argparse
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from typing import Generator

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Stream names ──────────────────────────────────────────────────
STREAMS = {
    "orders":       "project2-orders-stream",
    "transactions": "project2-transactions-stream",
    "clickstream":  "project2-clickstream-stream",
}

# ── Kinesis limits ────────────────────────────────────────────────
MAX_BATCH_SIZE    = 500          # Kinesis PutRecords max
MAX_RECORD_BYTES  = 1_000_000   # 1MB per record
MAX_BATCH_BYTES   = 5_000_000   # 5MB per batch


class KinesisProducer:
    """
    Batched Kinesis producer with automatic retry and error handling.
    Failed records within a batch are retried individually.
    """

    def __init__(self, region: str = "us-east-1"):
        self.client = boto3.client("kinesis", region_name=region)
        self.region = region

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _put_records_batch(self, stream_name: str, records: list) -> dict:
        return self.client.put_records(
            StreamName=stream_name,
            Records=records,
        )

    def send_events(self, stream_name: str, events: list[dict]) -> dict:
        """
        Send a list of events to Kinesis in batches of 500.
        Returns a summary of successes and failures.
        """
        total_sent    = 0
        total_failed  = 0
        batches       = 0

        # Chunk into batches
        for i in range(0, len(events), MAX_BATCH_SIZE):
            batch_events = events[i : i + MAX_BATCH_SIZE]
            records = []

            for event in batch_events:
                payload = json.dumps(event, default=str).encode("utf-8")
                if len(payload) > MAX_RECORD_BYTES:
                    log.warning(f"Record too large ({len(payload)} bytes), skipping")
                    total_failed += 1
                    continue
                records.append({
                    "Data":         payload,
                    "PartitionKey": event.get("customer_id", str(uuid.uuid4())),
                })

            if not records:
                continue

            try:
                response = self._put_records_batch(stream_name, records)
                failed = response.get("FailedRecordCount", 0)

                if failed > 0:
                    log.warning(f"Batch {batches}: {failed} records failed — retrying individually")
                    # Retry failed records one by one
                    for j, record in enumerate(response["Records"]):
                        if "ErrorCode" in record:
                            try:
                                self.client.put_record(
                                    StreamName=stream_name,
                                    Data=records[j]["Data"],
                                    PartitionKey=records[j]["PartitionKey"],
                                )
                                total_sent += 1
                            except ClientError as e:
                                log.error(f"Individual retry failed: {e}")
                                total_failed += 1
                        else:
                            total_sent += 1
                else:
                    total_sent += len(records)

                batches += 1
                log.info(f"Batch {batches}: sent {len(records)} records to {stream_name}")

            except ClientError as e:
                log.error(f"Batch {batches} failed: {e}")
                total_failed += len(records)

        return {
            "stream":        stream_name,
            "total_events":  len(events),
            "sent":          total_sent,
            "failed":        total_failed,
            "batches":       batches,
        }


# ── Event generators (simulation mode) ───────────────────────────

COUNTRIES   = ["US", "GB", "CA", "DE", "FR", "AU", "NG", "GH", "JP", "SG"]
CURRENCIES  = ["USD", "EUR", "GBP", "CAD", "AUD", "NGN"]
STATUSES    = ["pending", "confirmed", "shipped", "delivered"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
DEVICES     = ["mobile", "desktop", "tablet"]
CATEGORIES  = ["electronics", "fashion", "food", "beauty", "sports", "home"]


def generate_order_event() -> dict:
    customer_id = f"cust_{random.randint(1000, 9999)}"
    merchant_id = f"merch_{random.randint(100, 499)}"
    gross       = round(random.uniform(10, 2000), 2)
    discount    = round(gross * random.uniform(0, 0.3), 2) if random.random() > 0.6 else 0

    return {
        "event_type":      "order",
        "order_id":        f"ord_{uuid.uuid4().hex[:12]}",
        "customer_id":     customer_id,
        "merchant_id":     merchant_id,
        "order_status":    random.choice(STATUSES),
        "order_ts":        datetime.now(timezone.utc).isoformat(),
        "currency":        random.choice(CURRENCIES),
        "gross_amount":    gross,
        "discount_amount": discount,
        "item_count":      random.randint(1, 10),
        "payment_method":  random.choice(PAYMENT_METHODS),
        "shipping_country":random.choice(COUNTRIES),
        "source_system":   "ecommerce_app",
        "ingest_ts":       datetime.now(timezone.utc).isoformat(),
    }


def generate_transaction_event() -> dict:
    customer_id = f"cust_{random.randint(1000, 9999)}"
    merchant_id = f"merch_{random.randint(100, 499)}"
    amount      = round(random.uniform(5, 5000), 2)
    is_intl     = random.random() > 0.7

    return {
        "event_type":       "transaction",
        "transaction_id":   f"txn_{uuid.uuid4().hex[:12]}",
        "order_id":         f"ord_{uuid.uuid4().hex[:12]}" if random.random() > 0.3 else None,
        "customer_id":      customer_id,
        "merchant_id":      merchant_id,
        "transaction_ts":   datetime.now(timezone.utc).isoformat(),
        "amount":           amount,
        "currency":         random.choice(CURRENCIES),
        "transaction_type": "purchase",
        "payment_method":   random.choice(PAYMENT_METHODS),
        "status":           "completed",
        "card_bin":         str(random.randint(400000, 499999)),
        "card_last4":       str(random.randint(1000, 9999)),
        "ip_country":       random.choice(COUNTRIES),
        "device_type":      random.choice(DEVICES),
        "merchant_category":random.choice(CATEGORIES),
        "is_international": is_intl,
        "source_system":    "payments_service",
        "ingest_ts":        datetime.now(timezone.utc).isoformat(),
    }


def generate_clickstream_event() -> dict:
    return {
        "event_type":   "click",
        "session_id":   f"sess_{uuid.uuid4().hex[:10]}",
        "customer_id":  f"cust_{random.randint(1000, 9999)}",
        "page":         random.choice(["/home", "/product", "/cart", "/checkout", "/search"]),
        "product_id":   f"prod_{random.randint(1000, 9999)}" if random.random() > 0.4 else None,
        "device_type":  random.choice(DEVICES),
        "country":      random.choice(COUNTRIES),
        "event_ts":     datetime.now(timezone.utc).isoformat(),
        "source_system":"web_app",
    }


EVENT_GENERATORS = {
    "orders":      generate_order_event,
    "transactions": generate_transaction_event,
    "clickstream": generate_clickstream_event,
}


def simulate_stream(
    producer: KinesisProducer,
    stream_type: str,
    count: int,
    rate_per_second: float = 10.0,
) -> None:
    """Continuously send simulated events at a controlled rate."""
    stream_name = STREAMS[stream_type]
    generator   = EVENT_GENERATORS[stream_type]
    batch_size  = max(1, int(rate_per_second))
    interval    = batch_size / rate_per_second

    sent = 0
    log.info(f"Simulating {count} {stream_type} events at {rate_per_second}/sec → {stream_name}")

    while sent < count:
        batch = [generator() for _ in range(min(batch_size, count - sent))]
        result = producer.send_events(stream_name, batch)
        sent += result["sent"]
        log.info(f"Progress: {sent}/{count} events sent")
        if sent < count:
            time.sleep(interval)

    log.info(f"Simulation complete: {sent} events sent to {stream_name}")


def main():
    parser = argparse.ArgumentParser(description="Kinesis event producer for Project 2")
    parser.add_argument("--stream", choices=list(STREAMS.keys()), required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--simulate", action="store_true", help="Generate synthetic events")
    parser.add_argument("--count", type=int, default=100, help="Events to simulate")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--event-file", help="JSON file with events to send")
    args = parser.parse_args()

    producer = KinesisProducer(region=args.region)

    if args.simulate:
        simulate_stream(producer, args.stream, args.count, args.rate)
    elif args.event_file:
        with open(args.event_file) as f:
            events = json.load(f)
        result = producer.send_events(STREAMS[args.stream], events)
        log.info(f"Result: {result}")
    else:
        parser.error("Either --simulate or --event-file is required")


if __name__ == "__main__":
    main()
