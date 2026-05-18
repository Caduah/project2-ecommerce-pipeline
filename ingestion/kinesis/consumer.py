"""
ingestion/kinesis/consumer.py

Reads events from Kinesis Streams and writes them to S3 bronze zone
as partitioned Parquet files.

This is the bridge between real-time streaming and the batch pipeline:
  Kinesis → Consumer → S3 bronze (partitioned by date) → Airflow picks up

In production this runs as an ECS task or Lambda (see lambda/ folder).
Locally it runs as a continuous process for testing.

Two flush strategies:
  1. Time-based: flush every N seconds (default 60s)
  2. Size-based:  flush when buffer hits N records (default 10,000)
"""

import argparse
import json
import logging
import time
import os
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
STREAM_TO_PREFIX = {
    "project2-orders-stream":       "bronze/ecommerce/orders",
    "project2-transactions-stream": "bronze/financial/transactions",
    "project2-clickstream-stream":  "bronze/ecommerce/clickstream",
}

DEFAULT_FLUSH_SECONDS = 60
DEFAULT_FLUSH_RECORDS = 10_000
SHARD_ITERATOR_TYPE  = "LATEST"   # TRIM_HORIZON to reprocess from start


class KinesisConsumer:
    """
    Polls Kinesis shards, buffers records, and flushes to S3 as Parquet.
    Checkpoints shard iterators to avoid reprocessing on restart.
    """

    def __init__(
        self,
        stream_name: str,
        s3_bucket: str,
        region: str = "us-east-1",
        flush_seconds: int = DEFAULT_FLUSH_SECONDS,
        flush_records: int = DEFAULT_FLUSH_RECORDS,
    ):
        self.stream_name   = stream_name
        self.s3_bucket     = s3_bucket
        self.s3_prefix     = STREAM_TO_PREFIX.get(stream_name, "bronze/unknown")
        self.flush_seconds = flush_seconds
        self.flush_records = flush_records

        self.kinesis = boto3.client("kinesis", region_name=region)
        self.s3      = boto3.client("s3", region_name=region)

        self.buffer: list[dict]         = []
        self.shard_iterators: dict[str] = {}
        self.last_flush                 = time.time()
        self.total_written              = 0

    def _get_shards(self) -> list[str]:
        resp = self.kinesis.describe_stream_summary(StreamName=self.stream_name)
        shard_count = resp["StreamDescriptionSummary"]["OpenShardCount"]
        shards_resp = self.kinesis.list_shards(StreamName=self.stream_name)
        return [s["ShardId"] for s in shards_resp["Shards"]]

    def _get_shard_iterator(self, shard_id: str) -> str:
        """Get or refresh a shard iterator."""
        if shard_id not in self.shard_iterators:
            resp = self.kinesis.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType=SHARD_ITERATOR_TYPE,
            )
            self.shard_iterators[shard_id] = resp["ShardIterator"]
        return self.shard_iterators[shard_id]

    def _poll_shard(self, shard_id: str) -> int:
        """Poll one shard and add records to buffer. Returns record count."""
        iterator = self._get_shard_iterator(shard_id)
        try:
            resp = self.kinesis.get_records(
                ShardIterator=iterator,
                Limit=1000,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ExpiredIteratorException":
                log.warning(f"Shard iterator expired for {shard_id}, refreshing")
                del self.shard_iterators[shard_id]
                return 0
            raise

        # Update iterator for next poll
        self.shard_iterators[shard_id] = resp["NextShardIterator"]

        records = []
        for record in resp["Records"]:
            try:
                data = json.loads(record["Data"].decode("utf-8"))
                data["_kinesis_seq"]           = record["SequenceNumber"]
                data["_kinesis_partition_key"] = record["PartitionKey"]
                data["_kinesis_arrival_ts"]    = record["ApproximateArrivalTimestamp"].isoformat()
                records.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning(f"Failed to decode record: {e}")

        self.buffer.extend(records)
        return len(records)

    def _should_flush(self) -> bool:
        if len(self.buffer) >= self.flush_records:
            return True
        if time.time() - self.last_flush >= self.flush_seconds:
            return True
        return False

    def _flush_to_s3(self) -> None:
        if not self.buffer:
            return

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y-%m-%d")
        hour_partition = now.strftime("%H")
        ts_suffix      = now.strftime("%Y%m%d_%H%M%S")
        record_count   = len(self.buffer)

        s3_key = (
            f"{self.s3_prefix}/"
            f"date={date_partition}/"
            f"hour={hour_partition}/"
            f"kinesis_{ts_suffix}_{record_count}.parquet"
        )

        # Convert to Parquet via PyArrow
        # Flatten any nested dicts to strings for schema stability
        flat_records = []
        for r in self.buffer:
            flat = {}
            for k, v in r.items():
                if isinstance(v, (dict, list)):
                    flat[k] = json.dumps(v)
                else:
                    flat[k] = v
            flat_records.append(flat)

        table  = pa.Table.from_pylist(flat_records)
        buffer = BytesIO()
        pq.write_table(
            table,
            buffer,
            compression="snappy",
            write_statistics=True,
        )
        buffer.seek(0)

        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
            Metadata={
                "record-count": str(record_count),
                "stream-name":  self.stream_name,
                "flush-ts":     now.isoformat(),
            },
        )

        self.total_written += record_count
        log.info(
            f"Flushed {record_count} records to s3://{self.s3_bucket}/{s3_key} "
            f"(total: {self.total_written})"
        )

        self.buffer.clear()
        self.last_flush = time.time()

    def run(self, max_iterations: int = None) -> None:
        """
        Main polling loop. Runs indefinitely unless max_iterations is set.
        """
        log.info(f"Starting consumer for stream: {self.stream_name}")
        shard_ids   = self._get_shards()
        iterations  = 0

        log.info(f"Found {len(shard_ids)} shards: {shard_ids}")

        while True:
            total_polled = 0
            for shard_id in shard_ids:
                count = self._poll_shard(shard_id)
                total_polled += count

            if total_polled == 0:
                # No records — back off to avoid hot polling
                time.sleep(1)

            if self._should_flush():
                self._flush_to_s3()

            iterations += 1
            if max_iterations and iterations >= max_iterations:
                log.info(f"Reached max iterations ({max_iterations}), flushing and stopping")
                self._flush_to_s3()
                break

        log.info(f"Consumer stopped. Total records written: {self.total_written}")


def main():
    parser = argparse.ArgumentParser(description="Kinesis → S3 consumer for Project 2")
    parser.add_argument("--stream",        required=True, help="Kinesis stream name")
    parser.add_argument("--bucket",        required=True, help="S3 bucket name")
    parser.add_argument("--region",        default="us-east-1")
    parser.add_argument("--flush-seconds", type=int, default=DEFAULT_FLUSH_SECONDS)
    parser.add_argument("--flush-records", type=int, default=DEFAULT_FLUSH_RECORDS)
    parser.add_argument("--max-iterations",type=int, default=None,
                        help="Stop after N poll iterations (for testing)")
    args = parser.parse_args()

    consumer = KinesisConsumer(
        stream_name   = args.stream,
        s3_bucket     = args.bucket,
        region        = args.region,
        flush_seconds = args.flush_seconds,
        flush_records = args.flush_records,
    )
    consumer.run(max_iterations=args.max_iterations)


if __name__ == "__main__":
    main()
