"""
ingestion/kinesis/consumer_local.py

Reads from LocalStack Kinesis and writes Parquet to LocalStack S3.
Runs for a fixed number of iterations then exits — good for testing.
"""

import os
import sys
import boto3

sys.path.insert(0, "/app")

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
BUCKET   = os.environ.get("S3_BUCKET", "project2-data-lake-dev")
REGION   = "us-east-1"

# Patch boto3 clients to point at LocalStack
original_client   = boto3.client
original_resource = boto3.resource

def patched_client(service, **kwargs):
    kwargs.setdefault("endpoint_url",          ENDPOINT)
    kwargs.setdefault("aws_access_key_id",     "test")
    kwargs.setdefault("aws_secret_access_key", "test")
    kwargs.setdefault("region_name",           REGION)
    return original_client(service, **kwargs)

def patched_resource(service, **kwargs):
    kwargs.setdefault("endpoint_url",          ENDPOINT)
    kwargs.setdefault("aws_access_key_id",     "test")
    kwargs.setdefault("aws_secret_access_key", "test")
    kwargs.setdefault("region_name",           REGION)
    return original_resource(service, **kwargs)

boto3.client   = patched_client
boto3.resource = patched_resource

from ingestion.kinesis.consumer import KinesisConsumer

def main():
    streams = [
        "project2-orders-stream",
        "project2-transactions-stream",
    ]

    for stream_name in streams:
        print(f"\nConsuming from: {stream_name}")
        consumer = KinesisConsumer(
            stream_name   = stream_name,
            s3_bucket     = BUCKET,
            flush_seconds = 5,
            flush_records = 20,
        )
        # Run 10 poll iterations then stop
        consumer.run(max_iterations=10)
        print(f"Consumer finished for {stream_name}")
        print(f"Total records written: {consumer.total_written}")

    print("\nAll consumers done. Check S3:")
    s3 = patched_client("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/")
    files = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
    print(f"Parquet files in bronze zone: {len(files)}")
    for f in files:
        print(f"  s3://{BUCKET}/{f}")

if __name__ == "__main__":
    main()
