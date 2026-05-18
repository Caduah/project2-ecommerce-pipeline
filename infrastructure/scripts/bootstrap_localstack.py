import boto3, os, time
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION = "us-east-1"
BUCKET = "project2-data-lake-dev"
def c(s):
    return boto3.client(s, region_name=REGION, endpoint_url=ENDPOINT, aws_access_key_id="test", aws_secret_access_key="test")
def main():
    print("Bootstrapping...")
    try:
        c("s3").create_bucket(Bucket=BUCKET)
        print("  S3 bucket created")
    except Exception as e:
        print("  S3 bucket exists")
    for name in ["project2-orders-stream","project2-transactions-stream","project2-clickstream-stream"]:
        try:
            c("kinesis").create_stream(StreamName=name, ShardCount=2)
            print("  Kinesis: " + name)
        except Exception:
            print("  Kinesis exists: " + name)
    time.sleep(3)
    print("Bootstrap complete!")
main()
