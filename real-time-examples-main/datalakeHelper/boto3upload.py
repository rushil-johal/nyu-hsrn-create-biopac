import boto3
import json
import datetime
from botocore.client import Config

import os
from dotenv import load_dotenv

load_dotenv()


# S3 client initialization.
s3 = boto3.client(
    's3',
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url='http://10.32.38.210',
    config=Config(signature_version='s3v4')
)

bucket_name = 'dream-data'
key = 'test_data_inmemory.json'

# Sample json data in memory
data_object = {
    "frame_number": 1,
    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    "data": ["test data 1", "test data 2"]
}

# Convert the data object to a json string
json_data = json.dumps(data_object)

# From memory to S3 directly
try:
    s3.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    print(f"UPLOAD SUCCESS to {bucket_name}/{key}")
except Exception as e:
    print(f"UPLOAD FAILED: {key}, ERROR: {e}")

