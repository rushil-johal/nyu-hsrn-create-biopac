import boto3
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
key = 'dvnasnyukttumpwtcacumxheg_2025-03-07T18-56-26.358932Z.json'  # File to download
local_filename = 'dvnasnyukttumpwtcacumxheg_2025-03-07T18-56-26.358932Z.json'  # Name of the file to save to

try:
    s3.download_file(bucket_name, key, local_filename)
    print(f"DOWNLOAD SUCCESS: {key} from bucket {bucket_name} to local file {local_filename}")
except Exception as e:
    print(f"DOWNLOAD FAIL: {key} from {bucket_name}, ERORR: {e}")

