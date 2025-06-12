import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv

load_dotenv()

# S3 client initialization.
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url='http://10.32.38.210',
    config=Config(signature_version='s3v4')
)

bucket_name = 'dream-data'

def list_all_objects(bucket):
    """List all objects in the given S3 bucket, handling pagination if needed."""
    all_objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket)
        
        if 'Contents' in response:
            all_objects.extend(response['Contents'])
        else:
            break

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return all_objects

# Retrieve all objects from the bucket.
objects = list_all_objects(bucket_name)

if not objects:
    print("No objects found in the bucket.")
else:
    # Sort objects by the LastModified timestamp (most recent first).
    sorted_objects = sorted(objects, key=lambda obj: obj['LastModified'], reverse=True)

    # Download each file.
    for obj in sorted_objects:
        key = obj['Key']
        # Use os.path.basename to avoid creating nested directories locally.
        local_filename = os.path.basename(key)
        try:
            s3.download_file(bucket_name, key, local_filename)
            print(f"DOWNLOAD SUCCESS: {key} from bucket {bucket_name} to local file {local_filename}")
        except Exception as e:
            print(f"DOWNLOAD FAIL: {key} from bucket {bucket_name}, ERROR: {e}")
