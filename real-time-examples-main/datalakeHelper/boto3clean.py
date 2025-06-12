import boto3
import os
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables from .env file.
load_dotenv()

# Read S3 credentials and configuration from environment variables.
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
endpoint_url = os.getenv("ENDPOINT_URL", "http://10.32.38.210")
bucket_name = os.getenv("BUCKET_NAME", "dream-data")

# Create the S3 client using the credentials from the .env file.
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url,
    config=Config(signature_version='s3v4')
)

def clean_bucket(bucket_name):
    """
    Deletes all objects from the specified S3 bucket.
    Uses a paginator to handle buckets with more than 1000 objects.
    """
    paginator = s3.get_paginator('list_objects_v2')
    total_deleted = 0

    for page in paginator.paginate(Bucket=bucket_name):
        # Check if the page has contents.
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            if objects_to_delete:
                response = s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                deleted = response.get('Deleted', [])
                total_deleted += len(deleted)
                print(f"Deleted {len(deleted)} objects from {bucket_name}.")
                errors = response.get('Errors', [])
                if errors:
                    print("Errors encountered during deletion:")
                    for error in errors:
                        print(error)
    print(f"Total objects deleted from bucket {bucket_name}: {total_deleted}")

if __name__ == '__main__':
    clean_bucket(bucket_name)

