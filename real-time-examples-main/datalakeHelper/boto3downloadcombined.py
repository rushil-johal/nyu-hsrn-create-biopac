import boto3
from botocore.client import Config
import os
import json
import re
from collections import defaultdict
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
    print(f"Found {len(all_objects)} objects in the bucket. The names are: {all_objects}")
    return all_objects

def parse_filename(key):
    """Parse the filename to extract experiment ID and participant ID."""
    filename = os.path.basename(key)
    # Expected format: {experiment_id}_{participant_id}_{timestamp}.json
    match = re.match(r'(\w+)_(\w+)_.*\.json', filename)
    if match:
        return match.group(1), match.group(2)  # experiment_id, participant_id
    return None, None

def download_and_combine_files():
    # Retrieve all objects from the bucket
    objects = list_all_objects(bucket_name)
    
    if not objects:
        print("No objects found in the bucket.")
        return
    
    # Sort objects by the LastModified timestamp (most recent first)
    sorted_objects = sorted(objects, key=lambda obj: obj['LastModified'], reverse=False)
    
    # Group files by experiment and participant
    experiment_participant_files = defaultdict(list)
    
    for obj in sorted_objects:
        key = obj['Key']
        experiment_id, participant_id = parse_filename(key)
        
        if experiment_id and participant_id:
            experiment_participant_files[(experiment_id, participant_id)].append(key)
    
    # Process each group
    for (experiment_id, participant_id), file_keys in experiment_participant_files.items():
        combined_data = []
        
        for key in file_keys:
            try:
                # Download to a temporary file
                temp_filename = f"temp_{os.path.basename(key)}"
                s3.download_file(bucket_name, key, temp_filename)
                print(f"Downloaded: {key}")
                
                # Read JSON data
                with open(temp_filename, 'r') as file:
                    try:
                        json_data = json.load(file)
                        if isinstance(json_data, list):
                            combined_data.extend(json_data)
                        else:
                            combined_data.append(json_data)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in {key}: {e}")
                
                # Remove temporary file
                os.remove(temp_filename)
                
            except Exception as e:
                print(f"DOWNLOAD FAIL: {key} from bucket {bucket_name}, ERROR: {e}")
        
        if combined_data:
            # Create output filename
            output_filename = f"{experiment_id}_{participant_id}_combined.json"
            
            # Write combined data to file
            with open(output_filename, 'w') as outfile:
                json.dump(combined_data, outfile, indent=2)
            
            print(f"SUCCESS: Created combined file {output_filename} with {len(combined_data)} records from {len(file_keys)} files")

if __name__ == "__main__":
    download_and_combine_files()