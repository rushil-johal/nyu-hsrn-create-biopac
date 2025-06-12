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

def list_all_objects(bucket, prefix=""):
    """List all objects in the given S3 bucket with the specified prefix, handling pagination if needed."""
    all_objects = []
    continuation_token = None

    while True:
        params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            params['ContinuationToken'] = continuation_token
            
        response = s3.list_objects_v2(**params)
        
        if 'Contents' in response:
            all_objects.extend(response['Contents'])
        
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return all_objects

def get_folder_structure(bucket):
    """
    Get the folder structure from the S3 bucket.
    Returns a dictionary with the folder structure.
    """
    all_objects = list_all_objects(bucket)
    
    # Build folder structure
    folders = {}
    
    for obj in all_objects:
        key = obj['Key']
        
        # Skip if not a folder or file inside a folder
        if '/' not in key:
            continue
        
        # Parse path components
        components = key.split('/')
        
        # Skip empty components (trailing slash)
        if not components[-1] and len(components) == 2:
            # This is a top-level folder
            folders[components[0]] = folders.get(components[0], {"files": 0, "subfolders": {}})
        
        elif len(components) >= 2:
            # This has a subfolder structure
            top_folder = components[0]
            if top_folder not in folders:
                folders[top_folder] = {"files": 0, "subfolders": {}}
            
            if len(components) >= 3 and components[1]:
                # This is inside a subfolder
                subfolder = components[1]
                if subfolder not in folders[top_folder]["subfolders"]:
                    folders[top_folder]["subfolders"][subfolder] = 0
                
                # Count only actual files, not subfolder markers
                if components[-1]:
                    folders[top_folder]["subfolders"][subfolder] += 1
            elif components[-1]:  # Only count files, not folder markers
                folders[top_folder]["files"] += 1
    
    return folders

def display_folder_structure(folders):
    """Display the folder structure in a user-friendly way."""
    print("\n===== S3 BUCKET FOLDER STRUCTURE =====")
    
    if not folders:
        print("No folders found in the bucket.")
        return
    
    # Print each experiment folder and its participants
    for i, (folder, data) in enumerate(folders.items(), 1):
        file_count = data["files"]
        subfolder_info = ""
        
        if file_count > 0:
            subfolder_info = f" ({file_count} files directly in folder)"
            
        print(f"{i}. Experiment: {folder}{subfolder_info}")
        
        # Print participant subfolders
        for j, (subfolder, count) in enumerate(data["subfolders"].items(), 1):
            print(f"   {i}.{j} Participant: {subfolder} ({count} files)")
    
    print("========================================\n")

def parse_folder_structure(key):
    """Parse the S3 key to extract experiment ID and participant ID from folder structure."""
    parts = key.split('/')
    
    if len(parts) >= 3 and parts[2]:  # Format: experiment_id/participant_id/filename.json
        experiment_id = parts[0]
        participant_id = parts[1]
        return experiment_id, participant_id
    elif len(parts) >= 2 and parts[1]:  # Format: experiment_id/filename.json
        experiment_id = parts[0]
        # For files directly in experiment folder, use the name pattern
        filename = parts[1]
        match = re.match(r'(\w+)_(\w+)_.*\.json', filename)
        if match:
            participant_id = match.group(2)
            return experiment_id, participant_id
    
    # Fallback to old format parsing if folder structure is not detected
    filename = os.path.basename(key)
    match = re.match(r'(\w+)_(\w+)_.*\.json', filename)
    if match:
        return match.group(1), match.group(2)  # experiment_id, participant_id
    
    return None, None

def get_user_selection(folders):
    """Get user selection of which folders to download."""
    valid_choices = []
    
    # Build mapping of choices to folder paths
    choice_map = {}
    
    folder_names = list(folders.keys())
    
    for i, folder in enumerate(folder_names, 1):
        choice_map[str(i)] = (folder, None)  # (experiment, participant=None)
        valid_choices.append(str(i))
        
        for j, subfolder in enumerate(folders[folder]["subfolders"].keys(), 1):
            choice_key = f"{i}.{j}"
            choice_map[choice_key] = (folder, subfolder)  # (experiment, participant)
            valid_choices.append(choice_key)
    
    # Add "all" option
    valid_choices.append("all")
    
    while True:
        print("\nOptions:")
        print("- Enter a number to download a specific experiment folder")
        print("- Enter a number with subfolder (e.g., '1.2') to download a specific participant")
        print("- Enter 'all' to download everything")
        choice = input("\nWhat would you like to download? ").strip().lower()
        
        if choice in valid_choices:
            if choice == "all":
                # Return all experiment folders with all participants
                return [(folder, None) for folder in folder_names]
            else:
                # Return the selected folder/subfolder
                return [choice_map[choice]]
        else:
            print("Invalid selection. Please try again.")

def download_and_combine_files(selection):
    """Download and combine files based on user selection."""
    output_dir = "combined_data"
    os.makedirs(output_dir, exist_ok=True)
    
    for experiment_id, participant_id in selection:
        # Create experiment directory
        experiment_dir = os.path.join(output_dir, experiment_id)
        os.makedirs(experiment_dir, exist_ok=True)
        
        prefix = f"{experiment_id}/"
        if participant_id:
            prefix = f"{experiment_id}/{participant_id}/"
        
        print(f"\nFetching files with prefix: {prefix}")
        objects = list_all_objects(bucket_name, prefix)
        
        if not objects:
            print(f"No objects found with prefix {prefix}")
            continue
            
        # Sort objects by the LastModified timestamp
        sorted_objects = sorted(objects, key=lambda obj: obj['LastModified'])
        
        # Group files by experiment and participant
        experiment_participant_files = defaultdict(list)
        
        for obj in sorted_objects:
            key = obj['Key']
            
            if not key.endswith('.json'):
                continue
                
            exp_id, part_id = parse_folder_structure(key)
            
            if exp_id and part_id:
                experiment_participant_files[(exp_id, part_id)].append(key)
        
        # Process each group
        for (exp_id, part_id), file_keys in experiment_participant_files.items():
            combined_data = []
            
            print(f"\nProcessing {len(file_keys)} files for experiment '{exp_id}', participant '{part_id}'")
            
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
                            # Handle different JSON structures
                            if "frames" in json_data and isinstance(json_data["frames"], list):
                                combined_data.extend(json_data["frames"])
                                print(f"  Added {len(json_data['frames'])} frames from file")
                            elif isinstance(json_data, list):
                                combined_data.extend(json_data)
                                print(f"  Added {len(json_data)} items from file")
                            else:
                                combined_data.append(json_data)
                                print(f"  Added 1 item from file")
                        except json.JSONDecodeError as e:
                            print(f"  Error decoding JSON in {key}: {e}")
                    
                    # Remove temporary file
                    os.remove(temp_filename)
                    
                except Exception as e:
                    print(f"  DOWNLOAD FAIL: {key} from bucket {bucket_name}, ERROR: {e}")
            
            if combined_data:
                # Create output filename
                output_filename = f"{exp_id}_{part_id}_combined.json"
                output_path = os.path.join(experiment_dir, output_filename)
                
                # Write combined data to file
                with open(output_path, 'w') as outfile:
                    json.dump(combined_data, outfile, indent=2)
                
                print(f"SUCCESS: Created combined file {output_path} with {len(combined_data)} records from {len(file_keys)} files")

def main():
    print("Fetching folder structure from S3 bucket...")
    folders = get_folder_structure(bucket_name)
    
    display_folder_structure(folders)
    
    if not folders:
        print("No folders to download. Exiting.")
        return
        
    selection = get_user_selection(folders)
    
    print(f"You selected to download: {selection}")
    confirm = input("Proceed with download? (y/n): ").strip().lower()
    
    if confirm == 'y':
        download_and_combine_files(selection)
        print("\nDownload and combination complete!")
    else:
        print("Download cancelled.")

if __name__ == "__main__":
    main()