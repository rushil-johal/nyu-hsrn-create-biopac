# import asyncio
# import json
# import time
# import datetime
# import io  # In-memory buffer

# import boto3
# from botocore.client import Config

# import corelink
# from corelink.resources.control import subscribe_to_stream

# import os
# from dotenv import load_dotenv

# load_dotenv()


# # S3 client initialization.
# s3 = boto3.client(
#     's3',
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#     endpoint_url='http://10.32.38.210',
#     config=Config(signature_version='s3v4')
# )
# bucket_name = 'dream-data'

# # Global variables for control events and recording state.
# record_on = False
# global_event = {}
# unique_id = None

# # Buffer for sensor frames and control events.
# buffered_frames = []

# # Async lock to prevent concurrent flushes.
# flush_lock = asyncio.Lock()

# async def flush_buffer():
#     """
#     Flush the buffered frames (sensor data and control events) to a JSON object and upload directly to S3 in memory.
#     The filename is generated using the unique_id (or "noid" if None) and the current UTC timestamp.
#     """
#     global buffered_frames

#     async with flush_lock:
#         if not buffered_frames:
#             return
#         # Copy the frames and clear the global buffer immediately.
#         frames_to_flush = buffered_frames.copy()
#         buffered_frames.clear()

#     current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"
#     identifier = unique_id if unique_id is not None else "noid"
#     safe_timestamp = current_timestamp.replace(":", "-")
#     filename = f"{identifier}_{safe_timestamp}.json"

#     data_object = {"frames": frames_to_flush}

#     def sync_upload():
#         try:
#             json_data = json.dumps(data_object)
#             bytes_obj = io.BytesIO(json_data.encode('utf-8'))
#             s3.upload_fileobj(bytes_obj, bucket_name, filename)
#             print(f"SUCCESS UPLOADED: {filename} to bucket {bucket_name}")
#         except Exception as e:
#             print(f"FAILED UPLOAD: {filename}, error: {e}")

#     # Run the upload in a thread so as not to block the async loop.
#     await asyncio.to_thread(sync_upload)

# async def update(response, key):
#     print(f'Updating as new sender valid in the workspace: {response}')
#     await subscribe_to_stream(response['receiverID'], response['streamID'])

# async def stale(response, key):
#     print("stale:", response)

# async def callback(data_bytes, streamID, header):
#     global record_on, global_event, unique_id, buffered_frames
#     try:
#         message = data_bytes.decode("utf-8").strip()
#     except Exception as e:
#         print("Failed to decode message:", e)
#         return

#     # Process control events.
#     try:
#         obj = json.loads(message)
#         if "event_name" in obj:
#             # Update the global control event always.
#             global_event = obj

#             # Append the control event to the buffer with metadata.
#             control_frame = {
#                 "control_event": obj,
#                 "received_utc": int(time.time() * 1000),
#                 "received_localtime": str(datetime.datetime.now())
#             }
#             buffered_frames.append(control_frame)
#             # (Optionally, you can flush immediately, but here we keep using the shared buffer.)
#             # await flush_buffer()

#             # Handle specific control events.
#             if obj["event_name"].lower() == "start":
#                 record_on = True
#                 unique_id = obj.get("event_hash") or obj.get("instanceID")
#                 print("Control Event START received.")
#                 print("Global event metadata:")
#                 print(json.dumps(global_event, indent=2))
#                 print("Unique Identifier:", unique_id)
#             elif obj["event_name"].lower() == "stop":
#                 # Only set record off when a stop event is received.
#                 record_on = False
#                 print("Control Event STOP received. Recording halted.")
#                 # Flush any remaining frames.
#                 if buffered_frames:
#                     await flush_buffer()
#             else:
#                 print("Control event received:", json.dumps(obj, indent=2))
#             return  # Do not treat control events as sensor data.
#     except Exception:
#         # Not a JSON message; assume sensor data.
#         pass

#     # Process sensor data message.
#     # Expected format: "counter,sensor1,sensor2,...,sensor16,timestamp, MP150/MP160"
#     parts = message.split(',')
#     if len(parts) == 19:
#         try:
#             counter = int(parts[0])
#             sensors = [float(x) for x in parts[1:17]]
#             timestamp = float(parts[17])
#             device_name = parts[18]
#             sensor_data = {
#                 "counter": counter,
#                 "sensors": sensors,
#                 "timestamp": timestamp,
#                 "received_utc": int(time.time() * 1000),
#                 "received_localtime": str(datetime.datetime.now()),
#                 "control_event": global_event,
#                 "unique_id": unique_id,
#                 "device_name": device_name
#             }
#             if record_on:
#                 print("Sensor data (recording ON):")
#                 print(json.dumps(sensor_data, indent=2))
#                 buffered_frames.append(sensor_data)
#                 # Trigger a flush when the buffer reaches 1000 frames.
#                 if len(buffered_frames) >= 1000:
#                     asyncio.create_task(flush_buffer())
#             else:
#                 print("Sensor data received but recording is OFF. Ignoring sensor data.")
#         except Exception as e:
#             print("Error processing sensor data:", e)
#     else:
#         print("Unrecognized message format:", message)

# async def subscriber(response, key):
#     print("subscriber:", response)

# async def dropped(response, key):
#     print("dropped:", response)

# async def monitor_exit():
#     """
#     Continuously waits for keyboard input. If the user types 'exit', the function returns,
#     triggering the shutdown sequence.
#     """
#     while True:
#         user_input = await asyncio.to_thread(input, "Type 'exit' to stop: ")
#         if user_input.strip().lower() == "exit":
#             print("Exit command received.")
#             return
# async def main():
#     await corelink.set_data_callback(callback)
#     await corelink.set_server_callback(update, 'update')
#     await corelink.set_server_callback(stale, 'stale')
    
#     await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    
#     receiver_id = await corelink.create_receiver("CREATE", "ws", data_type='sensorData', alert=True, echo=True)
#     control_receiver_id = await corelink.create_receiver("Holodeck", "ws", data_type='dreamstream', alert=True, echo=True)
    
#     print("Start receiving. Type 'exit' to stop.")
    
#     try:
#         while True:
#             try:
#                 # Wait for keyboard input with a timeout of 3600 seconds.
#                 user_input = await asyncio.wait_for(asyncio.to_thread(input, "Type 'exit' to stop: "), timeout=3600)
#                 if user_input.strip().lower() == "exit":
#                     print("Exit command received.")
#                     break
#             except asyncio.TimeoutError:
#                 # No input within 3600 seconds; continue looping.
#                 pass
#     except KeyboardInterrupt:
#         print("KeyboardInterrupt detected.")
#     finally:
#         print("Stopping receiver. Flushing any remaining buffered frames...")
#         if buffered_frames:
#             await flush_buffer()
#         await corelink.close()
#         print("Finished")

# corelink.run(main())
import asyncio
import json
import time
import datetime
import io  # In-memory buffer

import boto3
from botocore.client import Config

import corelink
from corelink.resources.control import subscribe_to_stream

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

# Participant information structure (matches the participants list approach from duo script)
active_participant = {
    "participant_id": None,
    "experiment_id": None,
    "record_on": False,
    "global_event": {},
    "buffer": [],
    "lock": asyncio.Lock()
}

async def check_if_folder_exists(folder_path):
    """Check if a folder exists in S3 bucket."""
    try:
        # Add trailing slash to ensure we're checking for a folder
        if not folder_path.endswith('/'):
            folder_path += '/'
            
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_path,
            MaxKeys=1
        )
        return 'Contents' in response
    except Exception as e:
        print(f"Error checking if folder exists: {e}")
        return False

async def create_folder_if_not_exists(folder_path):
    """Create a folder in S3 bucket if it doesn't exist."""
    folder_exists = await check_if_folder_exists(folder_path)
    
    if not folder_exists:
        try:
            # Create an empty object with folder name as key ending with '/'
            if not folder_path.endswith('/'):
                folder_path += '/'
                
            s3.put_object(
                Bucket=bucket_name,
                Key=folder_path
            )
            print(f"Created folder: {folder_path} in bucket {bucket_name}")
        except Exception as e:
            print(f"Error creating folder: {e}")

async def flush_buffer_for_participant():
    """
    Flush the buffered frames for the active participant to a JSON object and upload to S3.
    The file is stored in a folder structure: /{experiment_id}/{participant_id}/
    """
    global active_participant
    
    async with active_participant["lock"]:
        if not active_participant["buffer"]:
            return
        frames_to_flush = active_participant["buffer"].copy()
        active_participant["buffer"].clear()

    # Get participant ID and experiment ID or use defaults
    participant_id = active_participant["participant_id"] if active_participant["participant_id"] is not None else "unknown_participant"
    exp_id = active_participant["experiment_id"] if active_participant["experiment_id"] is not None else "unknown_experiment"
    
    # Create folder structure
    folder_path = f"{exp_id}/{participant_id}/"
    await create_folder_if_not_exists(f"{exp_id}/")
    await create_folder_if_not_exists(folder_path)
    
    # Create filename with timestamp
    current_timestamp = datetime.datetime.utcnow().isoformat().replace(":", "-")
    filename = f"{participant_id}_{current_timestamp}.json"
    full_path = f"{folder_path}{filename}"

    data_object = {"frames": frames_to_flush}

    def sync_upload():
        try:
            json_data = json.dumps(data_object)
            bytes_obj = io.BytesIO(json_data.encode('utf-8'))
            s3.upload_fileobj(bytes_obj, bucket_name, full_path)
            print(f"SUCCESS UPLOADED: {full_path} to bucket {bucket_name}")
        except Exception as e:
            print(f"FAILED UPLOAD: {full_path}, error: {e}")

    # Run the upload in a thread so as not to block the async loop.
    await asyncio.to_thread(sync_upload)

async def update(response, key):
    print(f'Updating as new sender valid in the workspace: {response}')
    await subscribe_to_stream(response['receiverID'], response['streamID'])

async def stale(response, key):
    print("stale:", response)

async def callback(data_bytes, streamID, header):
    global active_participant
    
    try:
        message = data_bytes.decode("utf-8").strip()
    except Exception as e:
        print("Failed to decode message:", e)
        return

    # Process control events
    try:
        obj = json.loads(message)
        if "event_name" in obj:
            event_name = obj["event_name"].lower()
            
            # Get participant ID from the control event
            participant_key = obj.get("participantID") or obj.get("event_hash") or obj.get("instanceID")
            
            # Extract experiment ID if available
            event_experiment_id = obj.get("studyName") or obj.get("experiment")
            
            # Create a control event frame
            control_frame = {
                "control_event": obj,
                "received_utc": int(time.time() * 1000),
                "received_localtime": str(datetime.datetime.now())
            }
            
            # Handle specific control events
            if event_name == "start":
                # Initialize participant with this start event
                active_participant["participant_id"] = participant_key
                active_participant["experiment_id"] = event_experiment_id
                active_participant["record_on"] = True
                active_participant["global_event"] = obj
                active_participant["buffer"].append(control_frame)
                
                print(f"Control Event START received for participant {participant_key} in experiment {event_experiment_id}.")
                print("Global event metadata:")
                print(json.dumps(obj, indent=2))
            
            elif event_name == "stop" and participant_key == active_participant["participant_id"]:
                # Stop event for our active participant
                active_participant["record_on"] = False
                active_participant["global_event"] = obj
                active_participant["buffer"].append(control_frame)
                
                print(f"Control Event STOP received for participant {participant_key}. Recording halted.")
                
                # Flush any remaining frames
                if active_participant["buffer"]:
                    await flush_buffer_for_participant()
            
            elif participant_key == active_participant["participant_id"]:
                # Any other event for our active participant
                active_participant["global_event"] = obj
                active_participant["buffer"].append(control_frame)
                
                print(f"Control event '{event_name}' received for active participant {participant_key}.")
            
            else:
                # Event for a different participant - ignore
                print(f"Control event '{event_name}' received for different participant {participant_key}. Ignoring.")
            
            return  # Do not treat control events as sensor data
    
    except Exception as e:
        # Not a JSON message; assume sensor data
        pass

    # Process sensor data message
    # Expected format: "counter,sensor1,sensor2,...,sensor16,timestamp, MP150/MP160"
    parts = message.split(',')
    if len(parts) == 19:
        try:
            # Only process sensor data if we have an active participant
            if active_participant["participant_id"] is None:
                print("Sensor data received but no active participant has been registered. Ignoring.")
                return
                
            if not active_participant["record_on"]:
                print(f"Sensor data received for participant {active_participant['participant_id']}, but recording is OFF. Ignoring.")
                return
                
            counter = int(parts[0])
            sensors = [float(x) for x in parts[1:17]]
            timestamp = float(parts[17])
            device_name = parts[18].strip().strip("'").strip('"')
            
            sensor_data = {
                "counter": counter,
                "sensors": sensors,
                "timestamp": timestamp,
                "received_utc": int(time.time() * 1000),
                "received_localtime": str(datetime.datetime.now()),
                "control_event": active_participant["global_event"],
                "participant_id": active_participant["participant_id"],
                "experiment_id": active_participant["experiment_id"],
                "device_name": device_name
            }
            
            print(f"Sensor data (recording ON) for participant {active_participant['participant_id']}:")
            print(json.dumps(sensor_data, indent=2))
            
            active_participant["buffer"].append(sensor_data)
            
            # Trigger a flush when the buffer reaches 1000 frames
            if len(active_participant["buffer"]) >= 1000:
                asyncio.create_task(flush_buffer_for_participant())
                
        except Exception as e:
            print(f"Error processing sensor data: {e}")
    else:
        print(f"Unrecognized message format: {message}")

async def subscriber(response, key):
    print("subscriber:", response)

async def dropped(response, key):
    print("dropped:", response)

async def main():
    await corelink.set_data_callback(callback)
    await corelink.set_server_callback(update, 'update')
    await corelink.set_server_callback(stale, 'stale')
    
    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    
    receiver_id = await corelink.create_receiver("CREATE", "ws", data_type='sensorData', alert=True, echo=True)
    control_receiver_id = await corelink.create_receiver("Holodeck", "ws", data_type='dreamstream', alert=True, echo=True)
    
    print("Start receiving. Type 'exit' to stop.")
    
    # Start the corelink keep_open task concurrently.
    keep_open_task = asyncio.create_task(corelink.keep_open())
    
    try:
        while True:
            try:
                # Wait for keyboard input with a timeout of 3600 seconds.
                user_input = await asyncio.wait_for(asyncio.to_thread(input, "Type 'exit' to stop: "), timeout=3600)
                if user_input.strip().lower() == "exit":
                    print("Exit command received.")
                    break
            except asyncio.TimeoutError:
                # No input within 3600 seconds; continue looping.
                pass
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected.")
    finally:
        # Cancel the keep_open task.
        keep_open_task.cancel()
        print("Stopping receiver. Flushing any remaining buffered frames...")
        if active_participant["buffer"]:
            await flush_buffer_for_participant()
        await corelink.close()
        print("Finished")

corelink.run(main())