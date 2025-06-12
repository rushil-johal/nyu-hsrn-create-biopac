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

# # Global state:
# # Participants are created/updated based on control events.
# # The first control event is assigned as participant p1,
# # the second as participant p2.
# # Each participant is a dict with:
# #   "participant_id": identifier used for mapping (from control event "participantID"),
# #   "record_on": recording flag,
# #   "global_event": latest control event JSON,
# #   "buffer": list to store sensor data frames,
# #   "lock": an asyncio Lock for buffer flushing.
# participants = []  # Maximum of 2 entries

# async def flush_buffer_for_participant(participant, participant_index):
#     """
#     Flush the buffered sensor frames for a given participant to a JSON object and upload to S3.
#     The filename is generated using the participant_id and a suffix (p1 or p2) based on order.
#     """
#     async with participant["lock"]:
#         if not participant["buffer"]:
#             return
#         frames_to_flush = participant["buffer"].copy()
#         participant["buffer"].clear()

#     current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"
#     safe_timestamp = current_timestamp.replace(":", "-")
#     suffix = "p1" if participant_index == 0 else "p2"
#     pid = participant["participant_id"] if participant["participant_id"] is not None else "nopid"
#     filename = f"{pid}_{suffix}_{safe_timestamp}.json"

#     data_object = {"frames": frames_to_flush}

#     def sync_upload():
#         try:
#             json_data = json.dumps(data_object)
#             bytes_obj = io.BytesIO(json_data.encode('utf-8'))
#             s3.upload_fileobj(bytes_obj, bucket_name, filename)
#             print(f"SUCCESS UPLOADED ({suffix}): {filename} to bucket {bucket_name}")
#         except Exception as e:
#             print(f"FAILED UPLOADED ({suffix}): {filename}, error: {e}")

#     await asyncio.to_thread(sync_upload)

# async def callback(data_bytes, streamID, header):
#     """
#     This callback processes both control events and sensor data.

#     Control events are sent as JSON and trigger participant assignment/updating.
#     We use the control event’s "participantID" (if present) as the mapping key.
#       - A "start" event for a new participant creates a new record (p1 if first, p2 if second).
#       - Any control event (except "stop") for an existing participant (same participantID)
#         updates its stored global_event.
#       - A "stop" event sets record_on to False and flushes the participant’s buffer.
    
#     Sensor data messages are expected as CSV strings with 19 comma-separated columns:
#       Format: counter, sensor1, ..., sensor16, timestamp, device_type
#     Based on device_type (expected "MP150" for p1 and "MP160" for p2), sensor data is routed.
#     """
#     global participants
#     try:
#         message = data_bytes.decode("utf-8").strip()
#         print("Received message:", message)
#     except Exception as e:
#         print("Failed to decode message:", e)
#         return

#     # First, try processing as a control event.
#     try:
#         obj = json.loads(message)
#         if "event_name" in obj:
#             event_name = obj["event_name"].lower()
#             # Use participantID as the mapping key if available.
#             participant_key = obj.get("participantID") or obj.get("event_hash") or obj.get("instanceID")
#             if event_name == "stop":
#                 participant = next((p for p in participants if p["participant_id"] == participant_key), None)
#                 if participant:
#                     participant["record_on"] = False
#                     print(f"Control Event STOP received for participant with participantID: {participant_key}")
#                     idx = participants.index(participant)
#                     if participant["buffer"]:
#                         await flush_buffer_for_participant(participant, idx)
#                 else:
#                     print("Control STOP event received for unknown participantID:", participant_key)
#             else:
#                 # For any event other than "stop" (including "start" and marker events):
#                 participant = next((p for p in participants if p["participant_id"] == participant_key), None)
#                 if participant:
#                     # Update the stored control event.
#                     participant["global_event"] = obj
#                     # If it's a "start" event, ensure recording is on.
#                     if event_name == "start":
#                         participant["record_on"] = True
#                     print(f"Control event received for participant with participantID: {participant_key}. Updated event: {event_name}")
#                 else:
#                     # Create a new participant only for a "start" event.
#                     if event_name == "start":
#                         if len(participants) < 2:
#                             new_participant = {
#                                 "participant_id": participant_key,
#                                 "record_on": True,
#                                 "global_event": obj,
#                                 "buffer": [],
#                                 "lock": asyncio.Lock()
#                             }
#                             participants.append(new_participant)
#                             label = "p1" if len(participants) == 1 else "p2"
#                             print(f"Control Event START received. Assigned participantID {participant_key} as participant {label}.")
#                         else:
#                             print("Received START event for a third participant. Ignoring.")
#                     else:
#                         print(f"Control event received for unknown participant with participantID: {participant_key}. Ignoring.")
#             return  # End control event processing.
#     except Exception:
#         # Not a JSON control event; assume sensor data.
#         pass

#     # Process sensor data message.
#     # Expecting 19 columns: counter, sensor1, ..., sensor16, timestamp, device_type
#     parts = message.split(',')
#     if len(parts) == 19:
#         try:
#             counter = int(parts[0])
#             sensors = [float(x) for x in parts[1:17]]
#             timestamp = float(parts[17])
#             # Remove extra quotes and spaces from device_type.
#             device_type = parts[18].strip().strip("'").strip('"')
#             sensor_data = {
#                 "counter": counter,
#                 "sensors": sensors,
#                 "timestamp": timestamp,
#                 "device_type": device_type,
#                 "received_utc": int(time.time() * 1000),
#                 "received_localtime": str(datetime.datetime.now())
#             }
#             # Route sensor data based on device_type.
#             if device_type == "MP150":
#                 if len(participants) >= 1:
#                     participant = participants[0]
#                     label = "p1"
#                 else:
#                     print("Sensor data for MP150 received but no participant (p1) exists yet. Ignoring sensor data.")
#                     return
#             elif device_type == "MP160":
#                 if len(participants) >= 2:
#                     participant = participants[1]
#                     label = "p2"
#                 else:
#                     print("Sensor data for MP160 received but no participant (p2) exists yet. Ignoring sensor data.")
#                     return
#             else:
#                 print("Unknown device type received:", device_type)
#                 return

#             if not participant["record_on"]:
#                 print(f"Sensor data received for participant {label}, but recording is off. Ignoring sensor data.")
#                 return

#             # Use the updated control event stored in the participant.
#             sensor_data["control_event"] = participant["global_event"]
#             sensor_data["participant_id"] = participant["participant_id"]
#             print(f"Sensor data (recording ON) for participant {label}:")
#             print(json.dumps(sensor_data, indent=2))
#             participant["buffer"].append(sensor_data)
#             if len(participant["buffer"]) >= 1000:
#                 idx = participants.index(participant)
#                 asyncio.create_task(flush_buffer_for_participant(participant, idx))
#         except Exception as e:
#             print("Error processing sensor data:", e)
#     else:
#         print("Unrecognized message format:", message)

# async def update(response, key):
#     print(f"Updating as new sender valid in the workspace: {response}")
#     await subscribe_to_stream(response['receiverID'], response['streamID'])

# async def stale(response, key):
#     print("stale:", response)

# async def subscriber(response, key):
#     print("subscriber:", response)

# async def dropped(response, key):
#     print("dropped:", response)

# # async def main():
# #     await corelink.set_data_callback(callback)
# #     await corelink.set_server_callback(update, 'update')
# #     await corelink.set_server_callback(stale, 'stale')

# #     await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)

# #     # Create sensor data receiver.
# #     sensor_receiver_id1 = await corelink.create_receiver("CREATE", "ws", data_type='sensorData', alert=True, echo=True)
# #     # Create control event receiver.
# #     control_receiver_id1 = await corelink.create_receiver("CREATE", "ws", data_type='dreamstream', alert=True, echo=True)

# #     print("Start receiving for participants (assigned dynamically based on control events).")
# #     await corelink.keep_open()
# #     try:
# #         while True:
# #             await asyncio.sleep(3600)
# #     except KeyboardInterrupt:
# #         print("Receiver terminated.")
# #         for idx, participant in enumerate(participants):
# #             if participant["buffer"]:
# #                 await flush_buffer_for_participant(participant, idx)
# #         await corelink.close()
# #     print("Finished")

# # corelink.run(main())
# async def main():
#     await corelink.set_data_callback(callback)
#     await corelink.set_server_callback(update, 'update')
#     await corelink.set_server_callback(stale, 'stale')

#     await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)

#     # Create sensor data receiver.
#     sensor_receiver_id1 = await corelink.create_receiver("CREATE", "ws", data_type='sensorData', alert=True, echo=True)
#     # Create control event receiver.
#     control_receiver_id1 = await corelink.create_receiver("Holodeck", "ws", data_type='dreamstream', alert=True, echo=True)

#     print("Start receiving for participants (assigned dynamically based on control events).")
    
#     # Start the corelink keep_open task concurrently.
#     keep_open_task = asyncio.create_task(corelink.keep_open())
    
#     try:
#         while True:
#             try:
#                 # Wait for keyboard input with a timeout of 3600 seconds.
#                 user_input = await asyncio.wait_for(
#                     asyncio.to_thread(input, "Type 'exit' to stop: "),
#                     timeout=3600
#                 )
#                 if user_input.strip().lower() == "exit":
#                     print("Exit command received.")
#                     break
#             except asyncio.TimeoutError:
#                 # No input received within the timeout period; continue looping.
#                 pass
#     except KeyboardInterrupt:
#         print("KeyboardInterrupt detected.")
#     finally:
#         # Cancel the keep_open task.
#         keep_open_task.cancel()
#         print("Receiver terminated.")
#         # Flush buffers for each participant if they have pending data.
#         for idx, participant in enumerate(participants):
#             if participant["buffer"]:
#                 await flush_buffer_for_participant(participant, idx)
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

# Global state:
# Participants are created/updated based on control events.
# The first control event is assigned as participant p1,
# the second as participant p2.
# Each participant is a dict with:
#   "participant_id": identifier used for mapping (from control event "participantID"),
#   "experiment_id": identifier for the experiment (from control event "experimentID" or "experiment"),
#   "record_on": recording flag,
#   "global_event": latest control event JSON,
#   "buffer": list to store sensor data frames,
#   "lock": an asyncio Lock for buffer flushing.
participants = []  # Maximum of 2 entries

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

async def flush_buffer_for_participant(participant, participant_index):
    """
    Flush the buffered sensor frames for a given participant to a JSON object and upload to S3.
    The file is stored in a folder structure: /{experiment_id}/{participant_id}/
    """
    async with participant["lock"]:
        if not participant["buffer"]:
            return
        frames_to_flush = participant["buffer"].copy()
        participant["buffer"].clear()

    # Get experiment ID or use default
    experiment_id = participant.get("experiment_id", "unknown_experiment")
    participant_id = participant.get("participant_id", "unknown_participant")
    
    # Use p1/p2 suffix for consistency with previous implementation
    suffix = "p1" if participant_index == 0 else "p2"
    
    # Create folder structure
    folder_path = f"{experiment_id}/{participant_id}/"
    await create_folder_if_not_exists(f"{experiment_id}/")
    await create_folder_if_not_exists(folder_path)
    
    # Create filename with timestamp
    current_timestamp = datetime.datetime.utcnow().isoformat().replace(":", "-")
    filename = f"{participant_id}_{suffix}_{current_timestamp}.json"
    full_path = f"{folder_path}{filename}"

    data_object = {"frames": frames_to_flush}

    def sync_upload():
        try:
            json_data = json.dumps(data_object)
            bytes_obj = io.BytesIO(json_data.encode('utf-8'))
            s3.upload_fileobj(bytes_obj, bucket_name, full_path)
            print(f"SUCCESS UPLOADED ({suffix}): {full_path} to bucket {bucket_name}")
        except Exception as e:
            print(f"FAILED UPLOADED ({suffix}): {full_path}, error: {e}")

    await asyncio.to_thread(sync_upload)

async def callback(data_bytes, streamID, header):
    """
    This callback processes both control events and sensor data.

    Control events are sent as JSON and trigger participant assignment/updating.
    We use the control event's "participantID" (if present) as the mapping key.
      - A "start" event for a new participant creates a new record (p1 if first, p2 if second).
      - Any control event (except "stop") for an existing participant (same participantID)
        updates its stored global_event.
      - A "stop" event sets record_on to False and flushes the participant's buffer.
    
    Sensor data messages are expected as CSV strings with 19 comma-separated columns:
      Format: counter, sensor1, ..., sensor16, timestamp, device_type
    Based on device_type (expected "MP150" for p1 and "MP160" for p2), sensor data is routed.
    """
    global participants
    try:
        message = data_bytes.decode("utf-8").strip()
        #print("Received message:", message)
    except Exception as e:
        print("Failed to decode message:", e)
        return

    # First, try processing as a control event.
    try:
        obj = json.loads(message)
        if "event_name" in obj:
            event_name = obj["event_name"].lower()
            # Use participantID as the mapping key if available.
            participant_key = obj.get("participantID") or obj.get("event_hash") or obj.get("instanceID")
            # Extract experiment ID if available
            experiment_id = obj.get("experimentID") or obj.get("studyName") or "unknown_experiment"
            
            if event_name == "stop":
                participant = next((p for p in participants if p["participant_id"] == participant_key), None)
                if participant:
                    participant["record_on"] = False
                    print(f"Control Event STOP received for participant with participantID: {participant_key}")
                    idx = participants.index(participant)
                    if participant["buffer"]:
                        await flush_buffer_for_participant(participant, idx)
                else:
                    print("Control STOP event received for unknown participantID:", participant_key)
            else:
                # For any event other than "stop" (including "start" and marker events):
                participant = next((p for p in participants if p["participant_id"] == participant_key), None)
                if participant:
                    # Update the stored control event and experiment ID
                    participant["global_event"] = obj
                    participant["experiment_id"] = experiment_id
                    # If it's a "start" event, ensure recording is on.
                    if event_name == "start":
                        participant["record_on"] = True
                    print(f"Control event received for participant with participantID: {participant_key}. Updated event: {event_name}")
                else:
                    # Create a new participant only for a "start" event.
                    if event_name == "start":
                        if len(participants) < 2:
                            new_participant = {
                                "participant_id": participant_key,
                                "experiment_id": experiment_id,
                                "record_on": True,
                                "global_event": obj,
                                "buffer": [],
                                "lock": asyncio.Lock()
                            }
                            participants.append(new_participant)
                            label = "p1" if len(participants) == 1 else "p2"
                            print(f"Control Event START received. Assigned participantID {participant_key} as participant {label} in experiment {experiment_id}.")
                        else:
                            print("Received START event for a third participant. Ignoring.")
                    else:
                        print(f"Control event received for unknown participant with participantID: {participant_key}. Ignoring.")
            return  # End control event processing.
    except Exception as e:
        # Not a JSON control event; assume sensor data.
        #print(f"Not a control event: {e}")
        pass

    # Process sensor data message.
    # Expecting 19 columns: counter, sensor1, ..., sensor16, timestamp, device_type
    parts = message.split(',')
    if len(parts) == 19:
        try:
            counter = int(parts[0])
            sensors = [float(x) for x in parts[1:17]]
            timestamp = float(parts[17])
            # Remove extra quotes and spaces from device_type.
            device_type = parts[18].strip().strip("'").strip('"')
            sensor_data = {
                "counter": counter,
                "sensors": sensors,
                "timestamp": timestamp,
                "device_type": device_type,
                "received_utc": int(time.time() * 1000),
                "received_localtime": str(datetime.datetime.now())
            }
            # Route sensor data based on device_type.
            if device_type == "MP150":
                if len(participants) >= 1:
                    participant = participants[0]
                    label = "p1"
                else:
                    print("Sensor data for MP150 received but no participant (p1) exists yet. Ignoring sensor data.")
                    return
            elif device_type == "MP160":
                if len(participants) >= 2:
                    participant = participants[1]
                    label = "p2"
                else:
                    print("Sensor data for MP160 received but no participant (p2) exists yet. Ignoring sensor data.")
                    return
            else:
                print("Unknown device type received:", device_type)
                return

            if not participant["record_on"]:
                print(f"Sensor data received for participant {label}, but recording is off. Ignoring sensor data.")
                return

            # Use the updated control event stored in the participant.
            sensor_data["control_event"] = participant["global_event"]
            sensor_data["participant_id"] = participant["participant_id"]
            sensor_data["experiment_id"] = participant.get("experiment_id", "unknown_experiment")
            print(f"Sensor data (recording ON) for participant {label}:")
            print(json.dumps(sensor_data, indent=2))
            participant["buffer"].append(sensor_data)
            if len(participant["buffer"]) >= 1000:
                idx = participants.index(participant)
                asyncio.create_task(flush_buffer_for_participant(participant, idx))
        except Exception as e:
            print("Error processing sensor data:", e)
    else:
        print("Unrecognized message format:", message)

async def update(response, key):
    print(f"Updating as new sender valid in the workspace: {response}")
    await subscribe_to_stream(response['receiverID'], response['streamID'])

async def stale(response, key):
    print("stale:", response)

async def subscriber(response, key):
    print("subscriber:", response)

async def dropped(response, key):
    print("dropped:", response)

async def main():
    await corelink.set_data_callback(callback)
    await corelink.set_server_callback(update, 'update')
    await corelink.set_server_callback(stale, 'stale')

    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)

    # Create sensor data receiver.
    sensor_receiver_id1 = await corelink.create_receiver("CREATE", "ws", data_type='sensorData', alert=True, echo=True)
    # Create control event receiver.
    control_receiver_id1 = await corelink.create_receiver("Holodeck", "ws", data_type='dreamstream', alert=True, echo=True)

    print("Start receiving for participants (assigned dynamically based on control events).")
    
    # Start the corelink keep_open task concurrently.
    keep_open_task = asyncio.create_task(corelink.keep_open())
    
    try:
        while True:
            try:
                # Wait for keyboard input with a timeout of 3600 seconds.
                user_input = await asyncio.wait_for(
                    asyncio.to_thread(input, "Type 'exit' to stop: "),
                    timeout=3600
                )
                if user_input.strip().lower() == "exit":
                    print("Exit command received.")
                    break
            except asyncio.TimeoutError:
                # No input received within the timeout period; continue looping.
                pass
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected.")
    finally:
        # Cancel the keep_open task.
        keep_open_task.cancel()
        print("Receiver terminated.")
        # Flush buffers for each participant if they have pending data.
        for idx, participant in enumerate(participants):
            if participant["buffer"]:
                await flush_buffer_for_participant(participant, idx)
        await corelink.close()
        print("Finished")

corelink.run(main())