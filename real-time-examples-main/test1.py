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
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url='http://10.32.38.210',
    config=Config(signature_version='s3v4')
)
bucket_name = 'dream-data'

# Global variables for control events and recording state.
record_on = False
global_event = {}
unique_id = None

# Buffer for sensor frames.
buffered_frames = []

# Async lock to prevent concurrent flushes.
flush_lock = asyncio.Lock()

async def flush_buffer():
    """
    Flush the buffered sensor frames to a JSON object and upload directly to S3 in memory.
    This function uses a lock to ensure only one flush operation is running at a time.
    The filename is generated using the unique_id (or "noid" if None) and the current UTC timestamp.
    """
    global buffered_frames

    # Acquire the lock to safely capture and clear the current buffer.
    async with flush_lock:
        if not buffered_frames:
            return
        # Copy the frames and immediately clear the global buffer.
        frames_to_flush = buffered_frames.copy()
        buffered_frames.clear()

    current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"
    identifier = unique_id if unique_id is not None else "noid"
    safe_timestamp = current_timestamp.replace(":", "-")
    filename = f"{identifier}_{safe_timestamp}.json"

    # Prepare the data object (each frame already contains all fields).
    data_object = {"frames": frames_to_flush}

    def sync_upload():
        try:
            json_data = json.dumps(data_object)
            bytes_obj = io.BytesIO(json_data.encode('utf-8'))
            s3.upload_fileobj(bytes_obj, bucket_name, filename)
            print(f"SUCCESS UPLOADED: {filename} to bucket {bucket_name}")
        except Exception as e:
            print(f"FAILED UPLOAD: {filename}, error: {e}")

    # Run the upload in a thread so as not to block the async loop.
    await asyncio.to_thread(sync_upload)

async def update(response, key):
    print(f'Updating as new sender valid in the workspace: {response}')
    await subscribe_to_stream(response['receiverID'], response['streamID'])

async def stale(response, key):
    print("stale:", response)

async def callback(data_bytes, streamID, header):
    print(f"Received data with length {len(data_bytes)} : {data_bytes.decode(encoding='UTF-8')}")
    # global record_on, global_event, unique_id, buffered_frames
    # try:
    #     message = data_bytes.decode("utf-8").strip()
    # except Exception as e:
    #     print("Failed to decode message:", e)
    #     return

    # # First, try to process control events.
    # try:
    #     obj = json.loads(message)
    #     if "event_name" in obj:
    #         if obj["event_name"].lower() == "start":
    #             record_on = True
    #             global_event = obj
    #             unique_id = obj.get("event_hash") or obj.get("instanceID")
    #             print("Control Event START received.")
    #             print("Global event metadata:")
    #             print(json.dumps(global_event, indent=2))
    #             print("Unique Identifier:", unique_id)
    #         elif obj["event_name"].lower() == "stop":
    #             record_on = False
    #             print("Control Event STOP received. Recording halted.")
    #             # Flush any remaining frames.
    #             if buffered_frames:
    #                 await flush_buffer()
    #         else:
    #             print("Control event received:", json.dumps(obj, indent=2))
    #         return  # Do not treat control events as sensor data.
    # except Exception:
    #     # Not a JSON message; assume sensor data.
    #     pass

    # # Process sensor data message.
    # # Expected format: "counter,sensor1,sensor2,...,sensor16,timestamp"
    # parts = message.split(',')
    # if len(parts) == 18:
    #     try:
    #         counter = int(parts[0])
    #         sensors = [float(x) for x in parts[1:17]]
    #         timestamp = float(parts[17])
    #         # Build sensor data including all required fields.
    #         sensor_data = {
    #             "counter": counter,
    #             "sensors": sensors,
    #             "timestamp": timestamp,
    #             "received_utc": int(time.time() * 1000),
    #             "received_localtime": str(datetime.datetime.now()),
    #             "control_event": global_event if record_on else None,
    #             "unique_id": unique_id
    #         }
    #         if record_on:
    #             print("Sensor data (recording ON):")
    #             print(json.dumps(sensor_data, indent=2))
    #             buffered_frames.append(sensor_data)
    #             # Trigger a flush when the buffer reaches 1000 frames.
    #             if len(buffered_frames) >= 1000:
    #                 # Create a background task to flush without blocking the callback.
    #                 asyncio.create_task(flush_buffer())
    #         else:
    #             print("Sensor data received but recording is OFF. Ignoring sensor data.")
    #     except Exception as e:
    #         print("Error processing sensor data:", e)
    # else:
    #     print("Unrecognized message format:", message)

async def subscriber(response, key):
    print("subscriber:", response)

async def dropped(response, key):
    print("dropped:", response)

async def main():
    await corelink.set_data_callback(callback)
    await corelink.set_server_callback(update, 'update')
    await corelink.set_server_callback(stale, 'stale')
    
    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    
    receiver_id = await corelink.create_receiver("CREATE", "ws", data_type = 'sensorData', alert=True, echo=True)
    control_receiver_id = await corelink.create_receiver("CREATE", "ws", data_type = 'dreamstream', alert=True, echo=True)

  
    print("Start receiving")
    await corelink.keep_open()
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("Receiver terminated.")
        if buffered_frames:
            await flush_buffer()
        await corelink.close()

    print("Finished")

corelink.run(main())
