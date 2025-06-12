
import cv2
from ultralytics import YOLO
import corelink
import asyncio
import aiofiles
import os
import struct
import time
import math

# Constants for chunk size, header size, and retry configuration
CHUNK_SIZE = 32 * 1024  # 32 KB chunk size
HEADER_SIZE = 14  # Updated to include timestamp (8 bytes) + frame number (2 bytes) + chunk index (2 bytes) + total chunks (2 bytes)
VALIDATION_TIMEOUT = 15  # seconds
RETRY_COUNT = 5  # Number of retries
RETRY_DELAY = 0.01  # Delay in seconds between retries

# Global variables for connection validation and frame counting
validConnection = False
frame_counter = 0  # Frame counter for sequential frame numbers

# Callback function for received data
async def callback(data_bytes, streamID, header):
    print(f"Received data with length {len(data_bytes)} : {data_bytes}")

# Subscriber callback function
async def subscriber(response, key):
    global validConnection
    print("subscriber: ", response)
    validConnection = True

# Dropped connection callback function
async def dropped(response, key):
    global validConnection
    print("dropped", response)
    validConnection = False

# Update callback function
async def update(response, key):
    print(f'Updating as new sender valid in the workspace: {response}')
    await corelink.subscribe_to_stream(response['receiverID'], response['streamID'])

# Stale connection callback function
async def stale(response, key):
    print(response)

# Function to check connection validity periodically
async def check_connection():
    global validConnection
    while True:
        await asyncio.sleep(VALIDATION_TIMEOUT)
        if not validConnection:
            print("Connection not validated, retrying...")

# Function to send a chunk of a file
async def send_file_chunk(chunk, frame_counter, chunk_index, total_chunks, timestamp):
    buffer = bytearray(HEADER_SIZE + len(chunk))
    struct.pack_into('>QHHH', buffer, 0, timestamp, frame_counter, chunk_index, total_chunks)
    buffer[HEADER_SIZE:] = chunk

    retries = 0
    while retries < RETRY_COUNT:
        try:
            await corelink.send(sender_id, buffer)
            return
        except PermissionError as e:
            retries += 1
            print(f"Failed to send chunk {chunk_index}/{total_chunks} for frame {frame_counter}: {e}. Retrying {retries}/{RETRY_COUNT}...")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"Failed to send chunk {chunk_index}/{total_chunks} for frame {frame_counter} due to a WebSocket error: {e}")
            break

# Function to send an entire file by splitting it into chunks
async def send_file(file_data, frame_counter):
    file_size = len(file_data)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    timestamp = int(time.time() * 1000)  # Convert to milliseconds

    tasks = [
        send_file_chunk(file_data[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE], frame_counter, i, total_chunks, timestamp)
        for i in range(total_chunks)
    ]
    await asyncio.gather(*tasks)

# Function to send an end message after file transfer is complete
async def send_end_message():
    end_message = b'FINISHED'
    try:
        await corelink.send(sender_id, end_message)
        print('End message sent.')
    except Exception as e:
        print(f"Failed to send end message: {e}")

async def main():
    global validConnection, sender_id, frame_counter
    await corelink.set_server_callback(subscriber, 'subscriber')
    await corelink.set_server_callback(dropped, 'dropped')
    await corelink.set_data_callback(callback)
    await corelink.set_server_callback(update, 'update')
    await corelink.set_server_callback(stale, 'stale')

    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    sender_id = await corelink.create_sender("detectionRaw", "ws", "description1")

    receiver_id = await corelink.create_receiver("detectionCtl", "ws", alert=True, echo=True)

    print(f'Receiver ID: {receiver_id}')
    print("Start receiving")

    asyncio.create_task(check_connection())  # Start connection validation in the background

    # Start video capture
    cap = cv2.VideoCapture(0)  # Change the argument to 0 for webcam or the path to a video file

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Convert the frame to bytes
        _, frame_encoded = cv2.imencode('.jpg', frame)
        frame_bytes = frame_encoded.tobytes()
        
        # Send the frame
        await send_file(frame_bytes, frame_counter)
        frame_counter += 1

    await send_end_message()  # Send end message when done

    cap.release()
    cv2.destroyAllWindows()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass

# Run the main function
corelink.run(main())
