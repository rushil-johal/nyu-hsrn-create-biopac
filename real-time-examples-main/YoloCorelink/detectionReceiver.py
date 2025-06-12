import corelink
import asyncio
import aiofiles
import os
import struct
import time
from collections import defaultdict
import logging
import sys
from io import BytesIO
import cv2
import numpy as np
from ultralytics import YOLO

# Initialize YOLO model
model = YOLO("yolov10x.pt")

HEADER_SIZE = 14  # Updated to include timestamp (8 bytes) + frame number (2 bytes) + chunk index (2 bytes) + total chunks (2 bytes)

# Configure logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Dictionary to hold the incoming chunks for each frame
incoming_frames = defaultdict(lambda: {
    "timestamp": 0,
    "total_slices": 0,
    "received_slices": 0,
    "chunks": [],
    "start_time": time.time()
})

# Function to process the frame with YOLO
async def process_frame_with_buffer(frame_data, frame_number, timestamp, start_time):
    # Convert frame data to an image
    frame_array = np.frombuffer(frame_data, dtype=np.uint8)
    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
    img = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

    # Perform inference
    results = model(img)
    
    counter=0
    for result in results:
        for box in result.boxes:
            class_id = int(box.cls)
            confidence = float(box.conf)
            bbox = box.xyxy
            label = result.names[class_id]
            
            print(f"{label}:{confidence:.2f} {bbox}")

            if label == "person" and confidence > 0.2:
                counter += 1
    print(counter)
    await corelink.send(sender_id, f"The total number of human in the frame is {counter}")


# Callback function for received data
async def callback(data_bytes, streamID, header):
    global incoming_frames

    # Extract the header information
    timestamp, frame_number, chunk_index, total_chunks = struct.unpack('>QHHH', data_bytes[:HEADER_SIZE])
    chunk_data = data_bytes[HEADER_SIZE:]
    arrival_time = time.time()

    frame = incoming_frames[frame_number]
    frame["timestamp"] = timestamp
    
    # Initialize frame entry if receiving the first chunk
    if frame["received_slices"] == 0:
        frame["total_slices"] = total_chunks
        frame["chunks"] = [None] * total_chunks
        frame["start_time"] = int(time.time() * 1000)

    # Store the chunk data in the correct position
    if chunk_index < total_chunks and frame["chunks"][chunk_index] is None:
        frame["chunks"][chunk_index] = chunk_data
        frame["received_slices"] += 1

        # Check if we have received all chunks for this frame
        if frame["received_slices"] == total_chunks:
            # Log transmission time
            transmission_time = time.time() - timestamp / 1000
            logging.info(f"Frame {frame_number} transmission time: {transmission_time:.6f}s")

            # Reconstruct the frame
            frame_data = b''.join(frame["chunks"])

            # Process the frame with YOLO
            asyncio.create_task(process_frame_with_buffer(frame_data, frame_number, frame["timestamp"], frame["start_time"]))

            # Clean up the completed frame entry
            del incoming_frames[frame_number]

            # Log arrival time and processing start time
            logging.info(f"Frame {frame_number} fully received at {arrival_time:.6f}, started processing at {time.time():.6f}")
    else:
        logging.info(f"Invalid or duplicate slice index: {chunk_index} for frame: {frame_number}")

# Server update callback
async def update(response, key):
    logging.info(f'Updating as new sender valid in the workspace: {response}')
    await corelink.subscribe_to_stream(response['receiverID'], response['streamID'])

# Server stale callback
async def stale(response, key):
    logging.info(response)

# Subscriber callback
async def subscriber(response, key):
    logging.info(f"subscriber: {response}")

# Dropped connection callback
async def dropped(response, key):
    logging.info(f"dropped: {response}")
    
# Main processing function
async def processing():
    global sender_id

    await corelink.set_server_callback(update, 'update')
    await corelink.set_server_callback(stale, 'stale')
    await corelink.set_data_callback(callback)
    
    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    
    receiver_id = await corelink.create_receiver("detectionRaw", "ws", alert=True, echo=True)
    logging.info(f"Receiver ID: {receiver_id}")
    
    logging.info("Start receiving process frames")
    await corelink.keep_open()
    
    await corelink.set_server_callback(subscriber, 'subscriber')
    await corelink.set_server_callback(dropped, 'dropped')

    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    sender_id = await corelink.create_sender("detectionCtl", "ws", "description1")

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logging.info('Receiver terminated.')

if __name__ == "__main__":
    corelink.run(processing())
