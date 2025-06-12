import time
from ctypes import *
from ctypes import c_int32, byref
import numpy as np
import mpenum
import asyncio
import sys
import os
import queue

import corelink
from corelink import *
from corelink import processing

# ---------------------------
# BIOPAC Device Configuration
# ---------------------------

# Constants for BIOPAC
SAMPLE_RATE_HZ = 1000  # Sample rate in Hz (adjust as needed)
NUM_CHANNELS = 16      # Total analog channels supported

# Load the BIOPAC DLL
mpdev = cdll.LoadLibrary("C:/Program Files (x86)/BIOPAC Systems, Inc/BIOPAC Hardware API 2.2.3 Research/x64/mpdev.dll")

def initialize_device():
    """
    Initialize the BIOPAC device and configure it for data acquisition.
    """
    print("Connecting to MP device...")
    mpdev.connectMPDev.argtypes = [c_int, c_int, c_char_p]
    ret_code = mpdev.connectMPDev(mpenum.MP160, mpenum.MPUDP, b'auto')  # Use auto or specify serial number
    if ret_code != mpenum.MPSUCCESS:
        raise Exception(f"Failed to connect to MP device. Error Code: {ret_code}")

    print("Setting acquisition channels...")
    channels = (c_bool * NUM_CHANNELS)(*([True] * NUM_CHANNELS))  # Activate all channels
    ret_code = mpdev.setAcqChannels(channels)
    if ret_code != mpenum.MPSUCCESS:
        raise Exception(f"Failed to set acquisition channels. Error Code: {ret_code}")

    print(f"Setting sample rate to {SAMPLE_RATE_HZ} Hz...")
    mpdev.setSampleRate.argtypes = [c_double]
    # Note: Adjust the value passed (here 0.5) per your device's API (e.g., 0.5 may equal 500 Hz)
    #This depends on how powerful your cpu is. 0.5=500, 0.3=1000 0.1=3,500
    retval = mpdev.setSampleRate(0.4)
    if retval != mpenum.MPSUCCESS:
        raise Exception(f"Failed to set up sample rate to {SAMPLE_RATE_HZ}. Error code: {retval}")

    print("Starting acquisition Daemon...")
    retval = mpdev.startMPAcqDaemon()
    if retval != mpenum.MPSUCCESS:
        raise Exception("Failed to set up acquisition daemon")

    print("Device initialized successfully.")

def acquire_data(data_queue):
    """
    Acquire data from BIOPAC and stream it out by placing messages on a queue.
    Each message is a comma‑separated string starting with a counter followed by channel data.
    For example: "123,0.12,0.34,0.56,..."
    """
    print("Starting BIOPAC real-time data acquisition. Press Ctrl+C to stop.")
    ret_code = mpdev.startAcquisition()
    buffer = (c_double * NUM_CHANNELS)()
    values_read = c_int32(0)
    total_counter = 0   # Used for the message header
    second_counter = 0  # For per-second logging
    startTime = time.time()

    if ret_code != mpenum.MPSUCCESS:
        raise Exception(f"Failed to start Acquisition. Error code: {ret_code}")
    try:
        while True:
            ret_code = mpdev.receiveMPData(buffer, NUM_CHANNELS, byref(values_read))
            if ret_code != mpenum.MPSUCCESS:
                print(f"Error reading data. Code: {ret_code}")
                continue

            # Extract the sample data from the buffer (one value per channel)
            sample_data = [buffer[i] for i in range(NUM_CHANNELS)]
            timestamp = time.time()
            # Build the message: counter followed by channel data (all comma-separated)
            message = f"{total_counter}," + ",".join(str(x) for x in sample_data) + f", {timestamp}, " + 'MP160'  # PLEASE CHANGE THE DEVICE NAME HERE
            # Put the message on the queue so that the async sender can transmit it
            data_queue.put(message)

            total_counter += 1
            second_counter += 1
            current_time = time.time()
            if current_time - startTime >= 1.0:
                print(f"BIOPAC - Samples received in the last second: {second_counter}")
                second_counter = 0
                startTime = current_time

            # Sleep to approximately maintain the desired sample rate
    except KeyboardInterrupt:
        print("\nStopping BIOPAC data acquisition...")

def cleanup_device():
    """
    Disconnect from the BIOPAC device and clean up.
    """
    print("Disconnecting from MP device...")
    ret_code = mpdev.disconnectMPDev()
    # Optionally check ret_code for successful disconnection

# ---------------------------
# Corelink Realtime Data Sending
# ---------------------------

VALIDATION_TIMEOUT = 15  # seconds
validConnection = False

async def callback(data_bytes, streamID, header):
    global validConnection
    print(f"Received data with length {len(data_bytes)} : {data_bytes.decode(encoding='UTF-8')}")

async def subscriber(response, key):
    print("subscriber:", response)

async def update(response, key):
    print("update:", response)

async def dropped(response, key):
    print("dropped:", response)

async def stale(response, key):
    print("stale:", response)

async def send_biopac_data(sender_id, data_queue):
    """
    Asynchronously wait for BIOPAC messages on the queue and send them using Corelink.
    """
    loop = asyncio.get_running_loop()
    while True:
        # Use run_in_executor to blockingly wait for a new message from the queue
        message = await loop.run_in_executor(None, data_queue.get)
        await corelink.send(sender_id, message)
      #  print(f"Corelink - Sent message: {message}")

async def corelink_setup():
    """
    Set up the Corelink connection, register callbacks, and return the sender ID.
    """
    await corelink.set_server_callback(subscriber, 'subscriber')
    await corelink.set_server_callback(dropped, 'dropped')
    await corelink.set_server_callback(stale, 'stale')
    await corelink.set_server_callback(update, 'update')
            
    await corelink.set_data_callback(callback)
    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    sender_id = await corelink.create_sender("CREATE", "ws", data_type="sensorData")
    print("Corelink - Sender ID:", sender_id)
    streams = await corelink.active_streams()
    print("Corelink - Active streams:", streams)
    return sender_id

# ---------------------------
# Main Application
# ---------------------------

async def main():
    # Initialize the BIOPAC device first.
    initialize_device()

    # Create a thread-safe queue to pass BIOPAC data to the Corelink sender.
    data_queue = queue.Queue()

    # Set up Corelink and obtain a sender ID.
    sender_id = await corelink_setup()

    # Run BIOPAC acquisition in a separate thread.
    bio_thread = asyncio.create_task(asyncio.to_thread(acquire_data, data_queue))

    # Run the asynchronous task that sends data from the queue over Corelink.
    corelink_sender_task = asyncio.create_task(send_biopac_data(sender_id, data_queue))

    # Wait for both tasks concurrently.
    await asyncio.gather(bio_thread, corelink_sender_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Exiting...")
    finally:
        cleanup_device()
