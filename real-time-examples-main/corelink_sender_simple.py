import asyncio
import sys
import os

import corelink
from corelink import *
from corelink import processing
from time import time

VALIDATION_TIMEOUT = 15  # seconds
validConnection = False

async def callback(data_bytes, streamID, header):
    global validConnection
    
    print(f"Received data with length {len(data_bytes)} : {data_bytes.decode(encoding='UTF-8')}")

async def subscriber(response, key):
    print("subscriber: ", response)

    
async def update(response, key):
    print("update: ", response)


async def dropped(response, key):
    print("dropped", response)


async def stale(response, key):
    print("stale", response)
    
async def check_connection():
    await asyncio.sleep(VALIDATION_TIMEOUT)
    if not validConnection:
        print("Connection not validated, retrying...")

async def main():
    await corelink.set_server_callback(subscriber, 'subscriber')
    await corelink.set_server_callback(dropped, 'dropped')
    await corelink.set_server_callback(stale, 'stale')
    await corelink.set_server_callback(update, 'update')
            
    await corelink.set_data_callback(callback)
    await corelink.connect("Testuser", "Testpassword", "corelink.hpc.nyu.edu", 20012)
    sender_id = await corelink.create_sender("CREATE", "ws", data_type = "dreamstream")
    sender_id2 = await corelink.create_sender("CREATE", "ws", data_type = "sensorData")
    sender_id3 = await corelink.create_sender("CREATE", "ws", data_type = "trash")
    print(sender_id)
    streams = await corelink.active_streams()
    print('The stream:', streams)

    counter = 0
    while True:
        await corelink.send(sender_id, str(counter))
        await corelink.send(sender_id2, 'sensorData hahaa')
        await corelink.send(sender_id3, 'trash data')
        print(f'Data: {str(counter)}, Size {len(str(counter))}')
        counter += 1
        await asyncio.sleep(1)

 

corelink.run(main())
