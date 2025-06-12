import asyncio
import json
import uuid
import corelink
from corelink import *

async def main():
    # Connection parameters
    username = "Testuser"
    password = "Testpassword"
    host = "corelink.hpc.nyu.edu"
    port = 20012
    workspace = "Holodeck11"
    protocol = "ws"
    stream_type = "dreamstreamaudio"  # control stream

    print("Connecting to Corelink...")
    await corelink.connect(username, password, host, port)

    # Create a sender for control events
    sender_id = await corelink.create_sender("CREATE", "ws", data_type = "dreamstream",metadata={"name": "Stop Event Sender"})

    print("Sender ID:", sender_id)

    # Build the stop event message with unique metadata
    event_metadata = {
        "event_hash": str(uuid.uuid4()),
        "instanceID": "instance_001",
        "itemID": "item_001",
        "participantID": "participant_001",
        "packetID": "packet_stop_001",
        "studyName": "Study_Alpha",
        "event_name": "stop"
    }
    message = json.dumps(event_metadata)
    print("Sending stop event message:")
    print(message)
    await corelink.send(sender_id, message)
    print("Stop event sent. Exiting...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Exiting...")
