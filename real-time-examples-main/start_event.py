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
    
    print("Connecting to Corelink...")
    await corelink.connect(username, password, host, port)

    # Create a sender for control events
    sender_id = await corelink.create_sender("CREATE", "ws", data_type = "dreamstream",metadata={"name": "Start Event Sender"})
  
    # Build the start event message with unique metadata
    event_metadata = {
        "event_hash": str(uuid.uuid4()),
        "instanceID": "instance_001",
        "itemID": "item_001",
        "participantID": "participant_001",
        "packetID": "packet_001",
        "studyName": "Study_Alpha",
        "event_name": "start"
    }
    message = json.dumps(event_metadata)
    print("Sending start event message:")
    print(message)
    await corelink.send(sender_id, message)
    print("Start event sent. Exiting...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Exiting...")
