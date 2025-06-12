# BIOPAC Sender & Corelink HTML Receiver & Real-Time Data Visualization

This project demonstrates a browser-based receiver that uses the Corelink browser client to subscribe to, plot, and manage live data streams from a BIOPAC Corelink Python sender. The receiver is built into an HTML file and uses Plotly for real-time charting. This project is a collaboration between the Real-Time Applications team of NYU Tandon's High-Speed Research Network VIP group and NYU Steinhardt's CREATE Lab.

## Overview

* **Sender:** Sends BIOPAC data in real-time.
* **Login:** The receiver connects to the Corelink server.
* **Workspace Selection:** Choose a workspace (e.g., "Holodeck").
* **List Streams:** Click **List Streams** to create and subscribe a receiver to the selected workspace.
* **Plot Streams:** Available streams are listed in the UI. Click **Plot** to visualize incoming data in real time.
* **Kill Stream:** Unsubscribes a single stream. (Note: this does not fully stop the network traffic.)
* **Disconnect All:** Terminates the connection to Corelink entirely.

## Important Usage Instructions

1. **Receiver Initialization First**
   *Before starting the sender, open the HTML file in your browser and click **List Streams**.*
   This initializes a receiver that can subscribe to streams. Without this step, incoming data will not be received.

2. **Start the Sender**
   After initializing the receiver, run your Python sender in the same workspace (e.g., "Holodeck").
   You should see subscriber events on the sender side and data plotted live in the browser.

   **To send data from the machine connected to BIOPAC MP150/160**, run:

   ```bash
   python3 rtp_corelink.py
   ```

   If more channels or different sensor data are needed, modify the configuration in the script to include additional BIOPAC channels and ensure the corresponding hardware is connected.

3. **Receiving Options**
   On the receiver device, use one of the following based on the number of active senders:

   ```bash
   python3 duoReceiveToDB.py   # for receiving from multiple senders simultaneously
   python3 receiveToDB.py      # for receiving from a single sender
   ```

   These scripts store received data to the **NYU HPC Datalake**.

4. **Managing Stored Data**
   To interact with the stored data:

   * **Delete all data from Datalake:**

     ```bash
     python3 datalakeHelper/boto3clean.py
     ```

   * **Download data from a specific experiment:**

     ```bash
     python3 datalakeHelper/boto3downloadfolders.py
     ```

5. **Managing Streams**

   * Use **Kill Stream** to unsubscribe from a specific stream.
   * Use **Disconnect All** to terminate the entire Corelink session.

6. **BIOPAC Side Notes**

   * Requires the **BIOPAC Realtime Data Acquisition License**.
   * Compatible with BIOPAC MP150 and MP160 devices.
   * Use the provided **X86** folder for necessary DLLs.

## Prerequisites

* **Corelink Browser Client Library**
  Loaded from:
  [https://corelink.hpc.nyu.edu/client/browser/corelink.browser.lib.js](https://corelink.hpc.nyu.edu/client/browser/corelink.browser.lib.js)

* **Plotly**
  Loaded from:
  [https://cdn.plot.ly/plotly-latest.min.js](https://cdn.plot.ly/plotly-latest.min.js)

* **Python Sender**
  Use the provided `rtp_corelink.py` script or your own version to send data using Corelink.
  Make sure the workspace and data type match those expected by the receiver.

## Running the Receiver

1. **Open the HTML File**
   Open the provided HTML file in a browser.

2. **Login**
   Click the **Login** button and enter your Corelink credentials.

3. **Select Workspace**
   Click **Select Workspace** and choose a workspace (e.g., "Holodeck").

4. **List Streams**
   Click **List Streams** to initialize a receiver in the selected workspace.
   *Important: Do this before starting the sender.*

5. **Start the Sender**
   On the BIOPAC machine, run:

   ```bash
   python3 rtp_corelink.py
   ```

6. **Receiving Options**
   On the receiver device:

   ```bash
   python3 duoReceiveToDB.py   # multiple senders
   python3 receiveToDB.py      # single sender
   ```

7. **Visualize or Manage Streams**

   * Click **Plot** next to a stream to view live data.
   * Click **Kill Stream** to stop individual streams.
   * Click **Disconnect All** to close all connections.

## Troubleshooting

* **No Streams Appearing**
  Ensure **List Streams** was clicked before running the sender.

* **Zero Data / No Updates**
  Confirm:

  * Sender is actively transmitting numeric values.
  * Workspace and data types match between sender and receiver.

* **Kill Doesn’t Stop Network Traffic**
  Using delete stream will disconnect the on going stream, but the sender will not stop and will keep sending.
