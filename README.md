# NetChat • Distributed Transport Layer Protocol & Telemetry System

A real-time socket communication system and network telemetry analyzer built for Computer Networks lab demonstration.

## Features
- **Real-Time TCP Chat & Image Transmission**: Direct peer-to-peer and broadcast messaging via persistent WebSockets.
- **Identity Routing Gateway**: Unique `#tag` identification system (`#nat`, `#har`, `#ava`, `#group`).
- **UDP Media Stream Simulation**: RTP datagram chunk streamer with packet loss injection and buffer health visualization.
- **Live Quality of Service (QoS) Telemetry**:
  - Round Trip Time (RTT) & RFC 6298 Smoothed RTT
  - RFC 3550 Interarrival Jitter dynamics
  - Live Bandwidth / Throughput streaming (KB/s)
  - Packet Flow Inspector Table (L3/L4 traces)
- **Multi-Device LAN / Cloud Deployment**: Works over local Wi-Fi or hosted on Render.

## Tech Stack
- **Backend**: Python 3 (FastAPI, Uvicorn, WebSockets)
- **Frontend**: HTML5, CSS3, Vanilla JavaScript, Chart.js

## Run Locally
```bash
pip install -r requirements.txt
python server.py
```
Open `http://localhost:8000` in your browser.
