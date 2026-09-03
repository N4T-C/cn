# NetChat — Real-Time Protocol & Telemetry Lab

A Computer Networks mini-project demonstrating real-time chat, TCP-style packet telemetry, UDP/RTP-style stream loss simulation, and a live analytics dashboard.

## Quick start (Windows)

### 1. Open PowerShell in this folder

```powershell
cd path\to\cn_project
```

### 2. Optional but recommended: create a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, use Command Prompt:

```cmd
.venv\Scripts\activate
```

### 3. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 4. Start the server

```powershell
python server.py
```

The terminal prints two addresses:

- PC: `http://localhost:8000`
- Wi-Fi/LAN: `http://<YOUR-LAN-IP>:8000`

### 5. Open the app

Open `http://localhost:8000` in a browser and choose a tag such as `#nat`.

For a second device, connect it to the **same Wi-Fi network** and open the LAN URL printed by the server. Choose another tag such as `#har`.

## Recommended demo flow

1. Connect `#nat` and `#har`.
2. Send a few TCP chat messages and show SEQ/ACK/RTT metadata.
3. Open **Network Analytics** from the left navigation.
4. Show the Network Health Score and live QoS charts.
5. Open **UDP / RTP Stream Playground**.
6. Try 0%, 15%, 30% and 50% loss presets.
7. Return to Analytics and show the packet trace and changing loss/jitter.
8. Click **Simulate TCP Fast Retransmit** and show the retransmission counter/cwnd change.
9. Export CSV/JSON if required for the report.

## Important implementation note

A browser WebSocket is transported over TCP. The UDP/RTP part of this project is therefore a **controlled application-level simulation** of UDP/RTP behaviour rather than a raw UDP socket directly from browser JavaScript. The simulation is intentional so the project can run from a normal browser while still demonstrating packet loss, jitter, stream health and transport trade-offs.

## Project structure

```text
cn_project/
├── public/
│   ├── index.html      # UI structure
│   ├── app.js          # WebSocket client + analytics logic
│   └── style.css       # Dark responsive UI
├── server.py           # FastAPI/WebSocket telemetry server
├── requirements.txt    # Python dependencies
├── REPORT.md           # Updated project report
└── README.md           # Setup and demo guide
```

## Troubleshooting

### `python` is not recognized
Install Python and make sure **Add Python to PATH** is enabled.

### `ModuleNotFoundError: fastapi`
Run:

```powershell
python -m pip install -r requirements.txt
```

### Mobile cannot connect
- Put the phone and PC on the same Wi-Fi.
- Use the LAN IP printed by `server.py`, not `localhost`.
- Allow Python through Windows Firewall if Windows asks.
- If the network is configured as a public/restricted network, temporarily use a trusted private network for the lab.

### Port 8000 is already in use
Close the other server using port 8000, or change the port in `server.py` and update the URLs accordingly.
