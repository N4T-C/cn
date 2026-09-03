# Computer Networks Laboratory Project Report

## PROJECT TITLE: Distributed Socket-Based Real-Time Communication & Transport Layer Telemetry System

---

## 1. AIM & OBJECTIVES

### Aim
To design and implement a distributed, real-time client-server communication application across a Local Area Network (LAN), featuring tag-based identity routing (`#nat`, `#har`, `#ava`, `#group`), reliable WebSocket/TCP messaging, UDP/RTP-style media-stream simulation, controlled packet-loss injection, and an interactive Quality of Service (QoS) analytics console.

### Objectives
1. **Multi-Node Routing:** Implement node identification using tags and support direct and group communication through a central Python server.
2. **Transport-Layer Demonstration:** Demonstrate the reliability/latency trade-off between TCP-based messaging and UDP/RTP-style streaming behavior.
3. **Live QoS Telemetry:** Measure and visualize RTT, RFC 6298-style Smoothed RTT, RFC 3550-style interarrival jitter, observed throughput, packet loss, congestion-window state, and packet counts.
4. **Network Fault Simulation:** Inject configurable UDP datagram loss from 0% to 50% and observe the effect on stream health and jitter.
5. **Interactive Analytics:** Convert raw telemetry into a Network Health Score, protocol distribution, automatic observations, charts, searchable packet traces, and exportable logs.
6. **Cross-Device LAN Deployment:** Run the central server on `0.0.0.0:8000` and connect browsers from devices on the same Wi-Fi network.

---

## 2. THEORETICAL BACKGROUND & COMPUTER NETWORKS CONCEPTS

### 2.1 OSI & TCP/IP Protocol Stack Mapping

| Layer | Protocol / Technology | Function in Project |
|---|---|---|
| Application | HTTP, WebSocket, HTML/CSS/JavaScript | Chat UI, telemetry dashboard, signaling and API requests |
| Transport | TCP via WebSocket; UDP/RTP behavior simulation | Reliable chat, sequence/ACK tracking, low-latency stream experiment |
| Network | IPv4 | LAN addressing and server access |
| Data Link / Physical | IEEE 802.11 Wi-Fi / Ethernet | Physical LAN connectivity |

> **Implementation note:** Browser WebSockets are carried over TCP. The project's UDP/RTP stream is an application-level transport-behavior simulation: the server applies probabilistic datagram loss and records UDP/RTP-style telemetry while returning the result through the browser's WebSocket connection. This makes the experiment reproducible in a normal browser without requiring a raw-UDP browser API.

### 2.2 TCP vs UDP Comparison

| Parameter | TCP | UDP |
|---|---|---|
| Connection | Connection-oriented | Connectionless |
| Project use | Direct/group chat | Media-stream experiment |
| Reliability | ACK/retransmission behavior | Best-effort / loss-tolerant behavior |
| Header overhead used in model | 40 B TCP/IP | 28 B UDP/IP |
| Main advantage | Reliable delivery | Lower latency / no head-of-line retransmission in the simulated media path |
| Main trade-off | Retransmission can increase delay | Lost datagrams may create stream artifacts |

### 2.3 QoS Formulas

**Smoothed RTT:**

$$SRTT_{new} = (1 - \alpha)SRTT_{old} + \alpha RTT_{sample}, \quad \alpha = 0.125$$

**RFC 3550-style interarrival jitter:**

$$J(i) = J(i-1) + \frac{|D(i-1,i)| - J(i-1)}{16}$$

**Observed UDP packet loss:**

$$Loss(\%) = \frac{Dropped\ UDP\ Datagrams}{Transmitted\ UDP\ Datagrams} \times 100$$

**Observed throughput:**

$$Throughput = \frac{Bytes\ observed}{Time\ window \times 1024} \quad KB/s$$

---

## 3. SYSTEM ARCHITECTURE

```text
                         CENTRAL PYTHON SERVER
                              0.0.0.0:8000
                    ┌─────────────────────────────┐
                    │ FastAPI + WebSocket         │
                    │                             │
                    │ Tag Registry / Rooms        │
                    │ TCP SEQ / ACK Telemetry     │
                    │ UDP/RTP Loss Simulator      │
                    │ QoS Statistics Engine       │
                    │ REST Analytics API          │
                    └──────────────┬──────────────┘
                                   │
                         Wi-Fi / LAN IPv4
                                   │
                  ┌────────────────┴────────────────┐
                  │                                 │
             Browser Node                      Browser Node
               #nat                              #har/#ava
                  │                                 │
        ┌─────────┴─────────┐             ┌─────────┴─────────┐
        │ Chat / TCP tags   │             │ Chat / Stream     │
        │ Analytics Console │             │ UDP experiment    │
        └───────────────────┘             └───────────────────┘
```

---

## 4. MAJOR FEATURES IMPLEMENTED

### 4.1 Real-Time Chat
- Tag-based direct channels (`#nat`, `#har`, `#ava`).
- `#group` broadcast room.
- Image attachment support.
- Message history stored in memory during server runtime.
- TCP-style `SEQ`, `ACK`, and RTT metadata shown with messages.

### 4.2 Network Analytics Console
The new analytics workspace converts raw counters into meaningful network information:

- **Network Health Score (0–100)**
- Health classification: Excellent / Good / Degraded / Poor
- RTT and Smoothed RTT cards
- RFC 3550-style jitter card
- Observed throughput card
- UDP loss card
- Live QoS timeline
- Live throughput graph
- TCP/UDP protocol distribution doughnut chart
- Packet-volume chart
- TCP vs UDP trade-off visualization
- Automatic plain-language network insights
- Server uptime and active connection count

### 4.3 Packet Flow Inspector
The packet inspector now supports:
- Protocol filtering (TCP / UDP / ALL)
- Text search
- Timestamp
- Packet type
- Header + payload size
- Sequence / acknowledgement values
- RTT
- Drop/OK status
- CSV export
- JSON telemetry export
- Server-side telemetry reset

### 4.4 Network Fault Injection
The analytics console provides presets for:

`0% → 15% → 30% → 50%`

The user can also select any value from 0% to 50% using the slider.

The experiment is designed to demonstrate that increased loss can increase observed jitter and reduce stream quality.

### 4.5 TCP Fast Retransmit Demonstration
A dedicated control simulates a triple-duplicate-ACK fast retransmit event. The telemetry engine increments the retransmission counter and reduces the modeled TCP congestion window by half.

### 4.6 UDP/RTP Stream Playground
Two modes are exposed:
- **UDP/RTP:** immediate, loss-tolerant stream behavior.
- **TCP/Buffered:** reliable-delivery/buffering explanation for comparison.

The stream canvas displays packet/frame activity, current mode, and loss-related visual artifacts.

---

## 5. REST TELEMETRY API

### `GET /api/stats`
Returns live telemetry including:
- TCP/UDP packet counters
- TCP/UDP byte counters
- retransmission count
- UDP dropped/transmitted datagrams
- RTT and Smoothed RTT
- jitter
- simulated loss rate
- observed UDP loss rate
- observed throughput
- modeled TCP congestion window
- active connections
- packet history
- server uptime

### `POST /api/stats/config`
Used to configure packet-loss simulation and trigger the TCP fast-retransmit demonstration.

### `POST /api/stats/reset`
Clears packet history and telemetry counters for a fresh experiment.

### `GET /api/health`
Provides a lightweight server health/status response.

---

## 6. EXPERIMENT PROCEDURE

### Experiment A — TCP Chat
1. Start the server.
2. Open two browser clients.
3. Connect one as `#nat` and another as `#har`.
4. Send several messages.
5. Observe `SEQ`, `ACK`, RTT and TCP packet entries.
6. Open **Network Analytics** and observe TCP packet volume.

### Experiment B — UDP/RTP Loss Behaviour
1. Open the **UDP / RTP Stream Playground**.
2. Select UDP/RTP mode.
3. Set simulated loss to 0%.
4. Record buffer/stream behaviour.
5. Increase loss to 15%, 30%, and 50%.
6. Compare dropped packets, jitter and stream health.

### Experiment C — TCP Fast Retransmit
1. Open Network Analytics.
2. Generate TCP traffic.
3. Click **Simulate TCP Fast Retransmit**.
4. Observe the retransmission counter and modeled congestion-window reduction.

### Experiment D — Exported Packet Trace
1. Generate chat and stream traffic.
2. Filter the Packet Flow Inspector by TCP or UDP.
3. Search for `SYN`, `PSH-ACK`, `RTP_MEDIA`, or `FAST-RETRANSMIT`.
4. Export the packet trace as CSV or all telemetry as JSON.

---

## 7. EXPECTED OBSERVATIONS

| Scenario | Expected Observation |
|---|---|
| TCP chat, low loss | Low RTT, ACKs visible, reliable message delivery |
| TCP retransmit simulation | Retransmission counter increases and modeled cwnd decreases |
| UDP, 0% loss | High stream health and no dropped datagrams |
| UDP, 15–30% loss | Dropped datagrams and increasing jitter; stream health falls |
| UDP, 50% loss | Significant loss and visibly degraded stream health |

Actual values depend on generated traffic and runtime conditions and should be recorded from the live dashboard during the final experiment.

---

## 8. SCREENSHOT CHECKLIST

1. **Identity Gateway:** `#nat`, `#har`, `#ava` login options.
2. **Direct Chat:** message bubbles with SEQ/ACK/RTT metadata.
3. **Network Analytics:** health score + QoS charts + protocol mix.
4. **Fault Injection:** 30% or 50% loss with degraded stream health.
5. **Packet Inspector:** filtered TCP/UDP trace.
6. **Export:** successful CSV/JSON export.

---

## 9. CONCLUSION

The project demonstrates how transport-layer behaviour can be observed from an interactive real-time communication system. The upgraded analytics console turns raw telemetry into understandable network-health information, while the packet inspector and fault-injection controls make it possible to perform repeatable experiments on RTT, jitter, throughput, packet loss, reliability, and congestion-window behaviour.

The central design also highlights the fundamental transport trade-off: reliable TCP-style communication is suitable for messages where correctness matters, whereas loss-tolerant UDP/RTP-style behaviour is useful for real-time media where timely delivery is often more important than retransmitting every missing datagram.
