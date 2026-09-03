import asyncio
import json
import socket
import time
import os
import random
from typing import Dict, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI(title="NetChat Multi-Client Protocol Server")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "public")
os.makedirs(STATIC_DIR, exist_ok=True)

SERVER_STARTED_AT = time.time()

# -----------------------------------------------------------------------------
# Global telemetry state
# -----------------------------------------------------------------------------
network_stats = {
    "total_tcp_packets": 0,
    "total_udp_packets": 0,
    "tcp_bytes": 0,
    "udp_bytes": 0,
    "tcp_retransmissions": 0,
    "udp_packets_dropped": 0,
    "udp_packets_sent": 0,
    "current_rtt_ms": 7.4,
    "smoothed_rtt_ms": 8.0,
    "jitter_ms": 1.0,
    "simulated_loss_rate": 0.0,
    "throughput_kbps": 0.0,
    "cwnd_kb": 64.0,
    "active_connections": 0,
    "packet_history": [],
    "started_at": SERVER_STARTED_AT,
    "last_update": SERVER_STARTED_AT,
}

user_sockets: Dict[str, WebSocket] = {}
socket_users: Dict[WebSocket, str] = {}
client_seq_counters: Dict[str, int] = {}
message_store: Dict[str, List[dict]] = {}

# Used to calculate real observed throughput over a short rolling window.
throughput_samples: List[tuple[float, int]] = []
last_packet_transit_time = 0.0


def get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()

    return ip


def get_channel_key(user1: str, user2: str) -> str:
    if user2 == "#group" or user1 == "#group" or user2 == "all":
        return "#group"
    pair = sorted([user1.lower(), user2.lower()])
    return f"{pair[0]}_{pair[1]}"


def calculate_throughput_kbps(now: float) -> float:
    """Return observed application traffic over the last 5 seconds."""
    cutoff = now - 5.0
    while throughput_samples and throughput_samples[0][0] < cutoff:
        throughput_samples.pop(0)

    if len(throughput_samples) < 2:
        return round(network_stats["throughput_kbps"], 1)

    total_bytes = sum(item[1] for item in throughput_samples)
    duration = max(0.25, throughput_samples[-1][0] - throughput_samples[0][0])
    return round((total_bytes / 1024.0) / duration, 1)


def record_packet(
    protocol: str,
    p_type: str,
    src: str,
    dst: str,
    payload_size: int,
    seq: int = 0,
    ack: int = 0,
    dropped: bool = False,
    rtt: float = 0.0,
):
    global last_packet_transit_time

    header_size = 40 if protocol == "TCP" else 28
    total_size = payload_size + header_size
    now = time.time()

    transit = rtt if rtt > 0 else (
        random.uniform(4.0, 12.0) if protocol == "TCP" else random.uniform(3.0, 16.0)
    )

    if last_packet_transit_time > 0:
        d = abs(transit - last_packet_transit_time)
        network_stats["jitter_ms"] = round(
            network_stats["jitter_ms"] + (d - network_stats["jitter_ms"]) / 16.0, 2
        )
    last_packet_transit_time = transit

    if protocol == "TCP":
        network_stats["total_tcp_packets"] += 1
        network_stats["tcp_bytes"] += total_size
        network_stats["current_rtt_ms"] = round(transit, 2)
        network_stats["smoothed_rtt_ms"] = round(
            0.875 * network_stats["smoothed_rtt_ms"] + 0.125 * transit, 2
        )
    else:
        network_stats["total_udp_packets"] += 1
        network_stats["udp_bytes"] += total_size
        network_stats["udp_packets_sent"] += 1
        if dropped:
            network_stats["udp_packets_dropped"] += 1

    throughput_samples.append((now, total_size))
    network_stats["throughput_kbps"] = calculate_throughput_kbps(now)
    network_stats["last_update"] = now

    packet_entry = {
        "id": len(network_stats["packet_history"]) + 1,
        "epoch": now,
        "timestamp": time.strftime("%H:%M:%S", time.localtime(now))
        + f".{int((now % 1) * 1000):03d}",
        "protocol": protocol,
        "type": p_type,
        "src": src,
        "dst": dst,
        "payload_bytes": payload_size,
        "header_bytes": header_size,
        "total_bytes": total_size,
        "seq": seq,
        "ack": ack,
        "dropped": dropped,
        "rtt_ms": round(transit, 2),
        "jitter_ms": network_stats["jitter_ms"],
    }

    network_stats["packet_history"].append(packet_entry)
    if len(network_stats["packet_history"]) > 120:
        network_stats["packet_history"].pop(0)


def reset_telemetry():
    global last_packet_transit_time
    network_stats.update(
        {
            "total_tcp_packets": 0,
            "total_udp_packets": 0,
            "tcp_bytes": 0,
            "udp_bytes": 0,
            "tcp_retransmissions": 0,
            "udp_packets_dropped": 0,
            "udp_packets_sent": 0,
            "current_rtt_ms": 7.4,
            "smoothed_rtt_ms": 8.0,
            "jitter_ms": 1.0,
            "throughput_kbps": 0.0,
            "cwnd_kb": 64.0,
            "packet_history": [],
            "last_update": time.time(),
        }
    )
    throughput_samples.clear()
    last_packet_transit_time = 0.0


@app.get("/api/stats")
async def get_stats():
    now = time.time()
    network_stats["throughput_kbps"] = calculate_throughput_kbps(now)

    total_udp = network_stats["udp_packets_sent"]
    observed_loss = (
        network_stats["udp_packets_dropped"] / total_udp * 100 if total_udp else 0.0
    )

    total_packets = network_stats["total_tcp_packets"] + network_stats["total_udp_packets"]
    tcp_share = network_stats["total_tcp_packets"] / total_packets * 100 if total_packets else 0.0
    udp_share = 100 - tcp_share if total_packets else 0.0

    return {
        **network_stats,
        "observed_udp_loss_rate": round(observed_loss, 2),
        "tcp_share_percent": round(tcp_share, 1),
        "udp_share_percent": round(udp_share, 1),
        "uptime_seconds": round(now - SERVER_STARTED_AT, 1),
        "server_time": now,
    }


@app.post("/api/stats/config")
async def update_stats_config(payload: dict):
    if "loss_rate" in payload:
        network_stats["simulated_loss_rate"] = max(
            0.0, min(1.0, float(payload["loss_rate"]))
        )

    if payload.get("retransmit"):
        network_stats["tcp_retransmissions"] += 1
        network_stats["cwnd_kb"] = max(16.0, round(network_stats["cwnd_kb"] * 0.5, 1))
        record_packet(
            "TCP",
            "FAST-RETRANSMIT",
            "Client",
            "Server:8000",
            0,
            seq=0,
            ack=0,
            rtt=max(4.0, network_stats["current_rtt_ms"] * 1.4),
        )

    return {
        "status": "ok",
        "current_loss_rate": network_stats["simulated_loss_rate"],
        "cwnd_kb": network_stats["cwnd_kb"],
    }


@app.post("/api/stats/reset")
async def reset_stats():
    reset_telemetry()
    return {"status": "ok", "message": "Telemetry counters and packet history reset."}


@app.get("/api/health")
async def health():
    return {
        "status": "online",
        "service": "NetChat telemetry server",
        "server_ip": get_lan_ip(),
        "port": 8000,
        "active_connections": len(user_sockets),
        "uptime_seconds": round(time.time() - SERVER_STARTED_AT, 1),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_ip = websocket.client.host if websocket.client else "127.0.0.1"
    user_tag = ""

    # TCP 3-way handshake visualization.
    record_packet("TCP", "SYN", client_ip, "Server:8000", 0, seq=100)
    record_packet("TCP", "SYN-ACK", "Server:8000", client_ip, 0, seq=500, ack=101)
    record_packet("TCP", "ACK", client_ip, "Server:8000", 0, seq=101, ack=501)

    try:
        while True:
            data_text = await websocket.receive_text()
            data = json.loads(data_text)
            msg_type = data.get("type")

            if msg_type == "login":
                raw_tag = data.get("tag", "").strip().lower()
                user_tag = raw_tag if raw_tag.startswith("#") else f"#{raw_tag}"
                if not user_tag or user_tag == "#":
                    user_tag = f"#node_{random.randint(100, 999)}"

                user_sockets[user_tag] = websocket
                socket_users[websocket] = user_tag
                network_stats["active_connections"] = len(user_sockets)

                print(
                    f"[+] Client logged in: {user_tag} from {client_ip}. "
                    f"Active users: {list(user_sockets.keys())}"
                )

                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "login_success",
                            "tag": user_tag,
                            "server_ip": get_lan_ip(),
                            "active_users": list(user_sockets.keys()),
                        }
                    )
                )

                user_list_payload = json.dumps(
                    {"type": "user_status", "active_users": list(user_sockets.keys())}
                )
                for s in list(user_sockets.values()):
                    try:
                        await s.send_text(user_list_payload)
                    except Exception:
                        pass
                continue

            if msg_type == "chat":
                sender = socket_users.get(websocket, user_tag)
                raw_rec = data.get("recipient", "#group").strip().lower()
                recipient = raw_rec if raw_rec.startswith("#") else f"#{raw_rec}"
                content = data.get("content", "")
                image_data = data.get("image")
                msg_kind = data.get("kind", "text")
                sent_ts = data.get("client_timestamp", time.time() * 1000)

                client_seq = client_seq_counters.get(sender, 1000) + 1
                client_seq_counters[sender] = client_seq

                payload_len = len(image_data) if image_data else len(content.encode("utf-8"))
                calc_rtt = max(2.0, round((time.time() * 1000 - sent_ts), 1))

                record_packet(
                    "TCP",
                    "PSH-ACK",
                    sender,
                    recipient,
                    payload_len,
                    seq=client_seq,
                    ack=client_seq + payload_len,
                    rtt=calc_rtt,
                )

                msg_payload = {
                    "type": "chat",
                    "sender": sender,
                    "recipient": recipient,
                    "channel_key": get_channel_key(sender, recipient),
                    "content": content,
                    "image": image_data,
                    "kind": msg_kind,
                    "timestamp": time.strftime("%I:%M %p"),
                    "seq": client_seq,
                    "ack": client_seq + payload_len,
                    "rtt_ms": calc_rtt,
                }

                channel_key = msg_payload["channel_key"]
                message_store.setdefault(channel_key, []).append(msg_payload)
                if len(message_store[channel_key]) > 100:
                    message_store[channel_key].pop(0)

                json_str = json.dumps(msg_payload)

                if recipient == "#group" or recipient == "all":
                    for ws_client in list(user_sockets.values()):
                        try:
                            await ws_client.send_text(json_str)
                        except Exception:
                            pass
                else:
                    target_ws = user_sockets.get(recipient)
                    if target_ws:
                        try:
                            await target_ws.send_text(json_str)
                        except Exception as exc:
                            print(f"Error sending to {recipient}: {exc}")
                    try:
                        await websocket.send_text(json_str)
                    except Exception:
                        pass

                try:
                    await websocket.send_text(
                        json.dumps(
                            {
                                "type": "tcp_ack",
                                "seq": client_seq,
                                "ack": client_seq + payload_len,
                                "rtt_ms": calc_rtt,
                            }
                        )
                    )
                except Exception:
                    pass

            elif msg_type == "get_history":
                target = data.get("target", "#group").strip().lower()
                if not target.startswith("#"):
                    target = f"#{target}"
                ch_key = get_channel_key(user_tag, target)
                history = message_store.get(ch_key, [])
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "history_response",
                            "target": target,
                            "messages": history,
                        }
                    )
                )

            elif msg_type == "udp_stream":
                chunk_index = data.get("chunk_index", 0)
                chunk_size = int(data.get("chunk_size", 1400))
                is_dropped = random.random() < network_stats["simulated_loss_rate"]

                record_packet(
                    "UDP",
                    "RTP_MEDIA",
                    "StreamServer:5004",
                    f"{user_tag}:UDP",
                    chunk_size,
                    seq=chunk_index,
                    dropped=is_dropped,
                )

                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "udp_chunk_response",
                            "chunk_index": chunk_index,
                            "dropped": is_dropped,
                            "bytes": chunk_size,
                            "loss_rate": network_stats["simulated_loss_rate"],
                        }
                    )
                )

            elif msg_type in ["call_offer", "call_answer", "call_end"]:
                target_user = data.get("target_user", "").strip().lower()
                if target_user and not target_user.startswith("#"):
                    target_user = f"#{target_user}"
                data["sender"] = user_tag
                record_packet(
                    "UDP",
                    f"RTP_{msg_type.upper()}",
                    user_tag,
                    target_user or "Broadcast",
                    160,
                )

                if target_user and target_user in user_sockets:
                    await user_sockets[target_user].send_text(json.dumps(data))
                else:
                    for ws in list(user_sockets.values()):
                        if ws != websocket:
                            await ws.send_text(json.dumps(data))

    except WebSocketDisconnect:
        if websocket in socket_users:
            disconnected_tag = socket_users.pop(websocket)
            if disconnected_tag in user_sockets:
                del user_sockets[disconnected_tag]
            network_stats["active_connections"] = len(user_sockets)
            record_packet("TCP", "FIN-ACK", disconnected_tag, "Server:8000", 0)
            print(f"[-] Client disconnected: {disconnected_tag}")

            user_list_payload = json.dumps(
                {"type": "user_status", "active_users": list(user_sockets.keys())}
            )
            for ws in list(user_sockets.values()):
                try:
                    await ws.send_text(user_list_payload)
                except Exception:
                    pass


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


def main():
    lan_ip = get_lan_ip()
    port = 8000
    print("\n" + "=" * 70)
    print("  NETCHAT - REAL-TIME PROTOCOL & TELEMETRY SERVER")
    print("=" * 70)
    print(f"  [+] Local URL:       http://localhost:{port}")
    print(f"  [+] Wi-Fi URL:       http://{lan_ip}:{port}")
    print("  [+] Tags:            #nat, #har, #ava, #group, etc.")
    print("  [+] Analytics API:   /api/stats")
    print("=" * 70 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
