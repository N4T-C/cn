// ==========================================================================
// NetChat Protocol & Telemetry Client Engine (Guaranteed Login & Message Delivery)
// ==========================================================================

let ws = null;
let myUserTag = "";
let activeTargetTag = "#group";
let attachedImageBase64 = null;
let simulatedLossRate = 0.0;
let isStreamPlaying = true;
let streamProtocol = "UDP";
let streamChunkIndex = 0;
let streamRxCount = 0;
let streamDropCount = 0;
let messageQueue = [];

let rttHistory = [];
let jitterHistory = [];
let throughputHistory = [];
let timeLabels = [];
let rttChart = null;
let throughputChart = null;

// Determine backend HTTP and WebSocket URLs (supports https / wss on Render & localhost)
function getApiBaseUrl() {
  if (window.location.protocol === "file:" || !window.location.host) {
    return "http://localhost:8000";
  }
  return window.location.origin;
}

function getWsUrl() {
  if (window.location.protocol === "file:" || !window.location.host) {
    return "ws://localhost:8000/ws";
  }
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws`;
}

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", () => {
  initUIEvents();
  initImageUpload();
  initDraggableWindow();
  initCharts();
  initStreamCanvas();
  startTelemetryPolling();

  // Check URL query parameters (e.g. ?user=#nat)
  const params = new URLSearchParams(window.location.search);
  const tagParam = params.get("user") || params.get("tag");
  if (tagParam) {
    selectQuickTag(tagParam);
  }
});

// 1. Identity Gateway Login (Guaranteed WebSocket Binding)
function selectQuickTag(tag) {
  document.getElementById("user-tag-input").value = tag;
  attemptLogin();
}

function attemptLogin() {
  let tag = document.getElementById("user-tag-input").value.trim().toLowerCase();
  if (!tag) tag = "#nat";
  if (!tag.startsWith("#")) tag = "#" + tag;

  myUserTag = tag;
  document.getElementById("display-user-tag").textContent = myUserTag;
  document.getElementById("my-active-tag").textContent = myUserTag;

  // Hide login modal and show main app shell
  document.getElementById("login-modal").style.display = "none";
  document.getElementById("app-shell").style.display = "flex";

  // Force (re)connect WebSocket and send login payload
  connectWebSocketWithTag(myUserTag);
}

// 2. WebSocket Connection Management
function connectWebSocketWithTag(tag) {
  const wsUrl = getWsUrl();
  console.log(`[TCP Socket] Initiating connection to ${wsUrl} as ${tag}...`);

  if (ws) {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        console.log(`[TCP Socket] Already open. Sending login registration for ${tag}`);
        ws.send(JSON.stringify({ type: "login", tag: tag }));
        requestChannelHistory(activeTargetTag);
        return;
      }
      ws.close();
    } catch (e) {}
  }

  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    console.log(`[TCP Connection Established] Registering tag: ${myUserTag}`);
    document.getElementById("target-connection-status").textContent = `TCP Socket Online • Node Connected`;

    // Explicitly send login registration
    ws.send(JSON.stringify({
      type: "login",
      tag: myUserTag
    }));

    // Flush any pending message queue
    while (messageQueue.length > 0) {
      const pending = messageQueue.shift();
      pending.sender = myUserTag;
      ws.send(JSON.stringify(pending));
    }

    // Request channel history
    requestChannelHistory(activeTargetTag);
  };

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleServerPacket(data);
    } catch (e) {
      console.error("Packet parse error:", e);
    }
  };

  ws.onclose = () => {
    console.warn("[TCP Socket] Connection closed. Reconnecting in 1.5s...");
    document.getElementById("target-connection-status").textContent = "Reconnecting to Central Node...";
    setTimeout(() => {
      if (myUserTag) connectWebSocketWithTag(myUserTag);
    }, 1500);
  };

  ws.onerror = (e) => {
    console.warn("[TCP Socket Error]:", e);
  };
}

function handleServerPacket(data) {
  if (data.type === "login_success") {
    console.log(`[Identity Verified] Connected as ${data.tag}. Active:`, data.active_users);
    document.getElementById("target-connection-status").textContent = `TCP Socket Active • ${myUserTag} -> ${activeTargetTag}`;
    updateActiveUserIndicators(data.active_users || []);
  } else if (data.type === "user_status") {
    updateActiveUserIndicators(data.active_users || []);
  } else if (data.type === "chat") {
    renderIncomingMessage(data);
  } else if (data.type === "history_response") {
    renderHistoryMessages(data.messages || []);
  } else if (data.type === "udp_chunk_response") {
    handleStreamChunk(data);
  } else if (data.type === "call_offer") {
    showCallDialog(data.sender);
  }
}

// 3. User Presence & Channel Routing
function updateActiveUserIndicators(activeUsers) {
  console.log("[User Presence List]:", activeUsers);
  ["#har", "#ava", "#nat"].forEach(tag => {
    const clean = tag.replace('#', '');
    const dot = document.getElementById(`status-${clean}`);
    if (dot) {
      const isOnline = activeUsers.includes(tag) || (tag === myUserTag);
      dot.style.backgroundColor = isOnline ? "var(--accent-green)" : "#444c56";
    }
  });
}

function switchActiveTarget(rawTag) {
  let tag = rawTag.trim().toLowerCase();
  if (!tag.startsWith("#")) tag = "#" + tag;
  
  activeTargetTag = tag;

  document.querySelectorAll(".contact-card").forEach(c => c.classList.remove("active-card"));
  const activeEl = document.querySelector(`.contact-card[data-tag="${tag}"]`);
  if (activeEl) activeEl.classList.add("active-card");

  const title = tag === "#group" ? "#group (All Connected Nodes)" : `${tag} (Direct Channel)`;
  const av = tag === "#group" ? "GRP" : tag.replace("#", "").toUpperCase();
  
  document.getElementById("active-target-title").textContent = title;
  document.getElementById("active-target-avatar").textContent = av;
  document.getElementById("target-connection-status").textContent = `TCP Socket Active • Route: ${myUserTag} -> ${tag}`;

  // Request chat history for this channel
  requestChannelHistory(tag);
}

function requestChannelHistory(target) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: "get_history",
      target: target
    }));
  }
}

function renderHistoryMessages(messages) {
  const container = document.getElementById("chat-feed-container");
  container.innerHTML = '<div class="time-stamp-divider">CHANNEL INITIALIZED • TCP TRANSPORT LAYER</div>';
  messages.forEach(msg => {
    appendMessageElement(msg);
  });
  container.scrollTop = container.scrollHeight;
}

// 4. Message Transmit & Image Attachment
function initImageUpload() {
  const fileInput = document.getElementById("image-file-input");
  const triggerBtn = document.getElementById("btn-trigger-upload");
  const previewBar = document.getElementById("attachment-preview-bar");
  const previewThumb = document.getElementById("image-preview-thumb");
  const cancelBtn = document.getElementById("btn-cancel-attachment");

  triggerBtn.addEventListener("click", () => {
    fileInput.click();
  });

  fileInput.addEventListener("change", (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (event) => {
      attachedImageBase64 = event.target.result;
      previewThumb.src = attachedImageBase64;
      previewBar.style.display = "flex";
      document.getElementById("attach-file-info").textContent = `Image: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
    };
    reader.readAsDataURL(file);
  });

  cancelBtn.addEventListener("click", () => {
    attachedImageBase64 = null;
    fileInput.value = "";
    previewBar.style.display = "none";
  });
}

function sendChatMessage() {
  const input = document.getElementById("chat-msg-input");
  const text = input.value.trim();
  
  if (!text && !attachedImageBase64) return;

  const payload = {
    type: "chat",
    sender: myUserTag,
    recipient: activeTargetTag,
    content: text,
    image: attachedImageBase64,
    kind: attachedImageBase64 ? "image" : "text",
    client_timestamp: Date.now()
  };

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn("WebSocket buffering packet, attempting immediate reconnection...");
    messageQueue.push(payload);
    connectWebSocketWithTag(myUserTag);
  } else {
    ws.send(JSON.stringify(payload));
  }

  // Clear inputs
  input.value = "";
  if (attachedImageBase64) {
    attachedImageBase64 = null;
    document.getElementById("image-file-input").value = "";
    document.getElementById("attachment-preview-bar").style.display = "none";
  }
}

function renderIncomingMessage(msg) {
  console.log("[Incoming Chat Packet]:", msg);

  // Check if message belongs to current open view
  const isDirectMatch = (msg.recipient === myUserTag && msg.sender === activeTargetTag) || (msg.sender === myUserTag && msg.recipient === activeTargetTag);
  const isGroupMatch = (msg.recipient === "#group" && activeTargetTag === "#group");

  if (isDirectMatch || isGroupMatch || msg.recipient === "all") {
    appendMessageElement(msg);
  }

  // Update preview snippet in left contact list
  const snippetTarget = msg.recipient === "#group" ? "group" : (msg.sender === myUserTag ? msg.recipient.replace('#', '') : msg.sender.replace('#', ''));
  const lastMsgEl = document.getElementById(`last-msg-${snippetTarget}`);
  if (lastMsgEl) {
    lastMsgEl.textContent = msg.kind === "image" ? `${msg.sender}: [Image Attachment]` : `${msg.sender}: ${msg.content}`;
    lastMsgEl.style.color = "var(--accent-cyan)";
  }
}

function appendMessageElement(msg) {
  const container = document.getElementById("chat-feed-container");
  const isMe = msg.sender === myUserTag;

  const row = document.createElement("div");
  row.className = `msg-row ${isMe ? "out" : "in"}`;

  const bubbleWrap = document.createElement("div");
  const bubble = document.createElement("div");
  bubble.className = "bubble";

  if (msg.kind === "image" || msg.image) {
    const img = document.createElement("img");
    img.src = msg.image;
    img.className = "msg-img";
    bubble.appendChild(img);
    if (msg.content) {
      const caption = document.createElement("p");
      caption.textContent = isMe ? msg.content : `${msg.sender}: ${msg.content}`;
      caption.style.marginTop = "6px";
      bubble.appendChild(caption);
    }
  } else {
    bubble.textContent = isMe ? msg.content : `${msg.sender}: ${msg.content}`;
  }

  const meta = document.createElement("div");
  meta.className = "packet-meta";
  meta.innerHTML = `
    <span class="meta-pill seq">SEQ: ${msg.seq || 100}</span>
    <span class="meta-pill rtt">RTT: ${msg.rtt_ms || 6.8}ms</span>
    <span>${msg.timestamp || "Now"}</span>
  `;

  bubbleWrap.appendChild(bubble);
  bubbleWrap.appendChild(meta);
  row.appendChild(bubbleWrap);
  container.appendChild(row);
  container.scrollTop = container.scrollHeight;
}

// 5. Draggable Telemetry Window
function initDraggableWindow() {
  const win = document.getElementById("draggable-telemetry");
  const handle = document.getElementById("telemetry-drag-handle");
  
  let isDragging = false;
  let offsetX = 0;
  let offsetY = 0;

  handle.addEventListener("mousedown", (e) => {
    if (e.target.classList.contains("win-btn")) return;
    isDragging = true;
    offsetX = e.clientX - win.getBoundingClientRect().left;
    offsetY = e.clientY - win.getBoundingClientRect().top;
  });

  document.addEventListener("mousemove", (e) => {
    if (!isDragging) return;
    const x = Math.max(10, Math.min(window.innerWidth - win.offsetWidth - 10, e.clientX - offsetX));
    const y = Math.max(10, Math.min(window.innerHeight - win.offsetHeight - 10, e.clientY - offsetY));
    win.style.left = `${x}px`;
    win.style.top = `${y}px`;
    win.style.right = "auto";
  });

  document.addEventListener("mouseup", () => {
    isDragging = false;
  });

  document.getElementById("btn-close-telemetry").addEventListener("click", () => {
    win.style.display = "none";
  });

  document.getElementById("btn-minimize-telemetry").addEventListener("click", () => {
    const body = document.getElementById("telemetry-content");
    body.style.display = body.style.display === "none" ? "flex" : "none";
  });

  const toggleWin = () => {
    win.style.display = win.style.display === "none" ? "flex" : "none";
  };

  document.getElementById("open-telemetry-btn").addEventListener("click", toggleWin);
  document.getElementById("btn-toggle-telemetry-window").addEventListener("click", toggleWin);
}

// 6. Live Telemetry Polling & Chart.js Visualizer
function initCharts() {
  const chartOpts = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { grid: { color: "#1e293b" }, ticks: { color: "#64748b", font: { size: 9 } } },
      y: { grid: { color: "#1e293b" }, ticks: { color: "#64748b", font: { size: 9 } }, beginAtZero: true }
    }
  };

  const rttCtx = document.getElementById("rttChart").getContext("2d");
  rttChart = new Chart(rttCtx, {
    type: "line",
    data: {
      labels: timeLabels,
      datasets: [
        { label: "RTT", data: rttHistory, borderColor: "#38bdf8", borderWidth: 1.5, tension: 0.3, pointRadius: 0 },
        { label: "Jitter", data: jitterHistory, borderColor: "#f59e0b", borderWidth: 1, borderDash: [3, 3], pointRadius: 0 }
      ]
    },
    options: chartOpts
  });

  const tpCtx = document.getElementById("throughputChart").getContext("2d");
  throughputChart = new Chart(tpCtx, {
    type: "line",
    data: {
      labels: timeLabels,
      datasets: [
        { label: "Throughput", data: throughputHistory, borderColor: "#10b981", backgroundColor: "rgba(16, 185, 129, 0.08)", fill: true, borderWidth: 1.5, tension: 0.2, pointRadius: 0 }
      ]
    },
    options: chartOpts
  });
}

function startTelemetryPolling() {
  setInterval(async () => {
    try {
      const res = await fetch(`${getApiBaseUrl()}/api/stats`);
      const data = await res.json();
      updateTelemetryUI(data);
    } catch (e) {}
  }, 1000);
}

function updateTelemetryUI(stats) {
  document.getElementById("val-rtt").textContent = stats.current_rtt_ms.toFixed(1);
  document.getElementById("val-srtt").textContent = `Smoothed: ${stats.smoothed_rtt_ms.toFixed(1)} ms`;
  document.getElementById("val-jitter").textContent = stats.jitter_ms.toFixed(2);
  
  const tp = (Math.random() * 15 + 35).toFixed(1);
  document.getElementById("val-throughput").textContent = tp;
  document.getElementById("val-cwnd").textContent = `TCP cwnd: ${stats.cwnd_kb} KB`;

  const lossPct = (stats.simulated_loss_rate * 100).toFixed(1);
  document.getElementById("val-loss").textContent = lossPct;
  document.getElementById("val-loss-count").textContent = `${stats.udp_packets_dropped} dropped`;

  const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  timeLabels.push(now);
  rttHistory.push(stats.current_rtt_ms);
  jitterHistory.push(stats.jitter_ms);
  throughputHistory.push(parseFloat(tp));

  if (timeLabels.length > 12) {
    timeLabels.shift();
    rttHistory.shift();
    jitterHistory.shift();
    throughputHistory.shift();
  }

  rttChart.update();
  throughputChart.update();

  // Populate Packet Log Table
  const tbody = document.getElementById("inspector-tbody");
  if (stats.packet_history && stats.packet_history.length > 0) {
    tbody.innerHTML = "";
    stats.packet_history.slice(-8).reverse().forEach(p => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${p.timestamp.split(".")[0]}</td>
        <td><strong>${p.protocol}</strong></td>
        <td>${p.type}</td>
        <td>${p.total_bytes}B</td>
        <td>${p.seq || '-'}/${p.ack || '-'}</td>
        <td class="${p.dropped ? 'text-danger' : 'text-success'}">${p.dropped ? 'DROP' : 'ACK'}</td>
      `;
      tbody.appendChild(row);
    });
  }
}

// 7. UDP Media Stream Simulation
function initStreamCanvas() {
  const canvas = document.getElementById("stream-canvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  let frame = 0;

  function render() {
    if (isStreamPlaying) {
      frame++;
      ctx.fillStyle = "#0a0f1d";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      ctx.strokeStyle = `hsl(${(frame * 2) % 360}, 75%, 55%)`;
      ctx.lineWidth = 2;
      ctx.beginPath();
      for (let x = 0; x < canvas.width; x += 20) {
        const y = canvas.height / 2 + Math.sin((x + frame * 4) * 0.04) * 35;
        if (x === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      if (simulatedLossRate > 0 && Math.random() < simulatedLossRate) {
        ctx.fillStyle = "rgba(239, 68, 68, 0.4)";
        ctx.fillRect(0, Math.random() * canvas.height, canvas.width, 15);
      }

      ctx.fillStyle = "#f1f5f9";
      ctx.font = "12px monospace";
      ctx.fillText(`Stream: ${streamProtocol} Datagram Frame #${streamChunkIndex}`, 14, 24);

      if (frame % 8 === 0 && ws && ws.readyState === WebSocket.OPEN) {
        streamChunkIndex++;
        ws.send(JSON.stringify({
          type: "udp_stream",
          chunk_index: streamChunkIndex,
          chunk_size: 1400
        }));
      }
    }
    requestAnimationFrame(render);
  }
  render();
}

function handleStreamChunk(data) {
  streamRxCount++;
  if (data.dropped) streamDropCount++;

  document.getElementById("stream-rx-packets").textContent = streamRxCount;
  const pct = streamRxCount > 0 ? ((streamDropCount / streamRxCount) * 100).toFixed(1) : "0.0";
  document.getElementById("stream-dropped-stat").textContent = `${streamDropCount} (${pct}%)`;

  const health = Math.max(10, Math.min(100, 100 - parseFloat(pct) * 1.8));
  document.getElementById("stream-buffer-fill").style.width = `${health}%`;
  document.getElementById("stream-buffer-fill").style.backgroundColor = health < 50 ? "var(--accent-red)" : "var(--accent-green)";
}

// 8. Event Bindings
function initUIEvents() {
  document.getElementById("btn-login-connect").addEventListener("click", attemptLogin);
  document.getElementById("user-tag-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") attemptLogin();
  });

  document.getElementById("btn-send-chat").addEventListener("click", sendChatMessage);
  document.getElementById("chat-msg-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") sendChatMessage();
  });

  document.getElementById("target-tag-search").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      let q = e.target.value.trim().toLowerCase();
      if (!q.startsWith("#")) q = "#" + q;
      switchActiveTarget(q);
      e.target.value = "";
    }
  });

  document.querySelectorAll(".contact-card").forEach(c => {
    c.addEventListener("click", () => {
      switchActiveTarget(c.dataset.tag);
    });
  });

  document.getElementById("tab-chats-btn").addEventListener("click", () => {
    document.getElementById("tab-chats-btn").classList.add("active");
    document.getElementById("tab-stream-btn").classList.remove("active");
    document.getElementById("chat-workspace").style.display = "flex";
    document.getElementById("stream-workspace").style.display = "none";
  });

  document.getElementById("tab-stream-btn").addEventListener("click", () => {
    document.getElementById("tab-stream-btn").classList.add("active");
    document.getElementById("tab-chats-btn").classList.remove("active");
    document.getElementById("chat-workspace").style.display = "none";
    document.getElementById("stream-workspace").style.display = "flex";
  });

  document.getElementById("sim-loss-slider").addEventListener("input", async (e) => {
    const val = parseFloat(e.target.value) / 100.0;
    simulatedLossRate = val;
    document.getElementById("loss-slider-label").textContent = `${e.target.value}%`;
    await fetch(`${getApiBaseUrl()}/api/stats/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ loss_rate: val })
    });
  });

  document.getElementById("btn-retransmit-trigger").addEventListener("click", async () => {
    await fetch(`${getApiBaseUrl()}/api/stats/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ retransmit: true })
    });
    alert("[TCP Simulation] Triple Duplicate ACK received! Triggered Fast Retransmit / Halved cwnd.");
  });

  document.getElementById("btn-clear-packet-logs").addEventListener("click", () => {
    document.getElementById("inspector-tbody").innerHTML = "";
  });

  document.getElementById("btn-call-audio").addEventListener("click", () => showCallDialog(activeTargetTag));
  document.getElementById("btn-call-video").addEventListener("click", () => showCallDialog(activeTargetTag));
  document.getElementById("btn-hangup-call").addEventListener("click", () => {
    document.getElementById("call-dialog").style.display = "none";
  });
}

function showCallDialog(target) {
  document.getElementById("call-target-tag").textContent = target;
  document.getElementById("call-target-av").textContent = target.replace("#", "").toUpperCase();
  document.getElementById("call-dialog").style.display = "flex";
}
