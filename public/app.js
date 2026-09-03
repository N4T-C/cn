/* NetChat - Real-Time Protocol & Telemetry Dashboard */

let ws = null;
let myUserTag = "#nat";
let activeTargetTag = "#group";
let attachedImageBase64 = null;
let simulatedLossRate = 0;
let streamProtocol = "UDP";
let streamChunkIndex = 0;
let streamRxCount = 0;
let streamDropCount = 0;
let streamTxCount = 0;
let streamPaused = false;
let messageQueue = [];
let lastStats = null;
let telemetryTimer = null;

const history = {
  labels: [],
  rtt: [],
  jitter: [],
  throughput: [],
  tcp: [],
  udp: [],
  loss: []
};

let rttChart = null;
let throughputChart = null;
let protocolChart = null;
let packetRateChart = null;

function getApiBaseUrl() {
  if (window.location.protocol === "file:" || !window.location.host) return "http://localhost:8000";
  return window.location.origin;
}

function getWsUrl() {
  if (window.location.protocol === "file:" || !window.location.host) return "ws://localhost:8000/ws";
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws`;
}

function $(id) { return document.getElementById(id); }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function fmtBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}
function fmtDuration(seconds) {
  seconds = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return h ? `${h}h ${m}m` : m ? `${m}m ${s}s` : `${s}s`;
}

function showToast(message, type = "info") {
  const host = $("toast-host");
  if (!host) return;
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span class="toast-dot"></span><span>${message}</span>`;
  host.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("show"));
  setTimeout(() => {
    toast.classList.remove("show");
    setTimeout(() => toast.remove(), 250);
  }, 3000);
}

function selectQuickTag(tag) {
  $("user-tag-input").value = tag;
  attemptLogin();
}

function attemptLogin() {
  let tag = $("user-tag-input").value.trim().toLowerCase();
  if (!tag) tag = "#nat";
  if (!tag.startsWith("#")) tag = `#${tag}`;

  myUserTag = tag;
  $("display-user-tag").textContent = myUserTag;
  $("my-active-tag").textContent = myUserTag;
  initWebSocket();
  $("login-modal").style.display = "none";
  $("app-shell").style.display = "flex";
}

function initWebSocket() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    if (ws.readyState === WebSocket.OPEN && myUserTag) {
      ws.send(JSON.stringify({ type: "login", tag: myUserTag }));
    }
    return;
  }

  const wsUrl = getWsUrl();
  try {
    ws = new WebSocket(wsUrl);
  } catch (err) {
    console.error(err);
    setTimeout(initWebSocket, 1500);
    return;
  }

  setConnectionBadge("CONNECTING", "warn");

  ws.onopen = () => {
    setConnectionBadge("ONLINE", "good");
    ws.send(JSON.stringify({ type: "login", tag: myUserTag }));
    while (messageQueue.length) ws.send(JSON.stringify(messageQueue.shift()));
    requestChannelHistory(activeTargetTag);
    showToast("Connected to the NetChat transport node", "success");
  };

  ws.onmessage = event => {
    try { handleServerPacket(JSON.parse(event.data)); }
    catch (e) { console.error("Packet parse error", e); }
  };

  ws.onclose = () => {
    setConnectionBadge("RECONNECTING", "warn");
    setTimeout(initWebSocket, 1500);
  };

  ws.onerror = () => setConnectionBadge("SOCKET ERROR", "bad");
}

function setConnectionBadge(label, tone) {
  const el = $("target-connection-status");
  if (el) el.innerHTML = `<span class="status-led ${tone}"></span> ${label} <span class="mono-sep">•</span> WebSocket / TCP :8000`;
  const nav = $("nav-network-status");
  if (nav) nav.textContent = label;
}

function handleServerPacket(data) {
  if (data.type === "login_success") {
    $("target-connection-status").innerHTML = `<span class="status-led good"></span> ONLINE <span class="mono-sep">•</span> Node ${data.server_ip}:8000`;
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

function updateActiveUserIndicators(activeUsers) {
  $("active-node-count").textContent = activeUsers.length;
  ["#har", "#ava", "#nat"].forEach(tag => {
    const clean = tag.slice(1);
    const dot = $(`status-${clean}`);
    if (dot) dot.classList.toggle("online", activeUsers.includes(tag));
  });
}

function switchActiveTarget(rawTag) {
  let tag = rawTag.trim().toLowerCase();
  if (!tag.startsWith("#")) tag = `#${tag}`;
  activeTargetTag = tag;

  document.querySelectorAll(".contact-card").forEach(c => c.classList.remove("active-card"));
  const activeEl = document.querySelector(`.contact-card[data-tag="${CSS.escape(tag)}"]`);
  if (activeEl) activeEl.classList.add("active-card");

  const isGroup = tag === "#group";
  $("active-target-title").textContent = isGroup ? "#group" : tag;
  $("active-target-subtitle").textContent = isGroup ? "Broadcast channel • all connected nodes" : "Direct TCP channel • reliable delivery";
  $("active-target-avatar").textContent = isGroup ? "GRP" : tag.slice(1).slice(0, 3).toUpperCase();
  requestChannelHistory(tag);
}

function requestChannelHistory(target) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: "get_history", target }));
  }
}

function renderHistoryMessages(messages) {
  const container = $("chat-feed-container");
  container.innerHTML = `<div class="time-stamp-divider"><span></span> CHANNEL INITIALIZED • TCP TRANSPORT LAYER <span></span></div>`;
  messages.forEach(appendMessageElement);
  container.scrollTop = container.scrollHeight;
}

function initImageUpload() {
  const fileInput = $("image-file-input");
  $("btn-trigger-upload").addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", e => {
    const file = e.target.files[0];
    if (!file) return;
    if (file.size > 3 * 1024 * 1024) {
      showToast("Image is too large. Keep attachments below 3 MB.", "error");
      fileInput.value = "";
      return;
    }
    const reader = new FileReader();
    reader.onload = event => {
      attachedImageBase64 = event.target.result;
      $("image-preview-thumb").src = attachedImageBase64;
      $("attachment-preview-bar").style.display = "flex";
      $("attach-file-info").textContent = `${file.name} • ${fmtBytes(file.size)} • ready for TCP transmission`;
    };
    reader.readAsDataURL(file);
  });
  $("btn-cancel-attachment").addEventListener("click", clearAttachment);
}

function clearAttachment() {
  attachedImageBase64 = null;
  $("image-file-input").value = "";
  $("attachment-preview-bar").style.display = "none";
}

function sendChatMessage() {
  const input = $("chat-msg-input");
  const text = input.value.trim();
  if (!text && !attachedImageBase64) return;

  const payload = {
    type: "chat",
    recipient: activeTargetTag,
    content: text,
    image: attachedImageBase64,
    kind: attachedImageBase64 ? "image" : "text",
    client_timestamp: Date.now()
  };

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    messageQueue.push(payload);
    initWebSocket();
    showToast("Socket offline — packet queued", "warning");
  } else {
    ws.send(JSON.stringify(payload));
  }
  input.value = "";
  clearAttachment();
}

function renderIncomingMessage(msg) {
  const isDirectMatch = (msg.recipient === myUserTag && msg.sender === activeTargetTag) ||
    (msg.sender === myUserTag && msg.recipient === activeTargetTag);
  const isGroupMatch = msg.recipient === "#group" && activeTargetTag === "#group";
  if (isDirectMatch || isGroupMatch || msg.recipient === "all") appendMessageElement(msg);

  const snippetTarget = msg.recipient === "#group" ? "group" :
    (msg.sender === myUserTag ? msg.recipient.replace("#", "") : msg.sender.replace("#", ""));
  const lastMsgEl = $(`last-msg-${snippetTarget}`);
  if (lastMsgEl) {
    lastMsgEl.textContent = msg.kind === "image" ? `${msg.sender}: [Image Attachment]` : `${msg.sender}: ${msg.content}`;
    lastMsgEl.classList.add("recent-message");
  }
}

function appendMessageElement(msg) {
  const container = $("chat-feed-container");
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
    img.alt = "Transmitted image packet";
    bubble.appendChild(img);
    if (msg.content) {
      const caption = document.createElement("p");
      caption.textContent = msg.content;
      caption.className = "image-caption";
      bubble.appendChild(caption);
    }
  } else {
    bubble.textContent = msg.content || "[empty payload]";
  }

  const meta = document.createElement("div");
  meta.className = "packet-meta";
  meta.innerHTML = `<span class="meta-pill seq">SEQ ${msg.seq || 0}</span><span class="meta-pill ack">ACK ${msg.ack || 0}</span><span class="meta-pill rtt">RTT ${Number(msg.rtt_ms || 0).toFixed(1)}ms</span><span>${msg.timestamp || "Now"}</span>`;
  bubbleWrap.appendChild(bubble);
  bubbleWrap.appendChild(meta);
  row.appendChild(bubbleWrap);
  container.appendChild(row);
  container.scrollTop = container.scrollHeight;
}

function initDraggableWindow() {
  const win = $("draggable-telemetry");
  const handle = $("telemetry-drag-handle");
  let dragging = false, offsetX = 0, offsetY = 0;

  handle.addEventListener("mousedown", e => {
    if (e.target.closest("button")) return;
    dragging = true;
    const r = win.getBoundingClientRect();
    offsetX = e.clientX - r.left;
    offsetY = e.clientY - r.top;
  });
  document.addEventListener("mousemove", e => {
    if (!dragging) return;
    const x = clamp(e.clientX - offsetX, 8, window.innerWidth - win.offsetWidth - 8);
    const y = clamp(e.clientY - offsetY, 8, window.innerHeight - win.offsetHeight - 8);
    win.style.left = `${x}px`;
    win.style.top = `${y}px`;
    win.style.right = "auto";
  });
  document.addEventListener("mouseup", () => dragging = false);

  $("btn-close-telemetry").addEventListener("click", () => win.style.display = "none");
  $("btn-minimize-telemetry").addEventListener("click", () => {
    const body = $("telemetry-content");
    body.classList.toggle("collapsed");
  });
  const toggle = () => win.style.display = win.style.display === "none" ? "flex" : "none";
  $("btn-toggle-telemetry-window").addEventListener("click", toggle);
}

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 350 },
  interaction: { mode: "index", intersect: false },
  plugins: { legend: { display: false }, tooltip: { displayColors: false } },
  scales: {
    x: { grid: { color: "rgba(148,163,184,.07)" }, ticks: { color: "#64748b", font: { size: 9 } } },
    y: { beginAtZero: true, grid: { color: "rgba(148,163,184,.07)" }, ticks: { color: "#64748b", font: { size: 9 } } }
  }
};

function initCharts() {
  if (typeof Chart === "undefined") {
    showToast("Chart.js could not load — analytics cards still work", "warning");
    return;
  }

  rttChart = new Chart($("rttChart"), {
    type: "line",
    data: { labels: history.labels, datasets: [
      { label: "RTT", data: history.rtt, borderColor: "#38bdf8", borderWidth: 2, tension: .35, pointRadius: 0 },
      { label: "Jitter", data: history.jitter, borderColor: "#f59e0b", borderWidth: 1.5, borderDash: [4, 4], tension: .35, pointRadius: 0 }
    ]},
    options: chartOptions
  });

  throughputChart = new Chart($("throughputChart"), {
    type: "line",
    data: { labels: history.labels, datasets: [
      { label: "Throughput", data: history.throughput, borderColor: "#10b981", backgroundColor: "rgba(16,185,129,.10)", fill: true, borderWidth: 2, tension: .3, pointRadius: 0 }
    ]},
    options: chartOptions
  });

  protocolChart = new Chart($("protocolChart"), {
    type: "doughnut",
    data: { labels: ["TCP", "UDP"], datasets: [{ data: [0, 0], backgroundColor: ["#38bdf8", "#a78bfa"], borderWidth: 0 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: "72%", plugins: { legend: { display: false } } }
  });

  packetRateChart = new Chart($("packetRateChart"), {
    type: "bar",
    data: { labels: history.labels, datasets: [
      { label: "TCP", data: history.tcp, backgroundColor: "rgba(56,189,248,.65)", borderRadius: 4 },
      { label: "UDP", data: history.udp, backgroundColor: "rgba(167,139,250,.65)", borderRadius: 4 }
    ]},
    options: { ...chartOptions, plugins: { legend: { display: false } } }
  });
}

function startTelemetryPolling() {
  if (telemetryTimer) clearInterval(telemetryTimer);
  pollTelemetry();
  telemetryTimer = setInterval(pollTelemetry, 1000);
}

async function pollTelemetry() {
  try {
    const res = await fetch(`${getApiBaseUrl()}/api/stats`, { cache: "no-store" });
    if (!res.ok) throw new Error("stats unavailable");
    updateTelemetryUI(await res.json());
  } catch (e) {
    const dot = $("telemetry-live-dot");
    if (dot) dot.classList.add("offline");
  }
}

function computeHealth(stats) {
  const rttPenalty = clamp((stats.current_rtt_ms - 10) * 1.7, 0, 25);
  const jitterPenalty = clamp((stats.jitter_ms - 3) * 2.5, 0, 20);
  const lossPenalty = clamp(stats.observed_udp_loss_rate * 1.5, 0, 40);
  const tpBonus = clamp((stats.throughput_kbps / 100) * 10, 0, 10);
  return Math.round(clamp(100 - rttPenalty - jitterPenalty - lossPenalty + tpBonus, 0, 100));
}

function healthLabel(score) {
  if (score >= 85) return ["EXCELLENT", "good"];
  if (score >= 70) return ["GOOD", "good"];
  if (score >= 50) return ["DEGRADED", "warn"];
  return ["POOR", "bad"];
}

function metricLabel(value, type) {
  if (type === "rtt") return value < 20 ? ["Low", "good"] : value < 80 ? ["Moderate", "warn"] : ["High", "bad"];
  if (type === "jitter") return value < 5 ? ["Stable", "good"] : value < 20 ? ["Variable", "warn"] : ["Unstable", "bad"];
  if (type === "loss") return value < 2 ? ["Minimal", "good"] : value < 10 ? ["Moderate", "warn"] : ["High", "bad"];
  return value > 100 ? ["Strong", "good"] : value > 25 ? ["Normal", "warn"] : ["Low", "bad"];
}

function updateMetricBadge(id, value, type) {
  const el = $(id);
  if (!el) return;
  const [text, tone] = metricLabel(value, type);
  el.textContent = text;
  el.className = `metric-badge ${tone}`;
}

function updateTelemetryUI(stats) {
  lastStats = stats;
  $("telemetry-live-dot")?.classList.remove("offline");

  const score = computeHealth(stats);
  const [label, tone] = healthLabel(score);
  $("health-score").textContent = score;
  $("health-label").textContent = label;
  $("health-ring").style.setProperty("--health", `${score * 3.6}deg`);
  $("health-ring").className = `health-ring ${tone}`;
  $("health-summary").textContent = buildHealthSummary(stats, score);
  $("hud-health").textContent = score;
  $("hud-condition").textContent = label;
  $("hud-rtt").textContent = Number(stats.current_rtt_ms).toFixed(1);
  $("hud-jitter").textContent = Number(stats.jitter_ms).toFixed(2);
  $("hud-throughput").textContent = Number(stats.throughput_kbps).toFixed(1);
  $("hud-loss").textContent = Number(stats.observed_udp_loss_rate).toFixed(1);

  $("val-rtt").textContent = Number(stats.current_rtt_ms).toFixed(1);
  $("val-srtt").textContent = `SRTT ${Number(stats.smoothed_rtt_ms).toFixed(1)} ms • RFC 6298`;
  $("val-jitter").textContent = Number(stats.jitter_ms).toFixed(2);
  $("val-throughput").textContent = Number(stats.throughput_kbps).toFixed(1);
  $("val-cwnd").textContent = `TCP cwnd ${Number(stats.cwnd_kb).toFixed(1)} KB`;
  $("val-loss").textContent = Number(stats.observed_udp_loss_rate).toFixed(1);
  $("val-loss-count").textContent = `${stats.udp_packets_dropped} / ${stats.udp_packets_sent} UDP datagrams`;
  $("val-tcp-packets").textContent = stats.total_tcp_packets;
  $("val-udp-packets").textContent = stats.total_udp_packets;
  $("val-tcp-bytes").textContent = fmtBytes(stats.tcp_bytes);
  $("val-udp-bytes").textContent = fmtBytes(stats.udp_bytes);
  $("val-retransmissions").textContent = stats.tcp_retransmissions;
  $("val-active-connections").textContent = stats.active_connections;
  $("server-uptime").textContent = fmtDuration(stats.uptime_seconds);

  updateMetricBadge("rtt-badge", stats.current_rtt_ms, "rtt");
  updateMetricBadge("jitter-badge", stats.jitter_ms, "jitter");
  updateMetricBadge("loss-badge", stats.observed_udp_loss_rate, "loss");
  updateMetricBadge("throughput-badge", stats.throughput_kbps, "throughput");

  updateHistory(stats);
  updateCharts(stats);
  renderPacketInspector(stats.packet_history || []);
  updateProtocolBreakdown(stats);
  updateInsights(stats, score);
  updateStreamOverview(stats);
}

function buildHealthSummary(stats, score) {
  if (stats.observed_udp_loss_rate >= 10) return "Packet loss is currently the dominant risk. Streaming quality may degrade.";
  if (stats.current_rtt_ms >= 80) return "High round-trip latency can make interactive communication feel slow.";
  if (stats.jitter_ms >= 20) return "High jitter indicates inconsistent packet arrival timing.";
  if (score >= 85) return "The transport path looks stable for interactive chat and real-time media.";
  return "The network is usable, but one or more QoS metrics deserve attention.";
}

function updateHistory(stats) {
  const now = new Date().toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
  history.labels.push(now);
  history.rtt.push(Number(stats.current_rtt_ms));
  history.jitter.push(Number(stats.jitter_ms));
  history.throughput.push(Number(stats.throughput_kbps));
  history.tcp.push(Number(stats.total_tcp_packets));
  history.udp.push(Number(stats.total_udp_packets));
  history.loss.push(Number(stats.observed_udp_loss_rate));

  const max = 30;
  Object.values(history).forEach(arr => { while (arr.length > max) arr.shift(); });
}

function updateCharts(stats) {
  if (!rttChart) return;
  rttChart.update();
  throughputChart.update();
  protocolChart.data.datasets[0].data = [stats.total_tcp_packets, stats.total_udp_packets];
  protocolChart.update();
  packetRateChart.update();
}

function updateProtocolBreakdown(stats) {
  const total = stats.total_tcp_packets + stats.total_udp_packets;
  const tcpPct = total ? stats.total_tcp_packets / total * 100 : 0;
  const udpPct = total ? stats.total_udp_packets / total * 100 : 0;
  $("tcp-share-center").textContent = `${tcpPct.toFixed(0)}%`;
  $("tcp-share").textContent = `${tcpPct.toFixed(0)}%`;
  $("udp-share").textContent = `${udpPct.toFixed(0)}%`;
  $("tcp-share-bar").style.width = `${tcpPct}%`;
  $("udp-share-bar").style.width = `${udpPct}%`;
}

function updateInsights(stats, score) {
  const list = $("insight-list");
  const insights = [];
  if (stats.current_rtt_ms < 20) insights.push("RTT is low enough for responsive interactive chat.");
  else insights.push("RTT is elevated; interactive actions may feel slower.");
  if (stats.jitter_ms < 5) insights.push("Jitter is stable, which is favorable for real-time media.");
  else insights.push("Jitter is varying; media playback may need more buffering.");
  if (stats.observed_udp_loss_rate === 0 && stats.udp_packets_sent > 0) insights.push("No observed UDP loss in the current sample window.");
  else if (stats.udp_packets_sent > 0) insights.push(`${stats.observed_udp_loss_rate.toFixed(1)}% of UDP datagrams were dropped.`);
  if (stats.tcp_retransmissions > 0) insights.push(`TCP fast retransmit simulated ${stats.tcp_retransmissions} time(s); cwnd is ${stats.cwnd_kb} KB.`);
  if (!insights.length) insights.push("Generate traffic to populate network insights.");
  list.innerHTML = insights.slice(0, 4).map(text => `<li><span class="insight-icon">›</span>${text}</li>`).join("");
  $("insight-score").textContent = `${score}/100 health score`;
}

function renderPacketInspector(packets) {
  const tbody = $("inspector-tbody");
  const filter = ($("packet-search")?.value || "").toLowerCase();
  const proto = $("packet-protocol-filter")?.value || "ALL";
  const rows = packets.filter(p => {
    const matchesProto = proto === "ALL" || p.protocol === proto;
    const hay = `${p.timestamp} ${p.protocol} ${p.type} ${p.src} ${p.dst}`.toLowerCase();
    return matchesProto && hay.includes(filter);
  }).slice(-12).reverse();

  tbody.innerHTML = rows.length ? rows.map(p => `
    <tr>
      <td class="mono">${p.timestamp.split(".")[0]}</td>
      <td><span class="proto-tag ${p.protocol.toLowerCase()}">${p.protocol}</span></td>
      <td>${p.type}</td>
      <td>${p.total_bytes} B</td>
      <td class="mono">${p.seq || "—"}/${p.ack || "—"}</td>
      <td>${p.rtt_ms.toFixed(1)} ms</td>
      <td class="${p.dropped ? "text-danger" : "text-success"}">${p.dropped ? "DROP" : "OK"}</td>
    </tr>`).join("") : `<tr><td colspan="7" class="empty-table">No packets match the current filter.</td></tr>`;
}

function updateStreamOverview(stats) {
  const loss = Number(stats.observed_udp_loss_rate || 0);
  const health = clamp(100 - loss * 1.8 - Number(stats.jitter_ms || 0) * .7, 8, 100);
  $("stream-health-number").textContent = `${health.toFixed(0)}%`;
  $("stream-buffer-fill").style.width = `${health}%`;
  $("stream-buffer-fill").className = `stream-progress-fill ${health < 50 ? "bad" : health < 75 ? "warn" : "good"}`;
  $("stream-bitrate-stat").textContent = `${Math.max(0, Number(stats.throughput_kbps) * 8).toFixed(0)} kbps`;
}

function initStreamCanvas() {
  const canvas = $("stream-canvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  let frame = 0;

  function render() {
    frame++;
    ctx.fillStyle = "#080d16";
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    const loss = simulatedLossRate;
    const gradient = ctx.createLinearGradient(0, 0, canvas.width, canvas.height);
    gradient.addColorStop(0, "#10263a");
    gradient.addColorStop(1, "#16152d");
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    ctx.strokeStyle = streamProtocol === "UDP" ? "#a78bfa" : "#38bdf8";
    ctx.lineWidth = 3;
    ctx.beginPath();
    for (let x = 0; x < canvas.width; x += 4) {
      const y = canvas.height / 2 + Math.sin((x + frame * 4) * .035) * (28 + loss * 40);
      if (x === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();

    if (streamProtocol === "UDP" && loss > 0 && Math.random() < loss) {
      ctx.fillStyle = "rgba(239,68,68,.42)";
      ctx.fillRect(0, Math.random() * canvas.height, canvas.width, 18);
    }

    ctx.fillStyle = "#e2e8f0";
    ctx.font = "600 12px ui-monospace, monospace";
    ctx.fillText(`${streamProtocol} / RTP  •  frame #${streamChunkIndex}`, 16, 24);
    ctx.fillStyle = "#64748b";
    ctx.font = "11px ui-monospace, monospace";
    ctx.fillText(streamProtocol === "UDP" ? "Connectionless • low latency • loss tolerant" : "Reliable delivery • retransmission • buffering", 16, canvas.height - 18);

    if (!streamPaused && frame % 8 === 0 && ws && ws.readyState === WebSocket.OPEN) {
      streamChunkIndex++;
      streamTxCount++;
      ws.send(JSON.stringify({ type: "udp_stream", chunk_index: streamChunkIndex, chunk_size: 1400 }));
    }
    requestAnimationFrame(render);
  }
  render();
}

function handleStreamChunk(data) {
  streamRxCount++;
  if (data.dropped) streamDropCount++;
  $("stream-rx-packets").textContent = streamRxCount;
  $("stream-tx-packets").textContent = streamTxCount;
  const pct = streamRxCount ? streamDropCount / streamRxCount * 100 : 0;
  $("stream-dropped-stat").textContent = `${streamDropCount} (${pct.toFixed(1)}%)`;
}

function setStreamMode(mode) {
  streamProtocol = mode;
  document.querySelectorAll(".btn-stream-mode").forEach(b => b.classList.remove("active"));
  $(`btn-mode-${mode.toLowerCase()}`).classList.add("active");
  $("stream-protocol-tag").textContent = mode === "UDP" ? "UDP / RTP" : "TCP / BUFFERED";
  $("stream-mode-explanation").textContent = mode === "UDP" ?
    "Packets are delivered immediately; loss can create visible artifacts without head-of-line blocking." :
    "Reliable delivery is prioritized; retransmissions can add delay when loss occurs.";
  showToast(`${mode} stream mode selected`, "info");
}

function initUIEvents() {
  $("btn-login-connect").addEventListener("click", attemptLogin);
  $("user-tag-input").addEventListener("keydown", e => { if (e.key === "Enter") attemptLogin(); });
  $("btn-send-chat").addEventListener("click", sendChatMessage);
  $("chat-msg-input").addEventListener("keydown", e => { if (e.key === "Enter") sendChatMessage(); });

  $("target-tag-search").addEventListener("keydown", e => {
    if (e.key === "Enter") {
      const q = e.target.value.trim();
      if (q) switchActiveTarget(q);
      e.target.value = "";
    }
  });
  document.querySelectorAll(".contact-card").forEach(c => c.addEventListener("click", () => switchActiveTarget(c.dataset.tag)));

  $("tab-chats-btn").addEventListener("click", () => switchWorkspace("chat"));
  $("tab-stream-btn").addEventListener("click", () => switchWorkspace("stream"));
  $("tab-analytics-btn").addEventListener("click", () => switchWorkspace("analytics"));

  $("sim-loss-slider").addEventListener("input", e => setLossRate(Number(e.target.value)));
  document.querySelectorAll("[data-loss-preset]").forEach(btn => btn.addEventListener("click", () => setLossRate(Number(btn.dataset.lossPreset))));

  $("btn-retransmit-trigger").addEventListener("click", async () => {
    try {
      await fetch(`${getApiBaseUrl()}/api/stats/config`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ retransmit: true }) });
      showToast("TCP fast retransmit simulated — congestion window reduced", "warning");
    } catch { showToast("Could not reach telemetry API", "error"); }
  });

  $("btn-clear-packet-logs").addEventListener("click", resetTelemetry);
  $("packet-search").addEventListener("input", () => renderPacketInspector(lastStats?.packet_history || []));
  $("packet-protocol-filter").addEventListener("change", () => renderPacketInspector(lastStats?.packet_history || []));
  $("btn-export-json").addEventListener("click", exportTelemetryJSON);
  $("btn-export-csv").addEventListener("click", exportTelemetryCSV);
  $("btn-refresh-health").addEventListener("click", pollTelemetry);

  $("btn-mode-udp").addEventListener("click", () => setStreamMode("UDP"));
  $("btn-mode-tcp").addEventListener("click", () => setStreamMode("TCP"));
  $("btn-stream-toggle").addEventListener("click", () => {
    streamPaused = !streamPaused;
    $("btn-stream-toggle").textContent = streamPaused ? "Resume" : "Pause";
    $("stream-live-state").textContent = streamPaused ? "PAUSED" : "LIVE";
  });

  $("btn-call-audio").addEventListener("click", () => showCallDialog(activeTargetTag, "Audio"));
  $("btn-call-video").addEventListener("click", () => showCallDialog(activeTargetTag, "Video"));
  $("btn-hangup-call").addEventListener("click", () => $("call-dialog").style.display = "none");
}

function switchWorkspace(name) {
  ["chat", "stream", "analytics"].forEach(view => {
    const el = $(`${view}-workspace`);
    if (el) el.style.display = view === name ? "flex" : "none";
    const btn = $(`tab-${view === "chat" ? "chats" : view}-btn`);
    if (btn) btn.classList.toggle("active", view === name);
  });
}

async function setLossRate(percent) {
  simulatedLossRate = percent / 100;
  $("sim-loss-slider").value = percent;
  $("loss-slider-label").textContent = `${percent}%`;
  $("loss-impact-label").textContent = percent === 0 ? "No injected loss" : `${percent}% datagram drop probability`;
  try {
    await fetch(`${getApiBaseUrl()}/api/stats/config`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ loss_rate: simulatedLossRate }) });
    showToast(`UDP loss injection set to ${percent}%`, percent >= 25 ? "warning" : "info");
  } catch { showToast("Unable to update loss configuration", "error"); }
}

async function resetTelemetry() {
  try {
    await fetch(`${getApiBaseUrl()}/api/stats/reset`, { method: "POST" });
    history.labels.length = history.rtt.length = history.jitter.length = history.throughput.length = history.tcp.length = history.udp.length = history.loss.length = 0;
    streamRxCount = streamDropCount = streamTxCount = 0;
    $("stream-rx-packets").textContent = "0";
    $("stream-tx-packets").textContent = "0";
    $("stream-dropped-stat").textContent = "0 (0.0%)";
    showToast("Telemetry counters and packet history reset", "success");
    pollTelemetry();
  } catch { showToast("Could not reset telemetry", "error"); }
}

function downloadBlob(filename, content, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function exportTelemetryJSON() {
  if (!lastStats) return showToast("No telemetry data to export yet", "warning");
  downloadBlob("netchat-telemetry.json", JSON.stringify(lastStats, null, 2), "application/json");
  showToast("Telemetry JSON exported", "success");
}

function exportTelemetryCSV() {
  const packets = lastStats?.packet_history || [];
  if (!packets.length) return showToast("Generate some packets before exporting CSV", "warning");
  const headers = ["timestamp", "protocol", "type", "source", "destination", "payload_bytes", "header_bytes", "total_bytes", "seq", "ack", "rtt_ms", "jitter_ms", "dropped"];
  const lines = [headers.join(",")];
  packets.forEach(p => lines.push(headers.map(h => JSON.stringify(p[h] ?? "")).join(",")));
  downloadBlob("netchat-packet-log.csv", lines.join("\n"), "text/csv;charset=utf-8");
  showToast("Packet log CSV exported", "success");
}

function showCallDialog(target, mode = "Audio") {
  if (!target || target === "#group") {
    showToast("Select a direct node before starting a call", "warning");
    return;
  }
  $("call-target-tag").textContent = target;
  $("call-target-av").textContent = target.slice(1).toUpperCase();
  $("call-dialog-status").textContent = `${mode} call • UDP / RTP signaling`;
  $("call-dialog").style.display = "flex";
}

// Startup
window.addEventListener("DOMContentLoaded", () => {
  initUIEvents();
  initImageUpload();
  initDraggableWindow();
  initCharts();
  initStreamCanvas();
  startTelemetryPolling();

  const params = new URLSearchParams(window.location.search);
  const tagParam = params.get("tag");
  if (tagParam) selectQuickTag(tagParam);
});
