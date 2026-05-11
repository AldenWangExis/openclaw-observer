/**
 * OpenClaw Observer — vanilla JS UI.
 * No framework, no bundler. Runs as an ES module from <script type="module">.
 *
 * Layers:
 *   1) WebSocket client: backlog + live events + stats
 *   2) In-memory state: sessions map, events ring, selection
 *   3) DOM renderers: left sessions, center events, right detail, token panel
 *   4) Keyboard shortcuts + toolbar filters
 */

const MAX_EVENTS = 2000;
const ICONS = {
  message_received: "📩",
  message_sent:     "📤",
  llm_input:        "🎯",
  llm_output:       "📝",
  before_tool_call: "🔧",
  after_tool_call:  "✅",
  session_start:    "🌱",
  session_end:      "🏁",
  subagent_spawning:"🌿",
  subagent_spawned: "🌿",
  subagent_ended:   "🪦",
  before_compaction:"🔄",
  after_compaction: "🔄",
  default_hook:     "🪝",
  default_diag:     "ℹ️",
};

/**
 * Human-readable explanation for every event type we render.
 * Keys are event.type. Includes both hook events and diagnostic events.
 */
const EVENT_HINTS = {
  // ─── HOOK events (agent lifecycle) ───
  session_start:          "新会话开始",
  session_end:            "会话结束",
  message_received:       "收到用户消息",
  message_sent:           "向用户发送回复",
  llm_input:              "发送给模型的完整上下文",
  llm_output:             "模型返回结果 + token 用量",
  before_tool_call:       "工具调用开始",
  after_tool_call:        "工具调用结束",
  before_compaction:      "上下文压缩前（触发历史裁剪）",
  after_compaction:       "上下文压缩完成",
  subagent_spawning:      "正在派生子 agent",
  subagent_spawned:       "子 agent 已启动",
  subagent_ended:         "子 agent 运行结束",

  // ─── DIAGNOSTIC events ───
  "session.state":                  "会话状态机变化（idle/processing/…）",
  "session.stuck":                  "⚠ 会话卡住（长时间未推进）",
  "message.queued":                 "消息进入处理队列",
  "message.processed":              "消息处理完成（整轮耗时）",
  "diagnostic.heartbeat":           "心跳（gateway 存活信号）",
  "diagnostic.memory.sample":       "内存采样（Node 堆/RSS）",
  "diagnostic.liveness.warning":    "⚠ 存活性告警（事件循环延迟等）",
  "queue.lane.enqueue":             "任务入队列",
  "queue.lane.dequeue":             "任务出队列（开始处理）",
  "exec.process.started":           "exec 进程启动",
  "exec.process.completed":         "exec 进程结束",
  "exec.process.killed":            "exec 进程被终止",
  "tool.call.started":              "工具调用启动（诊断侧）",
  "tool.call.completed":            "工具调用完成（诊断侧）",
  "plugin.loaded":                  "插件加载完成",
  "plugin.failed":                  "⚠ 插件加载失败",
  "gateway.started":                "Gateway 启动",
  "gateway.stopping":               "Gateway 停止中",
};

/** Fallback explainer when type is not in the lookup table above. */
function explainEvent(evt) {
  // ── Dynamic per-event enrichment: pull useful fields from payload ──
  const p = evt.payload || {};

  // session.state: show state transition + reason
  if (evt.type === "session.state") {
    const prev = p.prevState || "?";
    const next = p.state || "?";
    const reason = p.reason ? ` · ${p.reason}` : "";
    const qd = typeof p.queueDepth === "number" && p.queueDepth > 0 ? ` · queue=${p.queueDepth}` : "";
    return `会话状态: ${prev} → ${next}${reason}${qd}`;
  }

  // session.stuck: show how long stuck
  if (evt.type === "session.stuck") {
    const ms = p.stuckForMs ?? p.durationMs;
    return ms ? `⚠ 会话卡住 ${Math.round(ms / 1000)}s 未推进` : "⚠ 会话卡住（长时间未推进）";
  }

  // message.queued / message.processed
  if (evt.type === "message.queued") {
    const qd = typeof p.queueDepth === "number" ? ` · queue=${p.queueDepth}` : "";
    return `用户消息入队${qd}`;
  }
  if (evt.type === "message.processed") {
    const dur = p.durationMs ?? evt.durationMs;
    return dur ? `消息处理完成 · 整轮耗时 ${Math.round(dur / 1000)}s` : "消息处理完成";
  }

  // heartbeat / memory — show live value
  if (evt.type === "diagnostic.memory.sample") {
    const rss = p.rssMb ?? p.rss;
    const heap = p.heapUsedMb ?? p.heapUsed;
    if (rss != null && heap != null) return `内存采样 · RSS ${fmtMb(rss)} · Heap ${fmtMb(heap)}`;
    return "内存采样（Node 堆/RSS）";
  }
  if (evt.type === "diagnostic.heartbeat") {
    const lag = p.eventLoopLagMs ?? p.lagMs;
    return lag != null ? `心跳 · eventLoop 延迟 ${Math.round(lag)}ms` : "心跳（gateway 存活信号）";
  }
  if (evt.type === "diagnostic.liveness.warning") {
    const lag = p.eventLoopLagMs ?? p.lagMs;
    return lag != null ? `⚠ 存活性告警 · 事件循环延迟 ${Math.round(lag)}ms` : "⚠ 存活性告警";
  }

  // queue lanes
  if (evt.type === "queue.lane.enqueue" || evt.type === "queue.lane.dequeue") {
    const lane = p.lane ? ` · lane=${p.lane}` : "";
    const qd = typeof p.queueDepth === "number" ? ` · queue=${p.queueDepth}` : "";
    return evt.type.endsWith("enqueue") ? `任务入队${lane}${qd}` : `任务出队${lane}${qd}`;
  }

  // exec lifecycle — show command preview + exit code
  if (evt.type === "exec.process.started") {
    const cmd = p.command || p.cmd;
    return cmd ? `exec 启动 · ${shortKey(cmd, 50)}` : "exec 进程启动";
  }
  if (evt.type === "exec.process.completed") {
    const code = p.exitCode ?? p.code;
    const dur = p.durationMs;
    const parts = [];
    if (code != null) parts.push(`exit=${code}`);
    if (dur != null) parts.push(`${dur}ms`);
    return parts.length ? `exec 结束 · ${parts.join(" · ")}` : "exec 进程结束";
  }
  if (evt.type === "exec.process.killed") {
    return `exec 被终止${p.signal ? " · " + p.signal : ""}`;
  }

  // ── hook events ──
  if (evt.type === "before_tool_call") {
    return evt.toolName ? `工具调用开始 · ${evt.toolName}` : "工具调用开始";
  }
  if (evt.type === "after_tool_call") {
    const dur = evt.durationMs != null ? ` · ${evt.durationMs}ms` : "";
    const tool = evt.toolName ? ` · ${evt.toolName}` : "";
    const status = evt.toolStatus === "error" ? " · ❌ error" : "";
    return `工具调用结束${tool}${dur}${status}`;
  }
  if (evt.type === "llm_input") {
    return evt.model ? `发送给模型 · ${shortKey(evt.model, 24)}` : "发送给模型（完整上下文）";
  }
  if (evt.type === "llm_output") {
    const tk = evt.tokens || {};
    const inOut = tk.input != null || tk.output != null
      ? ` · in ${fmtNum(tk.input ?? 0)} / out ${fmtNum(tk.output ?? 0)}`
      : "";
    return `模型返回${evt.model ? " · " + shortKey(evt.model, 24) : ""}${inOut}`;
  }
  if (evt.type === "message_received") {
    return `收到用户消息${evt.channel ? " · " + evt.channel : ""}`;
  }
  if (evt.type === "message_sent") {
    return `向用户发送回复${evt.channel ? " · " + evt.channel : ""}`;
  }

  // Static lookup fallback
  const hit = EVENT_HINTS[evt.type];
  if (hit) return hit;

  // Last-resort heuristic
  if (evt.category === "diag") return evt.type.replace(/[._]/g, " ");
  return evt.type;
}

function fmtMb(n) {
  if (n == null) return "?";
  // n may already be in MB or in bytes. Assume MB if it's below 10000, else bytes.
  if (n < 10000) return `${Math.round(n)}MB`;
  return `${Math.round(n / 1024 / 1024)}MB`;
}

const state = {
  events: [],            // newest-last array (chronological); capped to MAX_EVENTS
  sessions: new Map(),   // sessionKey → summary
  tokens: null,          // latest /api/tokens snapshot
  selectedEventId: null,
  selectedSessionKey: null,
  filters: {
    category: null,      // "hook" | "diag" | null
    search: "",
    onlyLlm: false,
    onlyTools: false,
  },
  paused: false,
  connected: false,
  stats: null,
  // cursor for j/k nav — index into the *visible* list
  cursor: -1,

  // UI state for the detail pane
  detailFullscreen: false,
  detailWrap: true,
  currentDetailJson: "",
};

// ────────────────────────────────────────────────────────────────────
// DOM cache

const $ = (id) => document.getElementById(id);
const els = {
  sessionsList: $("sessions-list"),
  sessionsCount: $("sessions-count"),
  eventsList: $("events-list"),
  eventsCount: $("events-count"),
  detail: $("detail"),
  tokenPanel: $("token-panel"),
  wsDot: $("ws-dot"),
  wsStatus: $("ws-status"),
  searchInput: $("search"),
  pauseBtn: $("pause-btn"),
  clearBtn: $("clear-btn"),
  catHookBtn: $("cat-hook"),
  catDiagBtn: $("cat-diag"),
  llmBtn: $("only-llm"),
  toolBtn: $("only-tool"),
  dbRows: $("st-db-rows"),
  totalSeq: $("st-total-seq"),
  bus: $("st-bus"),
  flushed: $("st-flushed"),
  dropped: $("st-dropped"),
  helpBox: $("help"),
  rightCol: $("right-col"),
  detailCopy: $("detail-copy"),
  detailWrap: $("detail-wrap"),
  detailFullscreen: $("detail-fullscreen"),
};

// ────────────────────────────────────────────────────────────────────
// WebSocket

let ws;
function connect() {
  const url = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws";
  ws = new WebSocket(url);
  ws.onopen = () => {
    state.connected = true;
    paintConnection();
  };
  ws.onclose = () => {
    state.connected = false;
    paintConnection();
    setTimeout(connect, 1500);
  };
  ws.onerror = () => {
    state.connected = false;
    paintConnection();
  };
  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      if (msg.type === "backlog" && Array.isArray(msg.events)) {
        msg.events.forEach(ingestEvent);
        renderEvents();
        renderSessions();
      } else if (msg.type === "event") {
        ingestEvent(msg.event);
        if (!state.paused) {
          renderEvents();
          renderSessions();
          renderDetailIfSelectionStale();
        }
      } else if (msg.type === "stats") {
        state.stats = msg.stats;
        paintStatusBar();
      }
    } catch (e) {
      console.warn("[observer] bad ws msg", e);
    }
  };
}

function paintConnection() {
  if (state.connected) {
    els.wsDot.className = "dot ok";
    els.wsStatus.textContent = "live";
  } else {
    els.wsDot.className = "dot err";
    els.wsStatus.textContent = "reconnecting…";
  }
}

// ────────────────────────────────────────────────────────────────────
// Ingest & derived state

function ingestEvent(evt) {
  // Keep chronological order (bus sends backlog newest-last, event live).
  state.events.push(evt);
  if (state.events.length > MAX_EVENTS) {
    state.events.splice(0, state.events.length - MAX_EVENTS);
  }
  // Derive a tiny session summary — full one comes from /api/sessions poll.
  if (evt.sessionKey) {
    let s = state.sessions.get(evt.sessionKey);
    if (!s) {
      s = {
        sessionKey: evt.sessionKey,
        parentSessionKey: evt.parentSessionKey,
        agentId: evt.agentId,
        channel: evt.channel,
        firstSeen: evt.ts,
        lastSeen: evt.ts,
        status: "active",
        eventCount: 0,
      };
      state.sessions.set(evt.sessionKey, s);
    }
    s.lastSeen = evt.ts;
    s.eventCount += 1;
    if (evt.parentSessionKey && !s.parentSessionKey) s.parentSessionKey = evt.parentSessionKey;
    if (evt.agentId && !s.agentId) s.agentId = evt.agentId;
    // status derivation mirrors SessionTracker (rough)
    const m = {
      session_start: "active", session_end: "done",
      message_received: "thinking", llm_input: "thinking",
      llm_output: "active", before_tool_call: "tool",
      after_tool_call: "active", message_sent: "idle",
    };
    if (m[evt.type]) s.status = m[evt.type];
  }
}

// ────────────────────────────────────────────────────────────────────
// Filters

function visibleEvents() {
  const f = state.filters;
  const q = f.search.trim().toLowerCase();
  let list = state.events;
  if (state.selectedSessionKey) {
    list = list.filter((e) => e.sessionKey === state.selectedSessionKey);
  }
  if (f.category) list = list.filter((e) => e.category === f.category);
  if (f.onlyLlm) list = list.filter((e) => e.type === "llm_input" || e.type === "llm_output");
  if (f.onlyTools) list = list.filter((e) => e.type === "before_tool_call" || e.type === "after_tool_call");
  if (q) {
    list = list.filter((e) => {
      const hay = [e.type, e.toolName, e.model, e.agentId, e.sessionKey, JSON.stringify(e.payload || {})]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }
  return list;
}

// ────────────────────────────────────────────────────────────────────
// Renderers

function fmtTs(ms) {
  const d = new Date(ms);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms3 = String(d.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms3}`;
}

function shortKey(s, n = 40) {
  if (!s) return "";
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}

function renderEvents() {
  const list = visibleEvents();
  els.eventsCount.textContent = list.length;
  // Render newest first
  const reversed = list.slice().reverse();
  const frag = document.createDocumentFragment();
  reversed.forEach((e) => {
    const row = document.createElement("div");
    row.className = "event-row cat-" + e.category;
    if (e.id === state.selectedEventId) row.classList.add("selected");
    row.dataset.id = e.id;

    const icon = ICONS[e.type] || (e.category === "hook" ? ICONS.default_hook : ICONS.default_diag);
    // Replace "附加信息" column with a human-readable Chinese explanation
    // of what this event means. Enriched with tool/model when available.
    const hint = explainEvent(e);
    const right = [];
    if (e.durationMs != null) right.push(e.durationMs + "ms");
    // Token chips: for llm_output show input/output/cache breakdown; for
    // non-LLM events show nothing (not "0" — you wanted blank when no llm).
    if (e.type === "llm_output" && e.tokens) {
      const inTok = e.tokens.input ?? 0;
      const outTok = e.tokens.output ?? 0;
      const crTok = e.tokens.cacheRead ?? 0;
      right.push(`<span class="e-tok e-tok-in" title="input tokens">in ${fmtNum(inTok)}</span>`);
      right.push(`<span class="e-tok e-tok-out" title="output tokens">out ${fmtNum(outTok)}</span>`);
      if (crTok > 0) right.push(`<span class="e-tok e-tok-cache" title="cache read">cr ${fmtNum(crTok)}</span>`);
    }

    row.innerHTML =
      `<span class="e-time">${fmtTs(e.ts)}</span>` +
      `<span class="e-icon">${icon}</span>` +
      `<span class="e-cat">${e.category}</span>` +
      `<span class="e-type">${e.type}</span>` +
      `<span class="e-extra" title="${escapeHtml(hint)}">${escapeHtml(hint)}</span>` +
      `<span class="e-right">${right.join(" ")}</span>`;
    row.addEventListener("click", () => selectEvent(e.id));
    frag.appendChild(row);
  });
  els.eventsList.replaceChildren(frag);
}

function renderSessions() {
  // Build a tree: roots = sessions without parent (or parent not in map)
  const all = Array.from(state.sessions.values())
    .sort((a, b) => b.lastSeen - a.lastSeen);
  els.sessionsCount.textContent = all.length;

  const byKey = new Map(all.map((s) => [s.sessionKey, s]));
  const childrenOf = new Map();
  for (const s of all) {
    const parent = s.parentSessionKey && byKey.has(s.parentSessionKey) ? s.parentSessionKey : null;
    if (parent) {
      if (!childrenOf.has(parent)) childrenOf.set(parent, []);
      childrenOf.get(parent).push(s);
    }
  }
  const roots = all.filter((s) => !s.parentSessionKey || !byKey.has(s.parentSessionKey));

  const frag = document.createDocumentFragment();
  const visit = (s, depth) => {
    const item = document.createElement("div");
    item.className = "session-item";
    item.dataset.depth = Math.min(depth, 3);
    if (s.sessionKey === state.selectedSessionKey) item.classList.add("selected");
    const title = s.agentId || "agent";
    const chan = s.channel ? `[${s.channel}]` : "";
    item.innerHTML =
      `<div class="s-line1">` +
        `<span class="s-pill status-${s.status}">${s.status}</span>` +
        `<span class="s-title">${title} ${chan}</span>` +
      `</div>` +
      `<div class="s-line2">` +
        `<span>${shortKey(s.sessionKey, 28)}</span>` +
        `<span>· ${s.eventCount} evt</span>` +
      `</div>`;
    item.addEventListener("click", () => selectSession(s.sessionKey));
    frag.appendChild(item);
    const kids = childrenOf.get(s.sessionKey) || [];
    kids.forEach((k) => visit(k, depth + 1));
  };
  roots.forEach((r) => visit(r, 0));

  // Plus a "clear filter" button if a session is selected
  if (state.selectedSessionKey) {
    const clear = document.createElement("div");
    clear.className = "session-item";
    clear.style.textAlign = "center";
    clear.style.color = "var(--muted)";
    clear.textContent = "× clear session filter";
    clear.addEventListener("click", () => selectSession(null));
    frag.insertBefore(clear, frag.firstChild);
  }
  els.sessionsList.replaceChildren(frag);
}

function renderDetail() {
  const e = state.events.find((x) => x.id === state.selectedEventId);
  if (!e) {
    els.detail.innerHTML = `<div class="detail-empty">Click an event to view its full payload</div>`;
    return;
  }

  // ── Header meta chips ────────────────────────────────────────
  const chips = [];
  const chip = (cls, label, val) => {
    if (val == null || val === "") return;
    chips.push(`<span class="d-chip ${cls}"><b>${label}</b>${escapeHtml(String(val))}</span>`);
  };
  chip("cat-" + (e.category || "diag"), "", (e.category || "").toUpperCase());
  chip("", "seq ", e.seq);
  chip("", "", new Date(e.ts).toLocaleTimeString());
  chip("", "agent ", e.agentId);
  if (e.toolName)    chip("tool", "🔧 ", e.toolName + (e.toolStatus ? " · " + e.toolStatus : ""));
  if (e.model)       chip("model", "", e.model);
  if (e.provider)    chip("", "", e.provider);
  if (e.durationMs != null) chip("", "", e.durationMs + "ms");

  // ── Token block (if present) ─────────────────────────────────
  let tokensBlock = "";
  if (e.tokens && typeof e.tokens === "object") {
    const t = e.tokens;
    const row = (k, v) => v != null ? `<span class="d-tok"><i>${k}</i>${Number(v).toLocaleString()}</span>` : "";
    tokensBlock = `<div class="d-tokens">${
      row("in", t.input ?? t.prompt ?? t.in)
    }${row("out", t.output ?? t.completion ?? t.out)}${
      row("cache r", t.cacheRead ?? t.cache_read)
    }${row("cache w", t.cacheWrite ?? t.cache_write)}${
      row("total", t.total)
    }</div>`;
  }

  // ── Meta table (collapsible detail list) ─────────────────────
  const metaRows = [];
  const push = (k, v) => v != null && v !== "" && metaRows.push(`<dt>${k}</dt><dd>${escapeHtml(String(v))}</dd>`);
  push("time", new Date(e.ts).toISOString());
  push("seq", e.seq);
  push("category", e.category);
  push("type", e.type);
  push("session", e.sessionKey);
  push("agent", e.agentId);
  push("runId", e.runId);
  push("traceId", e.traceId);
  push("toolName", e.toolName);
  push("toolStatus", e.toolStatus);
  push("provider", e.provider);
  push("model", e.model);
  push("durationMs", e.durationMs);

  // ── Smart payload renderer ───────────────────────────────────
  const payloadHtml = renderPayload(e);

  // ── Raw JSON as a collapsed <details> ────────────────────────
  let rawJson;
  try { rawJson = JSON.stringify(e.payload ?? {}, null, 2); }
  catch { rawJson = String(e.payload); }

  // Cache the raw JSON on the detail node for the Copy button
  state.currentDetailJson = rawJson;

  els.detail.innerHTML = `
    <div class="d-head">
      <h3>#${e.seq} <span class="d-type">${escapeHtml(e.type || "")}</span></h3>
      <div class="d-chips">${chips.join("")}</div>
      ${tokensBlock}
    </div>

    <details class="d-section" open>
      <summary>Meta</summary>
      <dl class="d-meta">${metaRows.join("")}</dl>
    </details>

    <details class="d-section" open>
      <summary>Payload</summary>
      <div class="d-payload">${payloadHtml}</div>
    </details>

    <details class="d-section">
      <summary>Raw JSON</summary>
      <pre class="d-raw${state.detailWrap ? " wrap" : ""}">${escapeHtml(rawJson)}</pre>
    </details>
  `;
}

// ── Payload renderers ────────────────────────────────────────────
function renderPayload(e) {
  const p = e.payload;
  if (p == null) return `<div class="detail-empty">(no payload)</div>`;

  // 1) Messages array (LLM input/output) ────────────────────────
  const msgs = extractMessages(p);
  if (msgs) return renderMessages(msgs);

  // 2) Tool call / result ────────────────────────────────────────
  if (e.toolName || p.tool || p.toolName || p.arguments || p.result) {
    return renderToolBlock(e, p);
  }

  // 3) String payloads ───────────────────────────────────────────
  if (typeof p === "string") return `<pre class="d-block">${escapeHtml(p)}</pre>`;

  // 4) Fallback key/value tree ───────────────────────────────────
  return renderKV(p);
}

function extractMessages(p) {
  if (!p || typeof p !== "object") return null;
  if (Array.isArray(p.messages)) return p.messages;
  if (Array.isArray(p.input?.messages)) return p.input.messages;
  if (Array.isArray(p.prompt?.messages)) return p.prompt.messages;
  if (Array.isArray(p.output?.messages)) return p.output.messages;
  if (Array.isArray(p.response?.messages)) return p.response.messages;
  return null;
}

function renderMessages(msgs) {
  return `<div class="d-msgs">${msgs.map((m) => {
    const role = (m.role || m.type || "msg").toLowerCase();
    let content = m.content ?? m.text ?? m.message ?? "";
    if (Array.isArray(content)) {
      content = content.map((c) => {
        if (typeof c === "string") return c;
        if (c?.type === "text") return c.text || "";
        if (c?.type === "tool_use") return `⟨tool_use → ${c.name || "?"}⟩\n${safeStringify(c.input)}`;
        if (c?.type === "tool_result") return `⟨tool_result⟩\n${typeof c.content === "string" ? c.content : safeStringify(c.content)}`;
        return safeStringify(c);
      }).join("\n\n");
    } else if (typeof content !== "string") {
      content = safeStringify(content);
    }
    return `<div class="d-msg role-${escapeHtml(role)}">
      <div class="d-msg-role">${escapeHtml(role)}</div>
      <pre class="d-msg-body">${escapeHtml(content)}</pre>
    </div>`;
  }).join("")}</div>`;
}

function renderToolBlock(e, p) {
  const name = e.toolName || p.toolName || p.tool || "(tool)";
  const args = p.arguments ?? p.input ?? p.params;
  const result = p.result ?? p.output ?? p.response;
  const parts = [];
  parts.push(`<div class="d-tool-name">🔧 ${escapeHtml(String(name))}${e.toolStatus ? ` <span class="d-chip">${escapeHtml(e.toolStatus)}</span>` : ""}</div>`);
  if (args !== undefined) parts.push(`<div class="d-sub">arguments</div><pre class="d-block">${escapeHtml(safeStringify(args))}</pre>`);
  if (result !== undefined) parts.push(`<div class="d-sub">result</div><pre class="d-block">${escapeHtml(safeStringify(result))}</pre>`);
  if (args === undefined && result === undefined) parts.push(renderKV(p));
  return parts.join("");
}

function renderKV(obj) {
  if (obj == null) return "";
  if (typeof obj !== "object") return `<pre class="d-block">${escapeHtml(String(obj))}</pre>`;
  const entries = Object.entries(obj);
  if (!entries.length) return `<div class="detail-empty">(empty)</div>`;
  return `<dl class="d-kv">${entries.map(([k, v]) => {
    let display;
    if (v != null && typeof v === "object") {
      display = `<pre class="d-block">${escapeHtml(safeStringify(v))}</pre>`;
    } else if (typeof v === "string" && v.length > 120) {
      display = `<pre class="d-block">${escapeHtml(v)}</pre>`;
    } else {
      display = escapeHtml(String(v));
    }
    return `<dt>${escapeHtml(k)}</dt><dd>${display}</dd>`;
  }).join("")}</dl>`;
}

function safeStringify(v) {
  try { return JSON.stringify(v, null, 2); }
  catch { return String(v); }
}


function renderDetailIfSelectionStale() {
  if (state.selectedEventId && !state.events.find((x) => x.id === state.selectedEventId)) {
    state.selectedEventId = null;
    renderDetail();
  }
}

function renderTokenPanel() {
  const t = state.tokens;
  if (!t) {
    els.tokenPanel.innerHTML = "";
    return;
  }

  // ── NEW behavior per user ask: show the MOST RECENT LLM call in detail.
  // If a specific llm_output event is selected, show THAT one. Otherwise
  // show the latest llm_output seen anywhere.
  let focus = null;
  const selEvt = state.events.find((x) => x.id === state.selectedEventId);
  if (selEvt && selEvt.type === "llm_output" && selEvt.tokens) {
    focus = selEvt;
  } else {
    // find latest llm_output in visible window
    for (let i = state.events.length - 1; i >= 0; i--) {
      const e = state.events[i];
      if (e.type === "llm_output" && e.tokens) {
        if (state.selectedSessionKey && e.sessionKey !== state.selectedSessionKey) continue;
        focus = e;
        break;
      }
    }
  }

  // Running totals under focus (for session context)
  const bySession = t.bySession || {};
  const sessionBucket = state.selectedSessionKey
    ? bySession[state.selectedSessionKey] || emptyBucket()
    : t.overall || emptyBucket();
  const sessionLabel = state.selectedSessionKey ? "session running total" : "all sessions";

  const byModel = t.byModel || {};
  const topModels = Object.entries(byModel)
    .map(([k, v]) => ({ name: k, ...v }))
    .sort((a, b) => (b.total || 0) - (a.total || 0))
    .slice(0, 5);
  const maxTotal = Math.max(1, ...topModels.map((m) => m.total || 0));

  const focusHtml = focus
    ? renderFocusCall(focus)
    : `<div class="focus-empty">no llm_output captured yet　${
        state.selectedSessionKey ? "(in this session)" : ""
      }</div>`;

  els.tokenPanel.innerHTML = `
    <h4>last llm call</h4>
    ${focusHtml}
    <h4 style="margin-top:12px;">${sessionLabel}</h4>
    <div class="token-grid">
      <div class="tg-label">input</div><div class="tg-label">output</div><div class="tg-label">calls</div>
      <div class="tg-val">${fmtNum(sessionBucket.input)}</div>
      <div class="tg-val">${fmtNum(sessionBucket.output)}</div>
      <div class="tg-val">${fmtNum(sessionBucket.calls)}</div>
    </div>
    ${topModels.length ? `
      <h4 style="margin-top:10px;">by model (top 5)</h4>
      <div class="token-bars">
        ${topModels.map((m) => `
          <div class="token-bar">
            <span class="tb-name">${shortKey(m.name, 34)}</span>
            <span class="tg-val" style="text-align:right">${fmtNum(m.total)}</span>
            <div class="tb-bar"><span style="width:${Math.round(((m.total || 0) / maxTotal) * 100)}%"></span></div>
          </div>
        `).join("")}
      </div>
    ` : ""}
  `;
}

function renderFocusCall(evt) {
  const tk = evt.tokens || {};
  const inTok = tk.input ?? 0;
  const outTok = tk.output ?? 0;
  const crTok = tk.cacheRead ?? 0;
  const cwTok = tk.cacheWrite ?? 0;
  const totalTok = tk.total ?? inTok + outTok;
  const time = new Date(evt.ts).toLocaleTimeString();
  const model = evt.model || "";
  const provider = evt.provider || "";
  return `
    <div class="focus-call">
      <div class="focus-line1">
        <span class="focus-time">${time}</span>
        <span class="focus-model">${shortKey(provider + "/" + model, 40)}</span>
      </div>
      <div class="focus-tokens">
        <div class="ft-cell ft-in">
          <div class="ft-label">input</div>
          <div class="ft-val">${fmtNum(inTok)}</div>
        </div>
        <div class="ft-cell ft-out">
          <div class="ft-label">output</div>
          <div class="ft-val">${fmtNum(outTok)}</div>
        </div>
        <div class="ft-cell ft-total">
          <div class="ft-label">total</div>
          <div class="ft-val">${fmtNum(totalTok)}</div>
        </div>
      </div>
      ${crTok > 0 || cwTok > 0 ? `
        <div class="focus-cache">
          <span>cache read: ${fmtNum(crTok)}</span>
          <span>cache write: ${fmtNum(cwTok)}</span>
        </div>
      ` : ""}
    </div>
  `;
}

function emptyBucket() {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, calls: 0 };
}

function paintStatusBar() {
  if (!state.stats) return;
  els.dbRows.textContent = state.stats.storage?.rowCount ?? "–";
  els.totalSeq.textContent = state.stats.bus?.totalSeq ?? "–";
  els.bus.textContent = state.stats.bus?.bufferedEvents ?? "–";
  els.flushed.textContent = state.stats.storage?.flushedEvents ?? "–";
  els.dropped.textContent = state.stats.storage?.droppedBatches ?? "–";
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function fmtNum(n) {
  if (n == null) return "0";
  if (n < 1000) return String(n);
  if (n < 1_000_000) return (n / 1000).toFixed(1) + "k";
  return (n / 1_000_000).toFixed(1) + "m";
}

// ────────────────────────────────────────────────────────────────────
// Selection & navigation

function selectEvent(id) {
  state.selectedEventId = id;
  renderEvents();
  renderDetail();
  renderTokenPanel();
}
function selectSession(key) {
  state.selectedSessionKey = key;
  state.selectedEventId = null;
  renderSessions();
  renderEvents();
  renderDetail();
  renderTokenPanel();
}

function moveCursor(delta) {
  const list = visibleEvents();
  if (list.length === 0) return;
  const reversed = list.slice().reverse();
  let idx = reversed.findIndex((e) => e.id === state.selectedEventId);
  if (idx === -1) idx = 0;
  else idx = Math.max(0, Math.min(reversed.length - 1, idx + delta));
  selectEvent(reversed[idx].id);
  const row = els.eventsList.querySelector(`[data-id="${CSS.escape(reversed[idx].id)}"]`);
  if (row) row.scrollIntoView({ block: "nearest" });
}

// ────────────────────────────────────────────────────────────────────
// Toolbar wiring

els.pauseBtn.addEventListener("click", () => {
  state.paused = !state.paused;
  els.pauseBtn.textContent = state.paused ? "▶ resume" : "⏸ pause";
  els.pauseBtn.classList.toggle("on", state.paused);
  if (!state.paused) { renderEvents(); renderSessions(); }
});
els.clearBtn.addEventListener("click", () => {
  state.events = [];
  state.selectedEventId = null;
  renderEvents();
  renderDetail();
});
els.catHookBtn.addEventListener("click", () => {
  state.filters.category = state.filters.category === "hook" ? null : "hook";
  paintFilterButtons();
  renderEvents();
});
els.catDiagBtn.addEventListener("click", () => {
  state.filters.category = state.filters.category === "diag" ? null : "diag";
  paintFilterButtons();
  renderEvents();
});
els.llmBtn.addEventListener("click", () => {
  state.filters.onlyLlm = !state.filters.onlyLlm;
  paintFilterButtons();
  renderEvents();
});
els.toolBtn.addEventListener("click", () => {
  state.filters.onlyTools = !state.filters.onlyTools;
  paintFilterButtons();
  renderEvents();
});
els.searchInput.addEventListener("input", (ev) => {
  state.filters.search = ev.target.value;
  renderEvents();
});

// Detail pane controls
if (els.detailFullscreen) {
  els.detailFullscreen.addEventListener("click", toggleDetailFullscreen);
}
if (els.detailCopy) {
  els.detailCopy.addEventListener("click", async () => {
    if (!state.currentDetailJson) return;
    try {
      await navigator.clipboard.writeText(state.currentDetailJson);
      flashButton(els.detailCopy, "✓");
    } catch {
      flashButton(els.detailCopy, "!");
    }
  });
}
if (els.detailWrap) {
  els.detailWrap.addEventListener("click", () => {
    state.detailWrap = !state.detailWrap;
    els.detailWrap.classList.toggle("on", state.detailWrap);
    renderDetail();
  });
  els.detailWrap.classList.toggle("on", state.detailWrap);
}

function toggleDetailFullscreen() {
  state.detailFullscreen = !state.detailFullscreen;
  document.body.classList.toggle("detail-fullscreen", state.detailFullscreen);
  if (els.detailFullscreen) {
    els.detailFullscreen.textContent = state.detailFullscreen ? "✕" : "⛶";
    els.detailFullscreen.title = state.detailFullscreen ? "Exit fullscreen (F / Esc)" : "Fullscreen (F)";
  }
}

function flashButton(btn, text) {
  const prev = btn.textContent;
  btn.textContent = text;
  btn.classList.add("flash");
  setTimeout(() => { btn.textContent = prev; btn.classList.remove("flash"); }, 800);
}

function paintFilterButtons() {
  els.catHookBtn.classList.toggle("on", state.filters.category === "hook");
  els.catDiagBtn.classList.toggle("on", state.filters.category === "diag");
  els.llmBtn.classList.toggle("on", state.filters.onlyLlm);
  els.toolBtn.classList.toggle("on", state.filters.onlyTools);
}

// ────────────────────────────────────────────────────────────────────
// Keyboard shortcuts

document.addEventListener("keydown", (ev) => {
  if (ev.target === els.searchInput) {
    if (ev.key === "Escape") { els.searchInput.blur(); els.searchInput.value = ""; state.filters.search = ""; renderEvents(); }
    return;
  }
  switch (ev.key) {
    case "/":
      ev.preventDefault();
      els.searchInput.focus();
      els.searchInput.select();
      break;
    case "j":
      ev.preventDefault();
      moveCursor(1);
      break;
    case "k":
      ev.preventDefault();
      moveCursor(-1);
      break;
    case "p":
      els.pauseBtn.click();
      break;
    case "c":
      els.clearBtn.click();
      break;
    case "?":
      els.helpBox.classList.toggle("hidden");
      break;
    case "f":
    case "F":
      ev.preventDefault();
      toggleDetailFullscreen();
      break;
    case "Escape":
      els.helpBox.classList.add("hidden");
      if (state.detailFullscreen) { toggleDetailFullscreen(); break; }
      if (state.selectedSessionKey) selectSession(null);
      break;
  }
});

// ────────────────────────────────────────────────────────────────────
// Periodic /api polls for sessions + tokens (WS only sends events + stats)

async function pollAux() {
  try {
    const [sRes, tRes, stRes] = await Promise.all([
      fetch("/api/sessions").then((r) => r.json()).catch(() => null),
      fetch("/api/tokens").then((r) => r.json()).catch(() => null),
      fetch("/api/stats").then((r) => r.json()).catch(() => null),
    ]);
    if (sRes && Array.isArray(sRes.sessions)) {
      // Merge server truth into local (server is authoritative for status)
      for (const s of sRes.sessions) {
        state.sessions.set(s.sessionKey, {
          sessionKey: s.sessionKey,
          parentSessionKey: s.parentSessionKey,
          agentId: s.agentId,
          channel: s.channel,
          firstSeen: s.firstSeen,
          lastSeen: s.lastSeen,
          status: s.status,
          eventCount: s.eventCount,
        });
      }
      renderSessions();
    }
    if (tRes) {
      state.tokens = tRes;
      renderTokenPanel();
    }
    if (stRes) {
      state.stats = stRes;
      paintStatusBar();
    }
  } catch (_) { /* ignore */ }
}

// ────────────────────────────────────────────────────────────────────
// Bootstrap

function boot() {
  paintConnection();
  paintFilterButtons();
  renderEvents();
  renderSessions();
  renderDetail();
  connect();
  pollAux();
  setInterval(pollAux, 3000);
}
boot();
