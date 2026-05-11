# 🦞 OpenClaw Observer

> Real-time observability dashboard for OpenClaw — see every prompt,
> every tool call, every token, every diagnostic event, **live**.

![dashboard screenshot](./docs-assets/screenshot.png)

OpenClaw Observer is a Gateway plugin that runs **in-process** with your
OpenClaw Gateway. It subscribes to Plugin Hooks (full content) and
Diagnostic Events (timing/metadata), buffers them in memory, persists
them to a local SQLite database, and serves a zero-build single-page
dashboard on `http://localhost:10010` (binds to all interfaces by
default — see [Security](#security-notes) before exposing it).

No external service. No extra agent. No code change to your existing
plugins. Drop it in and watch your agents work.

---

## Why?

When you run multiple agents, sub-agents, channels and tools at the
same time, the gateway logs scroll too fast and `/status` only shows a
snapshot. Observer gives you:

- **One screen** that summarises every active session
- **Full prompt / tool-result content** the moment it happens
- **Token accounting** per call, per session, per model
- **7 days of history** you can scroll back through
- **Zero impact** on the live agent loop — every hook is
  fire-and-forget with try/catch around the bus push

---

## Features

| | |
|---|---|
| 🪝 **All key hooks** | `llm_input`, `llm_output`, `before/after_tool_call`, `message_received`, `message_sent`, `session_start/end`, `subagent_*`, `before/after_compaction` |
| 🩺 **Diagnostic events** | All 35+ Gateway diagnostic events (run start/end, model tokens, channel send, etc.) |
| 🧠 **Multi-session view** | Left column shows every session (incl. sub-agents) with status, last activity and token totals |
| ⚡ **Token panel** | Live `input / output / cache_r / cache_w / total` per session and per model |
| 🔧 **Tool placeholders** | A tool call shows up as `🔧 running…` and is updated in place to `✅ done` with duration when it finishes |
| 🗄️ **SQLite history** | Configurable retention (default 7 days), WAL mode, batched writes |
| 🛡️ **Redaction** | `api_key`, `token`, `secret`, `Authorization`, `sk-…`, `ghp_…` are masked at push-time before any subscriber sees them; oversized fields auto-truncate |
| 🧪 **Zero front-end build** | Plain HTML + CSS + ES module JS. Edit `web/app.js`, refresh, done. |
| 🔄 **Hot-reload safe** | Singleton bus survives `register()` re-entry so events never get dropped during plugin reloads |

---

## Quick start

### Option A — Install from ClawHub (recommended)

```bash
openclaw plugins install clawhub:openclaw-observer
openclaw plugins enable openclaw-observer
openclaw gateway restart
```

Then open <http://localhost:10010>.

### Option B — Install from a local checkout

```bash
git clone https://github.com/shenlibin/openclaw-observer.git
cd openclaw-observer
npm install
npm run build
openclaw plugins install . --link
openclaw plugins enable openclaw-observer
openclaw gateway restart
```

`--link` means the plugin is symlinked, not copied — handy if you want
to hack on it.

### Open the dashboard

By default Observer binds to **all interfaces** (`0.0.0.0:10010`),
so any of these work:

```
http://localhost:10010
http://<host-ip>:10010
```

> ⚠️ Observer has **no auth** in v1. The default open bind is convenient
> for single-machine ops but **must be firewalled** in any shared
> network. Lock it down by either:
>
> - **Restricting to loopback** — set `bindHost: "127.0.0.1"` in your
>   OpenClaw config (see below) and use an SSH tunnel for remote
>   access:
>   ```bash
>   ssh -L 10010:127.0.0.1:10010 your-host
>   ```
> - **Binding to a specific NIC** — set `bindHost` to the NIC's IP
>   (e.g. `"10.0.1.23"`) so the public NIC never sees the port.
> - **Front with a reverse proxy** that adds auth (nginx/Caddy +
>   BasicAuth/OIDC) and keep Observer on `127.0.0.1`.
>
> See the [Security notes](#security-notes) section for the full
> threat model.

---

## What you see

```
┌─ Sessions ───────────┬─ Events ─────────────────────────────────┬─ Detail ──────────┐
│ ▾ main · feishu      │ 14:29:01 📩 message_received  · 12 chars │ token panel       │
│   ⚡ 1.4M · feishu… │ 14:29:01 🎯 llm_input         · 8.3K     │   in   42 130     │
│ ▸ subagent#a1        │ 14:29:03 🔧 read · path=/etc/hosts       │   out   1 211     │
│   running tool…      │ 14:29:03 ✅ read · 213 ms · 168 B        │   total 43 341    │
│ ▸ daily · cron       │ 14:29:04 📝 llm_output · 1.2K · 43 341tk │                   │
│                      │ 14:29:04 📤 message_sent    · 312 chars  │ event detail JSON │
└──────────────────────┴──────────────────────────────────────────┴───────────────────┘
   db rows 19 412 · buffered 5000/153 412 · flushed 153 100/520 · dropped 0   events 1247
```

- **Left** — collapsible session tree, parent ↔ sub-agent relations, token totals
- **Centre** — chronological event stream, filterable by category / type / agent
- **Right (top)** — selected session's token panel
- **Right (bottom)** — full JSON of the selected event (one click to copy)
- **Bottom** — live counters (db rows, buffer fill, flush stats, drops)

Keyboard: `j` / `k` move event cursor, `/` focus search, `p` pause stream, `c` clear, `?` help.

---

## Configuration

Add to your OpenClaw config (the plugin reads its own subtree):

```json
{
  "plugins": {
    "entries": {
      "openclaw-observer": {
        "enabled": true,
        "config": {
          "port": 10010,
          "bindHost": "0.0.0.0",
          "retentionDays": 7,
          "captureContent": true,
          "redact": {
            "enabled": true,
            "maxFieldBytes": 51200
          }
        }
      }
    }
  }
}
```

You can also use the config tool: `openclaw plugins enable openclaw-observer`
plus `gateway config.patch` to merge any of these keys.

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | Master switch |
| `port` | `10010` | HTTP/WebSocket port |
| `bindHost` | `0.0.0.0` | All interfaces by default. Set to `127.0.0.1` for loopback-only or to a specific NIC IP (firewall the port — there is no auth) |
| `dbPath` | `""` | Absolute path to SQLite DB. Empty = `<plugin>/data/observer.db` |
| `retentionDays` | `7` | TTL in days (1–365) |
| `bufferSize` | `5000` | In-memory ring buffer (100–100000) |
| `flushIntervalMs` | `1000` | Periodic SQLite flush interval |
| `flushBatchSize` | `500` | Force flush when queue ≥ this many events |
| `captureContent` | `true` | Capture prompts/tool params/results. `false` = metadata only |
| `redact.enabled` | `true` | Mask sensitive fields before they touch the bus |
| `redact.maxFieldBytes` | `51200` | Per-field byte cap; longer values are truncated |

---

## REST + WebSocket API

The dashboard is just a client; you can build your own.

| Endpoint | Method | Description |
|---|---|---|
| `/api/sessions` | GET | All known sessions with status, token totals, lifecycle markers. Default `source=db` — derived from the persisted events table so it survives gateway restarts. Pass `?source=bus` for the in-process `SessionTracker` snapshot, `?limit=` (1–1000) and `?since=<ts>` to scope. |
| `/api/events` | GET | Paged historical events (`?since=<ts>&limit=…&type=…&source=bus\|db`) |
| `/api/session/:sessionKey` | GET | Single-session summary + recent events |
| `/api/tokens` | GET | Aggregated token usage by model |
| `/stream` | WS | Live event stream (events + periodic stats) |

Every event has the same shape (see [`src/types.ts`](src/types.ts)):

```ts
interface ObserverEvent {
  id: string;          // uuid
  ts: number;          // epoch ms
  seq: number;         // monotonic per process
  category: "hook" | "diag";
  type: string;        // hook name or diagnostic event type
  sessionKey?: string;
  agentId?: string;
  runId?: string;
  toolName?: string;
  toolStatus?: "started" | "completed" | "error" | "blocked";
  model?: string;
  durationMs?: number;
  tokens?: { input?, output?, cacheRead?, cacheWrite?, total? };
  payload: unknown;    // full content (already redacted)
}
```

---

## Architecture

```
┌─ OpenClaw Gateway (single process) ─────────────────────────────────┐
│                                                                     │
│  ┌─ openclaw-observer ─────────────────────────────────────┐        │
│  │                                                          │        │
│  │  api.on(hook, …)        onDiagnosticEvent(…)             │        │
│  │        ↓                       ↓                         │        │
│  │  ┌────────────── EventBus (ring 5000) ──────────────┐    │        │
│  │  │   .push() applies redaction in-place             │    │        │
│  │  └──────────────────────────────────────────────────┘    │        │
│  │        ↓                       ↓                  ↓      │        │
│  │   SQLite writer        SessionTracker      WebSocket     │        │
│  │   (1s batch + WAL)     TokenAggregator     broadcaster   │        │
│  │                                                          │        │
│  └────────────────────────────┬─────────────────────────────┘        │
│                               ↓                                      │
│   HTTP/WS server on 0.0.0.0:10010 (default) → SPA + REST + /stream  │
└──────────────────────────────────────────────────────────────────────┘
```

Why a Gateway plugin and not an external `tail`? Hooks are delivered
**synchronously** in-process, which is the only way to capture full
prompts/tool params before the gateway forwards them.

---

## Performance

- `EventBus.push` is O(1) and never `await`s — it cannot back-pressure
  the agent loop.
- SQLite writes run in a `BEGIN…COMMIT` batch every `flushIntervalMs`
  (default 1 s) or whenever `flushBatchSize` is reached.
- WAL mode is enabled.
- The dashboard caps in-memory rendered events at 2000; older events
  are still in SQLite and reachable via `/api/events`.

On a small VM with ~10 active sessions Observer typically uses **<60 MB
RAM** and **<2 % CPU**.

---

## Security notes

- **No auth in v1.** Observer trusts everything that can reach its
  port. Anyone who hits `:10010` can read every prompt, tool call,
  model output and token total.
- **Default bind is `0.0.0.0`.** This is convenient for single-host
  workflows but means **the port is exposed to every network the host
  can reach** unless you firewall it. Lock it down with one of:
  - `bindHost: "127.0.0.1"` + SSH tunnel
    (`ssh -L 10010:127.0.0.1:10010 your-host`)
  - `bindHost: "<internal-NIC-IP>"` so only the internal network sees it
  - reverse proxy (nginx / Caddy) with BasicAuth or OIDC in front
  - host-level firewall rules limiting source IPs (`iptables`, `ufw`,
    cloud security groups)
- Redaction runs **before** anything reaches the bus, the database or
  WebSocket clients. The full unredacted payload never leaves the hook
  callback frame, but redaction is best-effort regex-based — assume a
  determined reader of the dashboard can still glean intent.
- Set `captureContent: false` to record only metadata (durations,
  token counts, types) and skip every prompt/tool body — useful when
  the gateway is processing third-party PII.
- The SQLite file may grow to GBs over a busy week. Lower
  `retentionDays` or set a smaller `bufferSize` if disk is tight.

---

## Development

```bash
git clone https://github.com/shenlibin/openclaw-observer.git
cd openclaw-observer
npm install
npm run watch        # tsc --watch
openclaw plugins install . --link
openclaw gateway restart
```

The front end has no build — edit `web/app.js` / `web/index.html` /
`web/style.css` and refresh the browser.

### Project layout

```
.
├── src/
│   ├── index.ts          # plugin entry (definePluginEntry)
│   ├── event-bus.ts      # in-memory ring buffer + broadcaster
│   ├── hooks.ts          # registers all Plugin Hooks
│   ├── diagnostics.ts    # subscribes to Diagnostic Events
│   ├── storage.ts        # better-sqlite3 + batched writer + TTL sweep
│   ├── http-server.ts    # HTTP + WS + REST API
│   ├── session-tracker.ts
│   ├── token-aggregator.ts
│   ├── redact.ts         # api_key/token/secret/Authorization masking
│   ├── types.ts
│   └── util.ts
├── web/                  # zero-build dashboard (HTML/CSS/JS)
├── openclaw.plugin.json  # manifest + config schema
├── package.json          # npm + openclaw.compat metadata
└── data/                 # observer.db (created at runtime, gitignored)
```

### Testing

There is no automated test harness in v1 — the design is validated
end-to-end by:

1. start the gateway with the plugin enabled
2. open `http://localhost:10010`
3. send a message that triggers tool calls / sub-agents
4. confirm events stream in, token totals add up, and the SQLite row
   count grows in the status bar

---

## Roadmap

- v0.2: optional Basic Auth and an HTTPS reverse-proxy guide
- v0.3: cost estimation per provider (price tables)
- v0.4: Gantt-style timeline of parallel tool calls
- v0.5: export a session to Markdown / share link
- v0.6: alert rules (`tool error rate`, `latency > x`) → webhook

PRs welcome.

---

## License

MIT © 2026 Libin Shen — see [LICENSE](./LICENSE).

Built for the OpenClaw community. Enjoy. 🦞
