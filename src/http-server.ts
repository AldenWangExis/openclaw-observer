/**
 * Minimal HTTP + WebSocket server for the Observer UI.
 *
 * Uses Node's native `http` plus the `ws` library (already in deps).
 * Binds to 127.0.0.1 by default; remote access must go through SSH tunnel.
 *
 * Endpoints:
 *   GET  /                   →  index.html from /web
 *   GET  /app.js|style.css   →  same folder
 *   GET  /api/stats          →  { bus, storage }
 *   GET  /api/events         →  query recent events (see params)
 *   GET  /ws                 →  websocket; initial backlog + live stream
 *
 * All handlers wrap in try/catch; errors emit 500 without leaking stacks.
 */

import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { readFileSync, existsSync, statSync } from "node:fs";
import { extname, join, resolve, normalize } from "node:path";
import { WebSocketServer, type WebSocket } from "ws";

import type { EventBus } from "./event-bus.js";
import type { Storage } from "./storage.js";
import type { ObserverEvent } from "./types.js";
import { safeStringify } from "./util.js";

export interface HttpServerOptions {
  port: number;
  bindHost: string;
  webRoot: string; // absolute path to the /web dir
}

export interface HttpServerLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
  error: (msg: string) => void;
}

export interface HttpExtras {
  sessions: () => unknown;
  tokens: () => unknown;
}

/** How many backlog events to push to a new websocket client. */
const WS_INITIAL_BACKLOG = 200;

export class ObserverHttpServer {
  private server: Server | null = null;
  private wss: WebSocketServer | null = null;
  private busUnsubscribe: (() => void) | null = null;
  private extras: HttpExtras | null = null;

  constructor(
    private readonly opts: HttpServerOptions,
    private readonly bus: EventBus,
    private readonly storage: Storage | null,
    private readonly logger: HttpServerLogger,
  ) {}

  attachExtras(extras: HttpExtras): void {
    this.extras = extras;
  }

  start(): void {
    this.server = createServer((req, res) => {
      try {
        this.handle(req, res);
      } catch (err) {
        this.fail(res, 500, "internal_error");
        this.logger.error(`[observer] http handler crashed: ${errMsg(err)}`);
      }
    });

    this.wss = new WebSocketServer({ noServer: true });
    this.wss.on("connection", (socket) => this.onWsClient(socket));

    this.server.on("upgrade", (req, socket, head) => {
      try {
        const url = new URL(req.url ?? "/", "http://localhost");
        if (url.pathname !== "/ws") {
          socket.destroy();
          return;
        }
        this.wss!.handleUpgrade(req, socket, head, (ws) => {
          this.wss!.emit("connection", ws, req);
        });
      } catch (err) {
        socket.destroy();
        this.logger.warn(`[observer] upgrade failed: ${errMsg(err)}`);
      }
    });

    this.server.on("error", (err) => {
      this.logger.error(`[observer] http server error: ${errMsg(err)}`);
    });

    this.server.listen(this.opts.port, this.opts.bindHost, () => {
      this.logger.info(
        `[observer] http listening on http://${this.opts.bindHost}:${this.opts.port} · ws path=/ws · web=${this.opts.webRoot}`,
      );
    });

    // Fan-out bus events to all ws clients
    this.busUnsubscribe = this.bus.subscribe((evt) => this.broadcast(evt));
  }

  stop(): void {
    this.busUnsubscribe?.();
    try {
      this.wss?.clients.forEach((c) => {
        try {
          c.close();
        } catch {
          /* ignore */
        }
      });
      this.wss?.close();
    } catch {
      /* ignore */
    }
    try {
      this.server?.close();
    } catch {
      /* ignore */
    }
  }

  // ────────────────────────────────────────────────────────────────────
  // HTTP
  // ────────────────────────────────────────────────────────────────────

  private handle(req: IncomingMessage, res: ServerResponse): void {
    const url = new URL(req.url ?? "/", "http://localhost");

    if (req.method !== "GET" && req.method !== "HEAD") {
      return this.fail(res, 405, "method_not_allowed");
    }

    if (url.pathname === "/api/stats") {
      return this.sendJson(res, 200, this.buildStats());
    }
    if (url.pathname === "/api/sessions") {
      return this.sendSessions(res, url);
    }
    if (url.pathname === "/api/tokens") {
      const data = this.extras?.tokens() ?? {};
      return this.sendJson(res, 200, data);
    }
    if (url.pathname === "/api/events") {
      return this.sendEvents(res, url);
    }
    if (url.pathname === "/api/health") {
      return this.sendJson(res, 200, { ok: true, ts: Date.now() });
    }

    return this.serveStatic(url.pathname, res);
  }

  private buildStats() {
    return {
      bus: this.bus.stats(),
      storage: this.storage?.stats() ?? { rowCount: 0, dbPath: "<disabled>" },
      ts: Date.now(),
    };
  }

  /**
   * /api/sessions  — list known sessions, newest active first.
   *
   * Two backing stores, behaviour mirrors /api/events:
   * - `source=db` (DEFAULT) — derived from the persisted events table via
   *   `Storage.querySessions()`. Survives gateway restarts; this is what
   *   the dashboard wants 99 % of the time.
   * - `source=bus` — current process's in-memory `SessionTracker`
   *   snapshot. Same shape, but resets on every restart. Useful when
   *   storage is disabled, or to compare live state vs. persisted view.
   *
   * Falls back to `bus` automatically when storage is unavailable
   * (memory-only mode after a native-binding failure, etc).
   */
  private sendSessions(res: ServerResponse, url: URL): void {
    const limit = clamp(parseIntOr(url.searchParams.get("limit"), 200), 1, 1000);
    const since = parseIntOr(url.searchParams.get("since"), 0);
    const requested = url.searchParams.get("source");
    const source: "db" | "bus" =
      requested === "bus" || !this.storage?.isReady() ? "bus" : "db";

    if (source === "bus") {
      const data = (this.extras?.sessions() ?? []) as unknown[];
      return this.sendJson(res, 200, {
        source: "bus",
        count: data.length,
        sessions: data,
      });
    }

    const sessions = this.storage!.querySessions({ limit, sinceTs: since });
    return this.sendJson(res, 200, {
      source: "db",
      count: sessions.length,
      sessions,
    });
  }

  private sendEvents(res: ServerResponse, url: URL): void {
    const limit = clamp(parseIntOr(url.searchParams.get("limit"), 200), 1, 5000);
    const session = url.searchParams.get("session") ?? undefined;
    const type = url.searchParams.get("type") ?? undefined;
    const category = url.searchParams.get("category") ?? undefined;
    const since = parseIntOr(url.searchParams.get("since"), 0);

    // Default source: in-memory ring (fast, newest). Only touch DB if the caller
    // explicitly asks for persisted history or filters require it.
    const source = url.searchParams.get("source") ?? (session || type || since ? "db" : "bus");

    if (source === "bus" || !this.storage) {
      let events = this.bus.recent(limit);
      if (type) events = events.filter((e) => e.type === type);
      if (category) events = events.filter((e) => e.category === category);
      if (session) events = events.filter((e) => e.sessionKey === session);
      return this.sendJson(res, 200, { source: "bus", count: events.length, events });
    }

    // DB path — rough MVP query. Keeps memory usage bounded via limit.
    const events = this.queryDb({ limit, session, type, category, since });
    this.sendJson(res, 200, { source: "db", count: events.length, events });
  }

  private queryDb(q: {
    limit: number;
    session?: string;
    type?: string;
    category?: string;
    since: number;
  }): ObserverEvent[] {
    if (!this.storage) return [];
    const db = (this.storage as unknown as { db: { prepare: (s: string) => { all: (...p: unknown[]) => unknown[] } } }).db;
    const wheres: string[] = [];
    const params: unknown[] = [];
    if (q.since > 0) {
      wheres.push("ts >= ?");
      params.push(q.since);
    }
    if (q.session) {
      wheres.push("session_key = ?");
      params.push(q.session);
    }
    if (q.type) {
      wheres.push("type = ?");
      params.push(q.type);
    }
    if (q.category) {
      wheres.push("category = ?");
      params.push(q.category);
    }
    const whereSql = wheres.length ? `WHERE ${wheres.join(" AND ")}` : "";
    const sql = `SELECT * FROM events ${whereSql} ORDER BY ts DESC LIMIT ?`;
    params.push(q.limit);
    try {
      const rows = db.prepare(sql).all(...params) as unknown as DbRow[];
      return rows.map(rowToEvent).reverse(); // return newest last to match bus order
    } catch (err) {
      this.logger.warn(`[observer] db query failed: ${errMsg(err)}`);
      return [];
    }
  }

  // ────────────────────────────────────────────────────────────────────
  // WebSocket
  // ────────────────────────────────────────────────────────────────────

  private onWsClient(socket: WebSocket): void {
    // Backlog
    try {
      const backlog = this.bus.recent(WS_INITIAL_BACKLOG);
      socket.send(safeStringify({ type: "backlog", events: backlog }));
      socket.send(safeStringify({ type: "stats", stats: this.buildStats() }));
    } catch (err) {
      this.logger.warn(`[observer] ws backlog send failed: ${errMsg(err)}`);
    }
    socket.on("error", () => {
      /* swallow; close handler will run */
    });
  }

  private broadcast(evt: ObserverEvent): void {
    if (!this.wss) return;
    const payload = safeStringify({ type: "event", event: evt });
    for (const client of this.wss.clients) {
      if (client.readyState === client.OPEN) {
        try {
          client.send(payload);
        } catch {
          /* ignore per-client errors */
        }
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────
  // Static files (restricted to webRoot)
  // ────────────────────────────────────────────────────────────────────

  private serveStatic(pathname: string, res: ServerResponse): void {
    const safePath = normalize(pathname).replace(/^\/+/, "");
    const rel = safePath === "" || safePath === "/" ? "index.html" : safePath;
    const absolute = resolve(join(this.opts.webRoot, rel));
    // traversal guard
    if (!absolute.startsWith(resolve(this.opts.webRoot))) {
      return this.fail(res, 403, "forbidden");
    }
    if (!existsSync(absolute) || !statSync(absolute).isFile()) {
      return this.fail(res, 404, "not_found");
    }
    try {
      const buf = readFileSync(absolute);
      res.writeHead(200, {
        "content-type": mimeOf(absolute),
        "cache-control": "no-cache",
        "content-length": String(buf.byteLength),
      });
      res.end(buf);
    } catch (err) {
      this.fail(res, 500, "io_error");
      this.logger.warn(`[observer] static read failed: ${errMsg(err)}`);
    }
  }

  private sendJson(res: ServerResponse, status: number, body: unknown): void {
    const json = safeStringify(body);
    res.writeHead(status, {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "content-length": String(Buffer.byteLength(json)),
    });
    res.end(json);
  }

  private fail(res: ServerResponse, status: number, code: string): void {
    try {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(`{"ok":false,"error":"${code}"}`);
    } catch {
      /* ignore */
    }
  }
}

// ────────────────────────────────────────────────────────────────────────

interface DbRow {
  id: string;
  ts: number;
  seq: number;
  category: string;
  type: string;
  run_id: string | null;
  session_key: string | null;
  session_id: string | null;
  agent_id: string | null;
  channel: string | null;
  trace_id: string | null;
  parent_session_key: string | null;
  tool_name: string | null;
  tool_call_id: string | null;
  tool_status: string | null;
  provider: string | null;
  model: string | null;
  duration_ms: number | null;
  tokens_input: number | null;
  tokens_output: number | null;
  tokens_cache_read: number | null;
  tokens_cache_write: number | null;
  tokens_total: number | null;
  payload: string;
}

function rowToEvent(r: DbRow): ObserverEvent {
  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(r.payload) as Record<string, unknown>;
  } catch {
    payload = { _raw: r.payload };
  }
  const tokens: ObserverEvent["tokens"] = {};
  if (r.tokens_input != null) tokens.input = r.tokens_input;
  if (r.tokens_output != null) tokens.output = r.tokens_output;
  if (r.tokens_cache_read != null) tokens.cacheRead = r.tokens_cache_read;
  if (r.tokens_cache_write != null) tokens.cacheWrite = r.tokens_cache_write;
  if (r.tokens_total != null) tokens.total = r.tokens_total;
  const evt: ObserverEvent = {
    id: r.id,
    ts: r.ts,
    seq: r.seq,
    category: r.category as "hook" | "diag",
    type: r.type,
    payload,
  };
  if (r.run_id) evt.runId = r.run_id;
  if (r.session_key) evt.sessionKey = r.session_key;
  if (r.session_id) evt.sessionId = r.session_id;
  if (r.agent_id) evt.agentId = r.agent_id;
  if (r.channel) evt.channel = r.channel;
  if (r.trace_id) evt.traceId = r.trace_id;
  if (r.parent_session_key) evt.parentSessionKey = r.parent_session_key;
  if (r.tool_name) evt.toolName = r.tool_name;
  if (r.tool_call_id) evt.toolCallId = r.tool_call_id;
  if (r.tool_status) evt.toolStatus = r.tool_status as ObserverEvent["toolStatus"];
  if (r.provider) evt.provider = r.provider;
  if (r.model) evt.model = r.model;
  if (r.duration_ms != null) evt.durationMs = r.duration_ms;
  if (Object.keys(tokens).length) evt.tokens = tokens;
  return evt;
}

function mimeOf(path: string): string {
  const ext = extname(path).toLowerCase();
  switch (ext) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "application/javascript; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".png":
      return "image/png";
    case ".ico":
      return "image/x-icon";
    default:
      return "application/octet-stream";
  }
}

function parseIntOr(s: string | null, d: number): number {
  if (!s) return d;
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : d;
}
function clamp(n: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, n));
}
function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
