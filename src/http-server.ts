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
 *   GET  /api/groups         →  known group aliases from SQLite
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

/** How many events the bus must contain before we consider it "warm". */
const BUS_WARM_THRESHOLD = 1;

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
const WS_INITIAL_BACKLOG = 2000;

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
    this.wss.on("connection", (socket, req) => this.onWsClient(socket, req));

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
      return this.sendTokens(res, url);
    }
    if (url.pathname === "/api/users") {
      return this.sendUsers(res, url);
    }
    if (url.pathname === "/api/groups") {
      return this.sendGroups(res, url);
    }
    if (url.pathname === "/api/events") {
      return this.sendEvents(res, url);
    }
    if (url.pathname === "/api/health") {
      return this.sendJson(res, 200, this.buildHealth());
    }
    if (url.pathname === "/api/metrics") {
      return this.sendText(res, 200, this.buildPromMetrics());
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

  private buildHealth() {
    const storageStats = this.storage?.stats();
    const lastFlushAgo =
      storageStats?.lastFlushAt && storageStats.lastFlushAt > 0
        ? Date.now() - storageStats.lastFlushAt
        : null;
    return {
      ok: !this.storage || storageStats?.ready !== false,
      ts: Date.now(),
      bus: "healthy",
      storage: !this.storage ? "disabled" : storageStats?.ready ? "ready" : "memory-only",
      lastFlushAgo,
      queueSize: storageStats?.queueSize ?? 0,
      maxQueueSize: storageStats?.maxQueueSize ?? 0,
      droppedEvents: storageStats?.droppedEvents ?? 0,
      lastFlushError: storageStats?.lastFlushError,
    };
  }

  private buildPromMetrics(): string {
    const bs = this.bus.stats();
    const ss = this.storage?.stats();
    const lines: string[] = [
      "# HELP observer_events_total Total observer events seen by this process.",
      "# TYPE observer_events_total counter",
    ];
    for (const [type, count] of Object.entries(bs.eventsByType ?? {})) {
      lines.push(`observer_events_total{type="${escapePromLabel(type)}"} ${count}`);
    }
    lines.push(
      "# HELP observer_events_by_category_total Total observer events by category.",
      "# TYPE observer_events_by_category_total counter",
    );
    for (const [category, count] of Object.entries(bs.eventsByCategory ?? {})) {
      lines.push(`observer_events_by_category_total{category="${escapePromLabel(category)}"} ${count}`);
    }
    lines.push(
      "# HELP observer_db_queue_size Current queued events waiting for SQLite flush.",
      "# TYPE observer_db_queue_size gauge",
      `observer_db_queue_size ${ss?.queueSize ?? 0}`,
      "# HELP observer_flush_latency_seconds Duration of the latest SQLite flush.",
      "# TYPE observer_flush_latency_seconds gauge",
      `observer_flush_latency_seconds ${((ss?.lastFlushDurationMs ?? 0) / 1000).toFixed(6)}`,
      "# HELP observer_dropped_batches_total Dropped storage batches or overflow incidents.",
      "# TYPE observer_dropped_batches_total counter",
      `observer_dropped_batches_total ${ss?.droppedBatches ?? 0}`,
      "# HELP observer_dropped_events_total Dropped observer events.",
      "# TYPE observer_dropped_events_total counter",
      `observer_dropped_events_total ${ss?.droppedEvents ?? 0}`,
      "# HELP observer_storage_ready Whether SQLite storage is ready (1=yes, 0=no).",
      "# TYPE observer_storage_ready gauge",
      `observer_storage_ready ${ss?.ready ? 1 : 0}`,
    );
    return `${lines.join("\n")}\n`;
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
    const openId = url.searchParams.get("openId") ?? undefined;
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

    const sessions = this.storage!.querySessions({ limit, sinceTs: since, openId });
    return this.sendJson(res, 200, {
      source: "db",
      count: sessions.length,
      sessions,
    });
  }

  /**
   * /api/events — return recent events.
   *
   * Source selection:
   * - explicit `?source=bus` → always in-memory ring buffer.
   * - explicit `?source=db`  → always persisted DB.
   * - omitted              → DB when storage is ready; bus as fallback.
   *   Additionally, if bus is empty (gateway just restarted) and storage is
   *   ready, we automatically use DB even if no explicit source was given.
   *
   * Filters: `?limit=`, `?session=`, `?type=`, `?category=`, `?since=`
   * (since is epoch-ms; sinceTs is epoch-ms too — same meaning).
   */
  private sendEvents(res: ServerResponse, url: URL): void {
    const limit = clamp(parseIntOr(url.searchParams.get("limit"), 200), 1, 5000);
    const session = url.searchParams.get("session") ?? undefined;
    const openId = url.searchParams.get("openId") ?? undefined;
    const type = url.searchParams.get("type") ?? undefined;
    const category = url.searchParams.get("category") ?? undefined;
    const since = parseIntOr(url.searchParams.get("since"), 0);
    const requested = url.searchParams.get("source");

    const busWarm = this.bus.recent(1).length >= BUS_WARM_THRESHOLD;
    const storageReady = this.storage?.isReady() ?? false;

    // Determine which backing store to use
    const useDb =
      requested === "db" ||
      (requested !== "bus" && storageReady && (!busWarm || session || openId || type || since > 0));

    if (!useDb || !this.storage) {
      let events = this.bus.recent(limit);
      if (type) events = events.filter((e) => e.type === type);
      if (category) events = events.filter((e) => e.category === category);
      if (session) events = events.filter((e) => e.sessionKey === session);
      if (openId) events = events.filter((e) => e.openId === openId);
      return this.sendJson(res, 200, { source: "bus", count: events.length, events });
    }

    const events = this.storage.queryRecentEvents({
      limit,
      sinceTs: since,
      session,
      openId,
      type,
      category,
    });
    this.sendJson(res, 200, { source: "db", count: events.length, events });
  }

  /**
   * /api/tokens — aggregated token usage.
   *
   * - DEFAULT (`source=db` or omitted): SQL aggregation from the events
   *   table via `Storage.queryTokens()`. Survives gateway restarts.
   * - `source=bus`: current process's in-memory `TokenAggregator` snapshot.
   *   Resets on every restart; useful for comparing live vs persisted view,
   *   or when storage is unavailable.
   *
   * Optional: `?since=<epoch-ms>` to limit the aggregation window (db only).
   */
  private sendTokens(res: ServerResponse, url: URL): void {
    const requested = url.searchParams.get("source");
    const since = parseIntOr(url.searchParams.get("since"), 0);
    const openId = url.searchParams.get("openId") ?? undefined;
    const storageReady = this.storage?.isReady() ?? false;

    const useDb = requested !== "bus" && storageReady;

    if (!useDb) {
      const data = { ...((this.extras?.tokens() ?? {}) as Record<string, unknown>) };
      delete data["lastHour"];
      delete data["lastDay"];
      if (this.storage?.isReady()) {
        const windows = this.storage.queryTokens({ sinceTs: since, openId });
        data["lastHour"] = windows.lastHour;
        data["lastDay"] = windows.lastDay;
        data["windowsSource"] = "db";
      }
      return this.sendJson(res, 200, { source: "bus", ...data });
    }

    const data = this.storage!.queryTokens({ sinceTs: since, openId });
    this.sendJson(res, 200, { source: "db", ...data });
  }

  private sendUsers(res: ServerResponse, url: URL): void {
    const limit = clamp(parseIntOr(url.searchParams.get("limit"), 200), 1, 1000);
    if (!this.storage?.isReady()) {
      return this.sendJson(res, 200, { source: "bus", count: 0, users: [] });
    }
    const users = this.storage.listKnownUsers({ limit });
    return this.sendJson(res, 200, { source: "db", count: users.length, users });
  }

  private sendGroups(res: ServerResponse, url: URL): void {
    const limit = clamp(parseIntOr(url.searchParams.get("limit"), 200), 1, 1000);
    if (!this.storage?.isReady()) {
      return this.sendJson(res, 200, { source: "bus", count: 0, groups: [] });
    }
    const groups = this.storage.listKnownGroups({ limit });
    return this.sendJson(res, 200, { source: "db", count: groups.length, groups });
  }

  // ────────────────────────────────────────────────────────────────────
  // WebSocket
  // ────────────────────────────────────────────────────────────────────

  private onWsClient(socket: WebSocket, req: IncomingMessage): void {
    try {
      const url = new URL(req.url ?? "/ws", "http://localhost");
      const requested = url.searchParams.get("source");
      const sinceSeq = parseIntOr(url.searchParams.get("sinceSeq"), 0);
      const sinceTs = parseIntOr(url.searchParams.get("sinceTs"), 0);
      const busBacklog =
        sinceSeq > 0
          ? this.bus.recent(WS_INITIAL_BACKLOG).filter((e) => e.seq > sinceSeq)
          : this.bus.recent(WS_INITIAL_BACKLOG);
      let backlog = busBacklog;
      let backlogSource: "bus" | "db" = "bus";

      // Default to persisted history whenever storage is available. The bus
      // is still merged in to cover events not flushed yet, and remains an
      // explicit debug/fallback source via /ws?source=bus or memory-only mode.
      if (requested !== "bus" && this.storage?.isReady()) {
        const dbBacklog = this.storage.queryRecentEvents({
          limit: WS_INITIAL_BACKLOG,
          sinceTs: sinceTs > 0 ? sinceTs : undefined,
          afterSeq: sinceTs > 0 ? undefined : sinceSeq,
        });
        backlog = mergeEventsNewestLast(dbBacklog, busBacklog).slice(-WS_INITIAL_BACKLOG);
        backlogSource = "db";
      }
      socket.send(safeStringify({ type: "backlog", source: backlogSource, events: backlog }));
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

  private sendText(res: ServerResponse, status: number, body: string): void {
    res.writeHead(status, {
      "content-type": "text/plain; version=0.0.4; charset=utf-8",
      "cache-control": "no-store",
      "content-length": String(Buffer.byteLength(body)),
    });
    res.end(body);
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
function mergeEventsNewestLast(a: ObserverEvent[], b: ObserverEvent[]): ObserverEvent[] {
  const seen = new Set<string>();
  const out: ObserverEvent[] = [];
  for (const evt of [...a, ...b]) {
    if (seen.has(evt.id)) continue;
    seen.add(evt.id);
    out.push(evt);
  }
  out.sort((x, y) => x.ts - y.ts || x.seq - y.seq);
  return out;
}
function escapePromLabel(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}
function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
