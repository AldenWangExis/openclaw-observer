/**
 * OpenClaw Observer Plugin — entry point.
 *
 * Subscribes to Plugin Hooks (full content) + Diagnostic Events (metadata)
 * and exposes a real-time dashboard on http://127.0.0.1:10010.
 *
 * See PLAN.md (project root) for the full spec.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { EventBus } from "./event-bus.js";
import type { ObserverEvent } from "./types.js";
import { registerObserverHooks } from "./hooks.js";
import { subscribeDiagnosticEvents } from "./diagnostics.js";
import { Storage } from "./storage.js";
import { ObserverHttpServer } from "./http-server.js";
import { SessionTracker } from "./session-tracker.js";
import { TokenAggregator } from "./token-aggregator.js";
import { redactEvent } from "./redact.js";

/**
 * Resolved runtime config (merged with defaults from configSchema).
 */
interface ObserverConfig {
  enabled: boolean;
  port: number;
  bindHost: string;
  dbPath: string;
  retentionDays: number;
  bufferSize: number;
  flushIntervalMs: number;
  flushBatchSize: number;
  captureContent: boolean;
  redact: {
    enabled: boolean;
    maxFieldBytes: number;
  };
}

const DEFAULTS: ObserverConfig = {
  enabled: true,
  port: 10010,
  bindHost: "127.0.0.1",
  dbPath: "",
  retentionDays: 7,
  bufferSize: 5000,
  flushIntervalMs: 1000,
  flushBatchSize: 500,
  captureContent: true,
  redact: {
    enabled: true,
    maxFieldBytes: 51200,
  },
};

function mergeConfig(input: unknown): ObserverConfig {
  const cfg = (input ?? {}) as Partial<ObserverConfig>;
  return {
    enabled: cfg.enabled ?? DEFAULTS.enabled,
    port: cfg.port ?? DEFAULTS.port,
    bindHost: cfg.bindHost ?? DEFAULTS.bindHost,
    dbPath: cfg.dbPath ?? DEFAULTS.dbPath,
    retentionDays: cfg.retentionDays ?? DEFAULTS.retentionDays,
    bufferSize: cfg.bufferSize ?? DEFAULTS.bufferSize,
    flushIntervalMs: cfg.flushIntervalMs ?? DEFAULTS.flushIntervalMs,
    flushBatchSize: cfg.flushBatchSize ?? DEFAULTS.flushBatchSize,
    captureContent: cfg.captureContent ?? DEFAULTS.captureContent,
    redact: {
      enabled: cfg.redact?.enabled ?? DEFAULTS.redact.enabled,
      maxFieldBytes: cfg.redact?.maxFieldBytes ?? DEFAULTS.redact.maxFieldBytes,
    },
  };
}

const observerPlugin = {
  id: "openclaw-observer",
  name: "OpenClaw Observer",
  description:
    "Real-time observability dashboard for agent runs, tool calls, model I/O, and diagnostic events",

  register(api: OpenClawPluginApi) {
    // Idempotent guard: gateway reload/hot-swap may call register() multiple
    // times in the same process. We need exactly one EventBus / HTTP server /
    // storage instance across all of them; otherwise hook callbacks get
    // sprayed over N sibling buses and the one bound to the HTTP server gets
    // only ~1/N of the traffic (how /api/tokens stayed empty).
    type ObserverSingleton = {
      bus: EventBus;
      logger: { info: (m: string) => void; warn: (m: string) => void; error: (m: string) => void };
      registeredHooks: number;
    };
    const globalKey = Symbol.for("openclaw-observer/singleton");
    const g = globalThis as unknown as { [k: symbol]: ObserverSingleton | undefined };
    if (g[globalKey]) {
      api.logger.info(
        `[observer] register() called again; reusing existing singleton (hooks=${g[globalKey]!.registeredHooks})`,
      );
      // Re-attach hook callbacks to the *existing* bus so the new register()
      // instance does not end up with a dead listener.
      const existing = g[globalKey]!;
      try {
        const rep = registerObserverHooks(api, existing.bus, {
          debug: (m) => api.logger.debug?.(m),
          warn: (m) => api.logger.warn(m),
        });
        existing.registeredHooks += rep.registered.length;
        api.logger.info(
          `[observer] re-bound ${rep.registered.length} hooks to existing bus (total=${existing.registeredHooks})`,
        );
      } catch (err) {
        api.logger.warn(`[observer] hook re-bind failed: ${(err as Error).message}`);
      }
      return;
    }

    const config = mergeConfig(api.pluginConfig);

    if (!config.enabled) {
      api.logger.info("[observer] plugin disabled via config");
      return;
    }

    // ── M2/M7: EventBus with in-place redaction ──
    // Single bus; events are redacted at push-time so every subscriber sees
    // the safe version. This avoids the previous two-bus fanout (where a
    // stale plugin instance could leave the redactedBus empty while the
    // producer-bus was the live one).
    const redactOpts = {
      enabled: !!config.redact?.enabled,
      maxFieldBytes: Number(config.redact?.maxFieldBytes ?? 50_000),
    };
    const rawBus = new EventBus({ bufferSize: config.bufferSize });
    // Wrap .push so producers stay oblivious of redaction.
    const originalPush = rawBus.push.bind(rawBus);
    rawBus.push = ((evt) => {
      const safe = redactEvent(evt as ObserverEvent, redactOpts);
      return originalPush(safe);
    }) as typeof rawBus.push;
    // From this point on, `bus` === `redactedBus` === the single source of truth.
    const bus = rawBus;

    const hookReport = registerObserverHooks(api, bus, {
      debug: (m) => api.logger.debug?.(m),
      warn: (m) => api.logger.warn(m),
    });

    // ── M3: SQLite storage + batch writer (consumes from redactedBus) ──
    const dbPath = resolveDbPath(config.dbPath);
    const storage = new Storage(
      {
        dbPath,
        flushIntervalMs: config.flushIntervalMs,
        flushBatchSize: config.flushBatchSize,
        retentionDays: config.retentionDays,
      },
      {
        info: (m) => api.logger.info(m),
        warn: (m) => api.logger.warn(m),
        error: (m) => api.logger.error(m),
      },
    );

    try {
      storage.init();
      bus.subscribe((evt) => storage.enqueue(evt));
    } catch (err) {
      api.logger.error(
        `[observer] storage init failed, running memory-only: ${(err as Error).message}`,
      );
    }

    // ── M5: SessionTracker (consumes bus) ──
    const sessionTracker = new SessionTracker({
      maxSessions: 200,
      idleTtlMs: 30 * 60 * 1000, // 30m
      sweepIntervalMs: 60 * 1000,
    });
    sessionTracker.attach(bus);

    // ── M6: TokenAggregator (consumes bus) ──
    const tokenAggregator = new TokenAggregator();
    tokenAggregator.attach(bus);

    // M3 verification: periodic stats line so we can watch row count grow.
    // Wrapped in try/catch because (a) setInterval callbacks have no upstream
    // handler, so any throw here becomes an uncaughtException that kills the
    // gateway, and (b) storage.stats() touches the SQLite layer — if the
    // native binding is broken or the DB closed under us, we'd rather log
    // and keep running than crash.
    const statsTimer = setInterval(() => {
      try {
        const bs = bus.stats();
        const ss = storage.stats();
        api.logger.info(
          `[observer] stats · bus=${bs.bufferedEvents}/${bs.totalSeq} subs=${bs.subscribers} · ` +
            `db.rows=${ss.rowCount} queue=${ss.queueSize} flushed=${ss.flushedEvents}/${ss.flushedBatches} dropped=${ss.droppedBatches}`,
        );
      } catch (err) {
        api.logger.warn(`[observer] stats tick failed: ${(err as Error).message}`);
      }
    }, 30_000);
    statsTimer.unref?.();

    // Subscribe to diagnostic events asynchronously; don't block register().
    void subscribeDiagnosticEvents(bus, {
      info: (m) => api.logger.info(m),
      warn: (m) => api.logger.warn(m),
    });

    // ── M4: HTTP + WebSocket server (serves from bus) ──
    const webRoot = resolveWebRoot();
    const httpServer = new ObserverHttpServer(
      { port: config.port, bindHost: config.bindHost, webRoot },
      bus,
      storage,
      {
        info: (m) => api.logger.info(m),
        warn: (m) => api.logger.warn(m),
        error: (m) => api.logger.error(m),
      },
    );
    httpServer.attachExtras({
      sessions: () => sessionTracker.snapshot(),
      tokens: () => tokenAggregator.snapshotAsJson(),
    });
    try {
      httpServer.start();
    } catch (err) {
      api.logger.error(`[observer] http start failed: ${(err as Error).message}`);
    }

    // Banner
    api.logger.info(
      `[observer] M5–M7 loaded · http=http://${config.bindHost}:${config.port} · ` +
        `hooks registered=${hookReport.registered.length}/${hookReport.registered.length + hookReport.failed.length} · ` +
        `redact=${redactOpts.enabled ? `on(${redactOpts.maxFieldBytes}B)` : "off"} · ` +
        `bufferSize=${config.bufferSize}`,
    );
    if (hookReport.failed.length > 0) {
      api.logger.warn(
        `[observer] hooks failed to register: ${hookReport.failed
          .map((f) => `${f.hook}(${f.error})`)
          .join(", ")}`,
      );
    }

    // Publish singleton so subsequent register() calls can re-bind hooks.
    g[globalKey] = {
      bus,
      logger: {
        info: (m) => api.logger.info(m),
        warn: (m) => api.logger.warn(m),
        error: (m) => api.logger.error(m),
      },
      registeredHooks: hookReport.registered.length,
    };

    // M8+ will polish the UI on top of the now-rich /api/* endpoints.
  },
};

export default observerPlugin;

/**
 * Resolve dbPath: if absolute use it; otherwise, relative to this file's
 * grandparent (plugin root's `data/` dir).
 */
function resolveDbPath(configured: string): string {
  if (configured && configured.length > 0) {
    return configured.startsWith("/") ? configured : resolve(process.cwd(), configured);
  }
  const here = dirname(fileURLToPath(import.meta.url));
  // dist/index.js  →  plugin root = parent of dist
  return resolve(here, "..", "data", "observer.db");
}

/** Plugin root's `web/` directory. */
function resolveWebRoot(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  return resolve(here, "..", "web");
}

/**
 * Build a compact one-line summary for M2 diagnostic logging.
 * Kept intentionally short; full payload is inspected in the UI.
 */
function summarize(evt: import("./types.js").ObserverEvent): string {
  const parts: string[] = [];
  if (evt.sessionKey) parts.push(`sk=${short(evt.sessionKey)}`);
  if (evt.agentId) parts.push(`agent=${short(evt.agentId)}`);
  if (evt.toolName) parts.push(`tool=${evt.toolName}`);
  if (evt.model) parts.push(`model=${short(evt.model)}`);
  if (evt.durationMs != null) parts.push(`${evt.durationMs}ms`);
  if (evt.tokens?.total != null) parts.push(`tok=${evt.tokens.total}`);
  else if (evt.tokens?.input != null || evt.tokens?.output != null) {
    parts.push(`tok=${evt.tokens.input ?? 0}/${evt.tokens.output ?? 0}`);
  }
  return parts.join(" ");
}

function short(s: string, n = 24): string {
  if (s.length <= n) return s;
  return s.slice(0, n - 1) + "…";
}
