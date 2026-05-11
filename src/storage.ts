/**
 * SQLite storage for ObserverEvents.
 *
 * Design:
 * - better-sqlite3 (synchronous C binding) — fastest choice for in-process writes.
 * - WAL journal mode so writes don't block UI-side reads.
 * - Events are queued in memory; a single transaction flushes the batch every
 *   `flushIntervalMs` or as soon as the queue reaches `flushBatchSize`.
 * - A cheap TTL sweep runs hourly.
 *
 * All I/O failures are caught and logged — dropping a batch is acceptable;
 * blocking the hot path is not.
 */

import Database from "better-sqlite3";
import type { Database as SqliteDatabase, Statement } from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import type { ObserverEvent } from "./types.js";
import { safeStringify } from "./util.js";

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS events (
  id                  TEXT PRIMARY KEY,
  ts                  INTEGER NOT NULL,
  seq                 INTEGER NOT NULL,
  category            TEXT    NOT NULL,
  type                TEXT    NOT NULL,
  run_id              TEXT,
  session_key         TEXT,
  session_id          TEXT,
  agent_id            TEXT,
  channel             TEXT,
  trace_id            TEXT,
  parent_session_key  TEXT,
  tool_name           TEXT,
  tool_call_id        TEXT,
  tool_status         TEXT,
  provider            TEXT,
  model               TEXT,
  duration_ms         INTEGER,
  tokens_input        INTEGER,
  tokens_output       INTEGER,
  tokens_cache_read   INTEGER,
  tokens_cache_write  INTEGER,
  tokens_total        INTEGER,
  payload             TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_ts          ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_session_ts  ON events(session_key, ts);
CREATE INDEX IF NOT EXISTS idx_events_run_ts      ON events(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts     ON events(type, ts);
CREATE INDEX IF NOT EXISTS idx_events_category    ON events(category, ts);
`;

export interface StorageOptions {
  dbPath: string;
  flushIntervalMs: number;
  flushBatchSize: number;
  retentionDays: number;
}

export interface StorageLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
  error: (msg: string) => void;
}

export interface StorageStats {
  rowCount: number;
  dbPath: string;
  queueSize: number;
  flushedEvents: number;
  flushedBatches: number;
  droppedBatches: number;
}

export class Storage {
  private db: SqliteDatabase | null = null;
  private insertStmt: Statement | null = null;
  private countStmt: Statement | null = null;
  private readonly opts: StorageOptions;
  private readonly logger: StorageLogger;

  private queue: ObserverEvent[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private ttlTimer: NodeJS.Timeout | null = null;

  private flushedEvents = 0;
  private flushedBatches = 0;
  private droppedBatches = 0;
  private closed = false;
  // True only after init() has fully prepared the DB and statements.
  // All read/write helpers must be no-ops when !ready so the plugin can
  // gracefully run "memory-only" if the native binding fails to load.
  private ready = false;

  constructor(opts: StorageOptions, logger: StorageLogger) {
    this.opts = opts;
    this.logger = logger;
  }

  /** Open the DB, create schema, prepare statements, start flush loop. */
  init(): void {
    mkdirSync(dirname(this.opts.dbPath), { recursive: true });
    this.db = new Database(this.opts.dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");
    this.db.pragma("foreign_keys = ON");
    this.db.exec(SCHEMA_SQL);

    this.insertStmt = this.db.prepare(`
      INSERT OR IGNORE INTO events (
        id, ts, seq, category, type,
        run_id, session_key, session_id, agent_id, channel,
        trace_id, parent_session_key,
        tool_name, tool_call_id, tool_status,
        provider, model, duration_ms,
        tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, tokens_total,
        payload
      ) VALUES (
        @id, @ts, @seq, @category, @type,
        @run_id, @session_key, @session_id, @agent_id, @channel,
        @trace_id, @parent_session_key,
        @tool_name, @tool_call_id, @tool_status,
        @provider, @model, @duration_ms,
        @tokens_input, @tokens_output, @tokens_cache_read, @tokens_cache_write, @tokens_total,
        @payload
      )
    `);

    this.countStmt = this.db.prepare(`SELECT COUNT(*) AS n FROM events`);

    // Flip the ready flag only after every statement is prepared. If any of
    // the lines above throw (e.g. better-sqlite3 native binding missing),
    // the caller's catch will trigger, ready stays false, and stats()/
    // rowCount()/enqueue() degrade safely instead of NPE-ing later.
    this.ready = true;

    // Periodic flush
    this.flushTimer = setInterval(() => {
      this.flush();
    }, this.opts.flushIntervalMs);
    this.flushTimer.unref?.();

    // Hourly TTL sweep
    this.ttlTimer = setInterval(
      () => {
        try {
          this.sweepExpired();
        } catch (err) {
          this.logger.warn(`[observer] ttl sweep failed: ${errMsg(err)}`);
        }
      },
      60 * 60 * 1000,
    );
    this.ttlTimer.unref?.();

    const rowCount = this.rowCount();
    this.logger.info(
      `[observer] storage ready · db=${this.opts.dbPath} · rows=${rowCount} · ` +
        `flush=${this.opts.flushIntervalMs}ms/${this.opts.flushBatchSize} · retention=${this.opts.retentionDays}d`,
    );

    // Kick off one TTL sweep shortly after start (not immediately, to avoid a
    // thundering-herd when many plugins init at once).
    setTimeout(() => {
      try {
        this.sweepExpired();
      } catch {
        /* ignore */
      }
    }, 30_000).unref?.();
  }

  /** Whether the SQLite backend is fully initialized. */
  isReady(): boolean {
    return this.ready && !this.closed;
  }

  /** Enqueue a single event. Non-blocking. */
  enqueue(evt: ObserverEvent): void {
    if (this.closed || !this.ready) return;
    this.queue.push(evt);
    if (this.queue.length >= this.opts.flushBatchSize) {
      this.flush();
    }
  }

  /** Write queued events in a single transaction. Safe to call anytime. */
  flush(): void {
    if (this.closed || !this.ready || this.queue.length === 0) return;
    const db = this.db;
    const insertStmt = this.insertStmt;
    if (!db || !insertStmt) return;
    const batch = this.queue;
    this.queue = [];

    try {
      const insertMany = db.transaction((rows: ObserverEvent[]) => {
        for (const r of rows) {
          insertStmt.run(toRow(r));
        }
      });
      insertMany(batch);
      this.flushedEvents += batch.length;
      this.flushedBatches += 1;
    } catch (err) {
      this.droppedBatches += 1;
      this.logger.error(
        `[observer] flush failed (batch=${batch.length}, dropped): ${errMsg(err)}`,
      );
    }
  }

  /** Delete events older than retentionDays. Returns deleted row count. */
  sweepExpired(): number {
    if (!this.ready || !this.db) return 0;
    const cutoff = Date.now() - this.opts.retentionDays * 86_400_000;
    const info = this.db.prepare(`DELETE FROM events WHERE ts < ?`).run(cutoff);
    const deleted = Number(info.changes);
    if (deleted > 0) {
      this.logger.info(
        `[observer] ttl sweep: deleted ${deleted} rows older than ${this.opts.retentionDays}d`,
      );
    }
    return deleted;
  }

  rowCount(): number {
    if (!this.ready || !this.countStmt) return 0;
    try {
      const row = this.countStmt.get() as { n: number } | undefined;
      return row?.n ?? 0;
    } catch {
      return 0;
    }
  }

  stats(): StorageStats {
    return {
      rowCount: this.rowCount(),
      dbPath: this.ready ? this.opts.dbPath : `${this.opts.dbPath} (memory-only)`,
      queueSize: this.queue.length,
      flushedEvents: this.flushedEvents,
      flushedBatches: this.flushedBatches,
      droppedBatches: this.droppedBatches,
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.flushTimer) clearInterval(this.flushTimer);
    if (this.ttlTimer) clearInterval(this.ttlTimer);
    // Final drain
    try {
      this.flush();
    } catch {
      /* ignore */
    }
    if (this.db) {
      try {
        this.db.close();
      } catch {
        /* ignore */
      }
    }
  }
}

// ────────────────────────────────────────────────────────────────────────

interface Row {
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

function toRow(e: ObserverEvent): Row {
  return {
    id: e.id,
    ts: e.ts,
    seq: e.seq,
    category: e.category,
    type: e.type,
    run_id: e.runId ?? null,
    session_key: e.sessionKey ?? null,
    session_id: e.sessionId ?? null,
    agent_id: e.agentId ?? null,
    channel: e.channel ?? null,
    trace_id: e.traceId ?? null,
    parent_session_key: e.parentSessionKey ?? null,
    tool_name: e.toolName ?? null,
    tool_call_id: e.toolCallId ?? null,
    tool_status: e.toolStatus ?? null,
    provider: e.provider ?? null,
    model: e.model ?? null,
    duration_ms: e.durationMs ?? null,
    tokens_input: e.tokens?.input ?? null,
    tokens_output: e.tokens?.output ?? null,
    tokens_cache_read: e.tokens?.cacheRead ?? null,
    tokens_cache_write: e.tokens?.cacheWrite ?? null,
    tokens_total: e.tokens?.total ?? null,
    payload: safeStringify(e.payload),
  };
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
