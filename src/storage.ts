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

import { runMigrations } from "./migrations.js";
import type { ObserverEvent, SessionState, SessionStatus } from "./types.js";
import { parseCronSessionKey, parseGroupChatIdFromSessionKey, safeStringify } from "./util.js";

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
CREATE INDEX IF NOT EXISTS idx_events_seq         ON events(seq);
CREATE INDEX IF NOT EXISTS idx_events_session_ts  ON events(session_key, ts);
CREATE INDEX IF NOT EXISTS idx_events_run_ts      ON events(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts     ON events(type, ts);
CREATE INDEX IF NOT EXISTS idx_events_category    ON events(category, ts);
`;

export interface StorageOptions {
  dbPath: string;
  flushIntervalMs: number;
  flushBatchSize: number;
  maxQueueSize?: number;
  retentionDays: number;
}

export interface StorageLogger {
  info: (msg: string) => void;
  warn: (msg: string) => void;
  error: (msg: string) => void;
}

export interface StorageStats {
  ready: boolean;
  rowCount: number;
  dbPath: string;
  queueSize: number;
  maxQueueSize: number;
  flushedEvents: number;
  flushedBatches: number;
  droppedBatches: number;
  droppedEvents: number;
  lastFlushAt: number;
  lastFlushDurationMs: number;
  lastFlushError?: string;
}

export interface KnownUser {
  openId: string;
  senderName?: string;
  firstSeen: number;
  lastSeen: number;
  messageCount: number;
  sessionCount: number;
}

export interface KnownGroup {
  chatId: string;
  groupName: string;
  source: string;
  updatedAt: number;
}

export interface KnownCronJob {
  jobId: string;
  jobName: string;
  enabled?: boolean;
  source: string;
  updatedAt: number;
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
  private droppedEvents = 0;
  private readonly maxQueueSize: number;
  private lastFlushAt = 0;
  private lastFlushDurationMs = 0;
  private lastFlushError: string | undefined;
  private closed = false;
  // True only after init() has fully prepared the DB and statements.
  // All read/write helpers must be no-ops when !ready so the plugin can
  // gracefully run "memory-only" if the native binding fails to load.
  private ready = false;

  constructor(opts: StorageOptions, logger: StorageLogger) {
    this.opts = opts;
    this.logger = logger;
    this.maxQueueSize = Math.max(1, opts.maxQueueSize ?? opts.flushBatchSize * 10);
  }

  /** Open the DB, create schema, prepare statements, start flush loop. */
  init(): void {
    mkdirSync(dirname(this.opts.dbPath), { recursive: true });
    this.db = new Database(this.opts.dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("wal_autocheckpoint = 1000");
    this.db.pragma("synchronous = NORMAL");
    this.db.pragma("foreign_keys = ON");
    this.db.exec(SCHEMA_SQL);
    runMigrations(this.db, this.logger);

    this.insertStmt = this.db.prepare(`
      INSERT OR IGNORE INTO events (
        id, ts, seq, category, type,
        run_id, session_key, session_id, agent_id, channel,
        trace_id, parent_session_key,
        tool_name, tool_call_id, tool_status,
        provider, model, duration_ms,
        tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, tokens_total,
        open_id, sender_name,
        payload
      ) VALUES (
        @id, @ts, @seq, @category, @type,
        @run_id, @session_key, @session_id, @agent_id, @channel,
        @trace_id, @parent_session_key,
        @tool_name, @tool_call_id, @tool_status,
        @provider, @model, @duration_ms,
        @tokens_input, @tokens_output, @tokens_cache_read, @tokens_cache_write, @tokens_total,
        @open_id, @sender_name,
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
    if (this.queue.length >= this.maxQueueSize) {
      this.queue.shift();
      this.droppedEvents += 1;
      this.droppedBatches += 1;
      if (this.droppedEvents === 1 || this.droppedEvents % 1000 === 0) {
        this.logger.warn(
          `[observer] storage queue overflow: dropped oldest event (droppedEvents=${this.droppedEvents}, maxQueueSize=${this.maxQueueSize})`,
        );
      }
    }
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

    const started = Date.now();
    try {
      const insertMany = db.transaction((rows: ObserverEvent[]) => {
        for (const r of rows) {
          insertStmt.run(toRow(r));
        }
      });
      insertMany(batch);
      this.lastFlushAt = Date.now();
      this.lastFlushDurationMs = this.lastFlushAt - started;
      this.lastFlushError = undefined;
      this.flushedEvents += batch.length;
      this.flushedBatches += 1;
    } catch (err) {
      this.lastFlushAt = Date.now();
      this.lastFlushDurationMs = this.lastFlushAt - started;
      this.lastFlushError = errMsg(err);
      this.droppedBatches += 1;
      this.droppedEvents += batch.length;
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
    const stats: StorageStats = {
      ready: this.isReady(),
      rowCount: this.rowCount(),
      dbPath: this.ready ? this.opts.dbPath : `${this.opts.dbPath} (memory-only)`,
      queueSize: this.queue.length,
      maxQueueSize: this.maxQueueSize,
      flushedEvents: this.flushedEvents,
      flushedBatches: this.flushedBatches,
      droppedBatches: this.droppedBatches,
      droppedEvents: this.droppedEvents,
      lastFlushAt: this.lastFlushAt,
      lastFlushDurationMs: this.lastFlushDurationMs,
    };
    if (this.lastFlushError) stats.lastFlushError = this.lastFlushError;
    return stats;
  }

  /**
   * Read recent events out of the persisted store, returned newest-last
   * to match the in-memory bus order. Used by:
   *  - `GET /api/events` when the caller asks for `source=db` (or when
   *    the in-memory ring buffer is empty after a restart and the
   *    handler falls back automatically).
   *  - The WebSocket initial backlog, which prefers `bus.recent()` and
   *    falls back to this when the bus has nothing yet.
   *
   * Filters are AND-ed; any of them may be omitted. The composite
   * (session_key, ts) and (type, ts) indexes cover the common cases.
   * Returns [] when storage is not ready (memory-only fallback).
   */
  queryRecentEvents(opts: {
    limit?: number;
    sinceTs?: number;
    afterSeq?: number;
    session?: string;
    openId?: string;
    type?: string;
    category?: string;
  } = {}): ObserverEvent[] {
    if (!this.ready || !this.db) return [];
    const limit = clampInt(opts.limit, 1, 5000, 200);
    const wheres: string[] = [];
    const params: unknown[] = [];
    if (opts.sinceTs && opts.sinceTs > 0) {
      wheres.push("ts >= ?");
      params.push(opts.sinceTs);
    }
    if (opts.afterSeq && opts.afterSeq > 0) {
      wheres.push("seq > ?");
      params.push(opts.afterSeq);
    }
    if (opts.session) {
      wheres.push("session_key = ?");
      params.push(opts.session);
    }
    if (opts.openId) {
      wheres.push("open_id = ?");
      params.push(opts.openId);
    }
    if (opts.type) {
      wheres.push("type = ?");
      params.push(opts.type);
    }
    if (opts.category) {
      wheres.push("category = ?");
      params.push(opts.category);
    }
    const whereSql = wheres.length ? `WHERE ${wheres.join(" AND ")}` : "";
    const sql = `SELECT * FROM events ${whereSql} ORDER BY ts DESC LIMIT ?`;
    params.push(limit);
    try {
      const rows = this.db.prepare(sql).all(...params) as DbEventRow[];
      // Reverse so the newest event is last — matches `bus.recent()` order
      // and lets the dashboard append-render without re-sorting.
      return rows.map(rowToEvent).reverse();
    } catch (err) {
      this.logger.warn(`[observer] queryRecentEvents failed: ${errMsg(err)}`);
      return [];
    }
  }

  /**
   * SQL-aggregate token usage straight out of the events table, in the
   * same shape that `TokenAggregator.snapshotAsJson()` produces. Survives
   * gateway restarts; the in-memory aggregator is reset to zero on every
   * restart whereas this method always reflects the full retention window.
   *
   * Aggregation rules mirror the in-memory code path:
   *  - Source rows: only `type='llm_output'` events with at least one
   *    non-null token column (otherwise `calls` would inflate).
   *  - Buckets: `overall`, `bySession[sessionKey]`, `byAgent[agentId]`,
   *    `byModel["provider/model"]`. Rows with NULL keys are skipped for
   *    the per-key buckets but still counted in `overall`.
   *  - Windows: `lastHour` / `lastDay` are computed with a `ts >= ?`
   *    cut-off in SQL, so the figures are absolute (last 1h / last 24h
   *    from now) — slightly different from the in-memory aggregator
   *    which uses fixed-edge buckets, but easier to reason about and
   *    matches what users expect when they look at "last hour".
   *  - `total`: prefer `tokens_total` if non-NULL, else `input + output`,
   *    matching `addInto()`.
   */
  queryTokens(opts: { sinceTs?: number; openId?: string } = {}): TokenSnapshot {
    if (!this.ready || !this.db) return emptyTokenSnapshot();
    const sinceTs = opts.sinceTs && opts.sinceTs > 0 ? opts.sinceTs : 0;
    const now = Date.now();
    const hourCutoff = now - 60 * 60 * 1000;
    const dayCutoff = now - 24 * 60 * 60 * 1000;

    // SUM expressions are reused by every grouping; defined once for clarity.
    // SQLite has no FILTER (bool), so we use CASE to mask rows outside the
    // window. tokens_total falls back to input+output to mirror addInto().
    const SUM_EXPR = `
      COALESCE(SUM(tokens_input), 0)        AS t_in,
      COALESCE(SUM(tokens_output), 0)       AS t_out,
      COALESCE(SUM(tokens_cache_read), 0)   AS t_cr,
      COALESCE(SUM(tokens_cache_write), 0)  AS t_cw,
      COALESCE(SUM(COALESCE(tokens_total, COALESCE(tokens_input,0) + COALESCE(tokens_output,0))), 0) AS t_tot,
      COUNT(*) AS calls
    `;
    const whereParts = [
      "type = 'llm_output'",
      "ts >= ?",
      "(tokens_input IS NOT NULL OR tokens_output IS NOT NULL OR tokens_total IS NOT NULL)",
    ];
    const baseParams: unknown[] = [sinceTs];
    if (opts.openId) {
      whereParts.push("open_id = ?");
      baseParams.push(opts.openId);
    }
    const BASE_WHERE = `WHERE ${whereParts.join(" AND ")}`;

    try {
      const overallRow = this.db
        .prepare(`SELECT ${SUM_EXPR} FROM events ${BASE_WHERE}`)
        .get(...baseParams) as TokenAggRow | undefined;

      const hourRow = this.db
        .prepare(`SELECT ${SUM_EXPR} FROM events ${BASE_WHERE} AND ts >= ?`)
        .get(...baseParams, hourCutoff) as TokenAggRow | undefined;

      const dayRow = this.db
        .prepare(`SELECT ${SUM_EXPR} FROM events ${BASE_WHERE} AND ts >= ?`)
        .get(...baseParams, dayCutoff) as TokenAggRow | undefined;

      const sessionRows = this.db
        .prepare(
          `SELECT session_key AS k, ${SUM_EXPR} FROM events ${BASE_WHERE} AND session_key IS NOT NULL GROUP BY session_key`,
        )
        .all(...baseParams) as TokenAggKeyedRow[];

      const agentRows = this.db
        .prepare(
          `SELECT agent_id AS k, ${SUM_EXPR} FROM events ${BASE_WHERE} AND agent_id IS NOT NULL GROUP BY agent_id`,
        )
        .all(...baseParams) as TokenAggKeyedRow[];

      // Model key is "provider/model" (or just one of them) — match the
      // in-memory `formatModelKey()` exactly.
      const modelRows = this.db
        .prepare(
          `SELECT
             CASE
               WHEN provider IS NOT NULL AND model IS NOT NULL THEN provider || '/' || model
               WHEN model IS NOT NULL THEN model
               WHEN provider IS NOT NULL THEN provider
             END AS k,
             ${SUM_EXPR}
           FROM events ${BASE_WHERE} AND (provider IS NOT NULL OR model IS NOT NULL)
           GROUP BY k`,
        )
        .all(...baseParams) as TokenAggKeyedRow[];

      return {
        overall: rowToBucket(overallRow),
        lastHour: { totals: rowToBucket(hourRow), windowStart: hourCutoff, windowEnd: now },
        lastDay: { totals: rowToBucket(dayRow), windowStart: dayCutoff, windowEnd: now },
        bySession: keyedRowsToBuckets(sessionRows),
        byAgent: keyedRowsToBuckets(agentRows),
        byModel: keyedRowsToBuckets(modelRows),
      };
    } catch (err) {
      this.logger.warn(`[observer] queryTokens failed: ${errMsg(err)}`);
      return emptyTokenSnapshot();
    }
  }

  /**
   * Derive a SessionState[] directly from persisted events.
   *
   * Why this exists: the in-memory `SessionTracker` only tracks events the
   * current process saw. After a gateway restart its Map is empty even
   * though SQLite still has the history, so the dashboard's session list
   * goes blank until new traffic arrives. This method runs one composite
   * query and rebuilds the same shape from the events table — survives
   * restarts, lives as long as `retentionDays`.
   *
   * Field mapping (must match `SessionTracker.apply()` semantics):
   * - firstSeen   = MIN(ts)
   * - lastSeen    = MAX(ts)
   * - eventCount  = COUNT(*)
   * - tokens.*    = SUM(...)
   * - parentSessionKey / agentId / channel = any non-null value
   *   (MIN ignores NULL in SQLite; for these "sticky" identity fields
   *   that's fine — a session has at most one of each in practice)
   * - status / currentAction / currentActionStart = derived from the most
   *   recent status-bearing hook event, mirroring the tracker's "last
   *   event wins" logic
   * - title       = first text-like field of the earliest message_received
   *   payload, mirroring `firstTextPreview()` in the tracker
   *
   * Performance: composite query uses four window-function sub-queries
   * (last_status, last_action, last_diag_state, first_msg). All indexed on
   * session_key/ts.
   * For ~10⁵ rows the query is sub-millisecond on a warm cache; for ~10⁶
   * still well under 100ms.
   *
   * Returns [] if storage is not ready (memory-only fallback) — caller
   * should then fall back to the in-memory tracker.
   */
  querySessions(opts: { limit?: number; sinceTs?: number; openId?: string } = {}): SessionState[] {
    if (!this.ready || !this.db) return [];
    const limit = clampInt(opts.limit, 1, 1000, 200);
    const sinceTs = opts.sinceTs && opts.sinceTs > 0 ? opts.sinceTs : 0;
    const openId = opts.openId && opts.openId.trim().length > 0 ? opts.openId.trim() : undefined;

    // Type IDs that the tracker reads. Keep these in sync with the
    // STATUS_FROM_HOOK / currentAction logic in session-tracker.ts.
    const STATUS_TYPES = [
      "session_start",
      "session_end",
      "message_received",
      "message_sent",
      "llm_input",
      "llm_output",
      "before_tool_call",
      "after_tool_call",
      "before_compaction",
      "after_compaction",
    ];
    const ACTION_TYPES = [
      "before_tool_call",
      "after_tool_call",
      "llm_input",
      "llm_output",
      "session_end",
    ];

    // Build IN (?, ?, …) placeholders so prepared-statement caching still works.
    const statusPlaceholders = STATUS_TYPES.map(() => "?").join(",");
    const actionPlaceholders = ACTION_TYPES.map(() => "?").join(",");
    const openIdFilter = openId
      ? `AND session_key IN (SELECT DISTINCT session_key FROM events WHERE open_id = ?)`
      : "";

    const sql = `
      WITH base AS (
        SELECT
          session_key,
          MIN(ts) AS first_seen,
          MAX(ts) AS last_seen,
          COUNT(*) AS event_count,
          COALESCE(SUM(tokens_input), 0) AS t_in,
          COALESCE(SUM(tokens_output), 0) AS t_out,
          COALESCE(SUM(tokens_cache_read), 0) AS t_cr,
          COALESCE(SUM(tokens_cache_write), 0) AS t_cw,
          COALESCE(SUM(tokens_total), 0) AS t_tot,
          MIN(parent_session_key) AS parent_session_key,
          MIN(agent_id) AS agent_id,
          MIN(channel) AS channel,
          CASE
            WHEN session_key GLOB 'agent:*:group:oc_*' AND INSTR(session_key, ':group:') > 0
              THEN SUBSTR(session_key, INSTR(session_key, ':group:') + 7)
            ELSE NULL
          END AS group_chat_id,
          CASE
            WHEN session_key GLOB 'agent:*:cron:*' AND INSTR(session_key, ':cron:') > 0
              THEN CASE
                WHEN INSTR(SUBSTR(session_key, INSTR(session_key, ':cron:') + 6), ':run:') > 0
                  THEN SUBSTR(
                    SUBSTR(session_key, INSTR(session_key, ':cron:') + 6),
                    1,
                    INSTR(SUBSTR(session_key, INSTR(session_key, ':cron:') + 6), ':run:') - 1
                  )
                ELSE SUBSTR(session_key, INSTR(session_key, ':cron:') + 6)
              END
            ELSE NULL
          END AS cron_job_id,
          CASE
            WHEN session_key GLOB 'agent:*:cron:*:run:*' AND INSTR(session_key, ':run:') > 0
              THEN SUBSTR(session_key, INSTR(session_key, ':run:') + 5)
            ELSE NULL
          END AS cron_run_id
        FROM events
        WHERE session_key IS NOT NULL AND ts >= ? ${openIdFilter}
        GROUP BY session_key
      ),
      last_status AS (
        SELECT session_key, type, ts FROM (
          SELECT session_key, type, ts,
                 ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY ts DESC, seq DESC) AS rn
          FROM events
          WHERE category = 'hook' AND type IN (${statusPlaceholders}) AND ts >= ?
        ) WHERE rn = 1
      ),
      last_action AS (
        SELECT session_key, type, ts, tool_name, model FROM (
          SELECT session_key, type, ts, tool_name, model,
                 ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY ts DESC, seq DESC) AS rn
          FROM events
          WHERE category = 'hook' AND type IN (${actionPlaceholders}) AND ts >= ?
        ) WHERE rn = 1
      ),
      last_diag_state AS (
        SELECT session_key, ts, diag_state, diag_reason FROM (
          SELECT
            session_key,
            ts,
            json_extract(payload, '$.state') AS diag_state,
            json_extract(payload, '$.reason') AS diag_reason,
            ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY ts DESC, seq DESC) AS rn
          FROM events
          WHERE category = 'diag' AND type = 'session.state' AND ts >= ?
        ) WHERE rn = 1
      ),
      first_msg AS (
        SELECT session_key, payload FROM (
          SELECT session_key, payload,
                 ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY ts ASC, seq ASC) AS rn
          FROM events
          WHERE category = 'hook' AND type = 'message_received' AND ts >= ?
        ) WHERE rn = 1
      )
      SELECT
        base.session_key,
        base.first_seen, base.last_seen, base.event_count,
        base.t_in, base.t_out, base.t_cr, base.t_cw, base.t_tot,
        base.parent_session_key, base.agent_id, base.channel,
        base.group_chat_id, ga.group_name,
        base.cron_job_id, base.cron_run_id, cja.job_name AS cron_job_name,
        last_status.type   AS status_type,
        last_action.type   AS action_type,
        last_action.ts     AS action_ts,
        last_action.tool_name AS action_tool,
        last_action.model     AS action_model,
        last_diag_state.diag_state,
        last_diag_state.diag_reason,
        last_diag_state.ts AS diag_ts,
        first_msg.payload AS first_msg_payload
      FROM base
      LEFT JOIN group_alias ga ON ga.chat_id = base.group_chat_id
      LEFT JOIN cron_job_alias cja ON cja.job_id = base.cron_job_id
      LEFT JOIN last_status ON last_status.session_key = base.session_key
      LEFT JOIN last_action ON last_action.session_key = base.session_key
      LEFT JOIN last_diag_state ON last_diag_state.session_key = base.session_key
      LEFT JOIN first_msg   ON first_msg.session_key   = base.session_key
      ORDER BY base.last_seen DESC
      LIMIT ?
    `;

    try {
      const args: unknown[] = [
        sinceTs,
        ...(openId ? [openId] : []),
        ...STATUS_TYPES,
        sinceTs,
        ...ACTION_TYPES,
        sinceTs,
        sinceTs,
        sinceTs,
        limit,
      ];
      const rows = this.db
        .prepare(sql)
        .all(...args) as SessionRow[];
      return rows.map(rowToSessionState);
    } catch (err) {
      this.logger.warn(`[observer] querySessions failed: ${errMsg(err)}`);
      return [];
    }
  }

  listKnownGroups(opts: { limit?: number } = {}): KnownGroup[] {
    if (!this.ready || !this.db) return [];
    const limit = clampInt(opts.limit, 1, 1000, 200);
    const sql = `
      SELECT
        chat_id AS chat_id,
        COALESCE(group_name, chat_id) AS group_name,
        source AS source,
        updated_at AS updated_at
      FROM group_alias
      ORDER BY updated_at DESC, chat_id ASC
      LIMIT ?
    `;
    try {
      const rows = this.db.prepare(sql).all(limit) as KnownGroupRow[];
      return rows.map((r) => ({
        chatId: r.chat_id,
        groupName: r.group_name,
        source: r.source,
        updatedAt: r.updated_at,
      }));
    } catch (err) {
      this.logger.warn(`[observer] listKnownGroups failed: ${errMsg(err)}`);
      return [];
    }
  }

  listKnownCronJobs(opts: { limit?: number } = {}): KnownCronJob[] {
    if (!this.ready || !this.db) return [];
    const limit = clampInt(opts.limit, 1, 1000, 200);
    const sql = `
      SELECT
        job_id AS job_id,
        job_name AS job_name,
        enabled AS enabled,
        source AS source,
        updated_at AS updated_at
      FROM cron_job_alias
      ORDER BY updated_at DESC, job_id ASC
      LIMIT ?
    `;
    try {
      const rows = this.db.prepare(sql).all(limit) as KnownCronJobRow[];
      return rows.map((r) => ({
        jobId: r.job_id,
        jobName: r.job_name,
        enabled: r.enabled == null ? undefined : r.enabled !== 0,
        source: r.source,
        updatedAt: r.updated_at,
      }));
    } catch (err) {
      this.logger.warn(`[observer] listKnownCronJobs failed: ${errMsg(err)}`);
      return [];
    }
  }

  listKnownUsers(opts: { limit?: number } = {}): KnownUser[] {
    if (!this.ready || !this.db) return [];
    const limit = clampInt(opts.limit, 1, 1000, 200);
    const sql = `
      WITH latest_name AS (
        SELECT
          open_id,
          sender_name,
          ROW_NUMBER() OVER (PARTITION BY open_id ORDER BY ts DESC, seq DESC) AS rn
        FROM events
        WHERE open_id IS NOT NULL AND sender_name IS NOT NULL
      )
      SELECT
        e.open_id AS open_id,
        ln.sender_name AS sender_name,
        MIN(e.ts) AS first_seen,
        MAX(e.ts) AS last_seen,
        SUM(CASE WHEN e.type = 'message_received' THEN 1 ELSE 0 END) AS message_count,
        COUNT(DISTINCT e.session_key) AS session_count
      FROM events e
      LEFT JOIN latest_name ln ON ln.open_id = e.open_id AND ln.rn = 1
      WHERE e.open_id IS NOT NULL
      GROUP BY e.open_id, ln.sender_name
      ORDER BY last_seen DESC
      LIMIT ?
    `;
    try {
      const rows = this.db.prepare(sql).all(limit) as KnownUserRow[];
      return rows.map((r) => ({
        openId: r.open_id,
        senderName: r.sender_name ?? undefined,
        firstSeen: r.first_seen,
        lastSeen: r.last_seen,
        messageCount: r.message_count,
        sessionCount: r.session_count,
      }));
    } catch (err) {
      this.logger.warn(`[observer] listKnownUsers failed: ${errMsg(err)}`);
      return [];
    }
  }

  close(): void {
    if (this.closed) return;
    if (this.flushTimer) clearInterval(this.flushTimer);
    if (this.ttlTimer) clearInterval(this.ttlTimer);
    // Final drain
    try {
      this.flush();
    } catch {
      /* ignore */
    }
    this.closed = true;
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
  open_id: string | null;
  sender_name: string | null;
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
    open_id: e.openId ?? null,
    sender_name: e.senderName ?? null,
    payload: safeStringify(e.payload),
  };
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// ────────────────────────────────────────────────────────────────────────
// querySessions support: row mapping + helpers
// ────────────────────────────────────────────────────────────────────────

interface SessionRow {
  session_key: string;
  first_seen: number;
  last_seen: number;
  event_count: number;
  t_in: number;
  t_out: number;
  t_cr: number;
  t_cw: number;
  t_tot: number;
  parent_session_key: string | null;
  agent_id: string | null;
  channel: string | null;
  group_chat_id: string | null;
  group_name: string | null;
  cron_job_id: string | null;
  cron_run_id: string | null;
  cron_job_name: string | null;
  status_type: string | null;
  action_type: string | null;
  action_ts: number | null;
  action_tool: string | null;
  action_model: string | null;
  diag_state: string | null;
  diag_reason: string | null;
  diag_ts: number | null;
  first_msg_payload: string | null;
}

/** Mirror of `STATUS_FROM_HOOK` in session-tracker.ts. */
const STATUS_FROM_HOOK: Record<string, SessionStatus> = {
  session_start: "active",
  session_end: "done",
  message_received: "thinking",
  message_sent: "idle",
  llm_input: "thinking",
  llm_output: "active",
  before_tool_call: "tool",
  after_tool_call: "active",
  before_compaction: "thinking",
  after_compaction: "active",
};

function rowToSessionState(r: SessionRow): SessionState {
  const hookStatus: SessionStatus = r.status_type
    ? (STATUS_FROM_HOOK[r.status_type] ?? "active")
    : "active";
  const state: SessionState = {
    sessionKey: r.session_key,
    firstSeen: r.first_seen,
    lastSeen: r.last_seen,
    eventCount: r.event_count,
    status: hookStatus,
    tokens: {
      input: r.t_in,
      output: r.t_out,
      cacheRead: r.t_cr,
      cacheWrite: r.t_cw,
      total: r.t_tot,
    },
  };
  if (r.parent_session_key) state.parentSessionKey = r.parent_session_key;
  if (r.agent_id) state.agentId = r.agent_id;
  if (r.channel) state.channel = r.channel;
  const groupChatId = r.group_chat_id ?? parseGroupChatIdFromSessionKey(r.session_key);
  if (groupChatId) state.groupChatId = groupChatId;
  if (r.group_name && r.group_name.trim().length > 0) state.groupName = r.group_name;
  const parsedCron = parseCronSessionKey(r.session_key);
  const cronJobId = r.cron_job_id ?? parsedCron?.cronJobId;
  const cronRunId = r.cron_run_id ?? parsedCron?.cronRunId;
  if (cronJobId) state.cronJobId = cronJobId;
  if (cronRunId) state.cronRunId = cronRunId;
  if (r.cron_job_name && r.cron_job_name.trim().length > 0) state.cronJobName = r.cron_job_name;

  // currentAction / currentActionStart: replay the action_type's effect
  // exactly like session-tracker.ts does in `apply()`.
  if (r.action_type === "before_tool_call" && r.action_tool) {
    state.currentAction = `tool:${r.action_tool}`;
    if (r.action_ts != null) state.currentActionStart = r.action_ts;
  } else if (r.action_type === "llm_input") {
    state.currentAction = `llm:${r.action_model ?? ""}`;
    if (r.action_ts != null) state.currentActionStart = r.action_ts;
  }
  // after_tool_call / llm_output / session_end → leave currentAction
  // undefined (the tracker would have cleared them too).

  const diagStatus = statusFromDiagState(r.diag_state, r.diag_reason, r.action_type);
  if (diagStatus) {
    state.status = diagStatus;
    if (diagStatus === "idle" || diagStatus === "done") {
      state.currentAction = undefined;
      state.currentActionStart = undefined;
    }
  }

  if (r.first_msg_payload) {
    const title = extractTitle(r.first_msg_payload);
    if (title) state.title = title;
  }

  return state;
}

function statusFromDiagState(
  diagState: string | null,
  diagReason: string | null,
  actionType: string | null,
): SessionStatus | undefined {
  if (!diagState) return undefined;
  if (diagState === "idle") return diagReason === "run_completed" ? "done" : "idle";
  if (diagState === "processing") {
    return actionType === "before_tool_call" ? "tool" : "thinking";
  }
  if (
    diagState === "thinking" ||
    diagState === "tool" ||
    diagState === "done" ||
    diagState === "error"
  ) {
    return diagState;
  }
  if (diagState === "active" || diagState === "running") return "active";
  if (diagState === "completed") return "done";
  if (diagState === "failed") return "error";
  return undefined;
}

/** Mirror of `firstTextPreview()` in session-tracker.ts, but parses JSON first. */
function extractTitle(payloadJson: string): string | undefined {
  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(payloadJson) as Record<string, unknown>;
  } catch {
    return undefined;
  }
  for (const k of ["text", "message", "content", "prompt", "body"]) {
    const v = payload[k];
    if (typeof v === "string" && v.trim().length > 0) {
      return v.slice(0, 80);
    }
  }
  const msg = payload["message"];
  if (msg && typeof msg === "object") {
    const text = (msg as Record<string, unknown>)["text"];
    if (typeof text === "string") return text.slice(0, 80);
  }
  return undefined;
}

function clampInt(value: number | undefined, lo: number, hi: number, dflt: number): number {
  if (value == null || !Number.isFinite(value)) return dflt;
  const n = Math.floor(value);
  return Math.max(lo, Math.min(hi, n));
}

// ────────────────────────────────────────────────────────────────────────
// queryRecentEvents support: Row → ObserverEvent (mirrors http-server.ts)
// ────────────────────────────────────────────────────────────────────────

// DbEventRow is the same shape as Row — aliased here for readability
// in queryRecentEvents.
type DbEventRow = Row;

export function rowToEvent(r: DbEventRow): ObserverEvent {
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
  if (r.open_id) evt.openId = r.open_id;
  if (r.sender_name) evt.senderName = r.sender_name;
  if (Object.keys(tokens).length) evt.tokens = tokens;
  return evt;
}

// ────────────────────────────────────────────────────────────────────────
// queryTokens support: types + helpers
// ────────────────────────────────────────────────────────────────────────

export interface TokenBucket {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  total: number;
  calls: number;
}

export interface TokenWindowSnapshot {
  totals: TokenBucket;
  windowStart: number;
  windowEnd: number;
}

export interface TokenSnapshot {
  overall: TokenBucket;
  lastHour: TokenWindowSnapshot;
  lastDay: TokenWindowSnapshot;
  bySession: Record<string, TokenBucket>;
  byAgent: Record<string, TokenBucket>;
  byModel: Record<string, TokenBucket>;
}

interface TokenAggRow {
  t_in: number;
  t_out: number;
  t_cr: number;
  t_cw: number;
  t_tot: number;
  calls: number;
}

interface TokenAggKeyedRow extends TokenAggRow {
  k: string;
}

interface KnownUserRow {
  open_id: string;
  sender_name: string | null;
  first_seen: number;
  last_seen: number;
  message_count: number;
  session_count: number;
}

interface KnownGroupRow {
  chat_id: string;
  group_name: string;
  source: string;
  updated_at: number;
}

interface KnownCronJobRow {
  job_id: string;
  job_name: string;
  enabled: number | null;
  source: string;
  updated_at: number;
}

function emptyBucket(): TokenBucket {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, calls: 0 };
}

function emptyTokenSnapshot(): TokenSnapshot {
  const now = Date.now();
  return {
    overall: emptyBucket(),
    lastHour: { totals: emptyBucket(), windowStart: now - 3_600_000, windowEnd: now },
    lastDay: { totals: emptyBucket(), windowStart: now - 86_400_000, windowEnd: now },
    bySession: {},
    byAgent: {},
    byModel: {},
  };
}

function rowToBucket(r: TokenAggRow | undefined): TokenBucket {
  if (!r) return emptyBucket();
  return {
    input: r.t_in,
    output: r.t_out,
    cacheRead: r.t_cr,
    cacheWrite: r.t_cw,
    total: r.t_tot,
    calls: r.calls,
  };
}

function keyedRowsToBuckets(rows: TokenAggKeyedRow[]): Record<string, TokenBucket> {
  const out: Record<string, TokenBucket> = {};
  for (const r of rows) {
    if (r.k) out[r.k] = rowToBucket(r);
  }
  return out;
}
