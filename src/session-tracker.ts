/**
 * SessionTracker — derive per-session state machines from the event stream.
 *
 * Input: every ObserverEvent from the bus.
 * Output: a live map of sessionKey → SessionState (status, current action,
 *         token totals, first/last seen, parent link, …).
 *
 * This is all derived data; it never blocks the hot path and lives entirely
 * in memory. Sessions that are idle for too long are evicted so the map
 * cannot grow without bound.
 */

import type { EventBus } from "./event-bus.js";
import type { ObserverEvent, SessionState, SessionStatus } from "./types.js";

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

export interface SessionTrackerOptions {
  /** Max number of sessions to keep in memory. */
  maxSessions: number;
  /** Evict sessions untouched for longer than this. */
  idleTtlMs: number;
  /** Sweep cadence. */
  sweepIntervalMs: number;
}

export class SessionTracker {
  private readonly sessions = new Map<string, SessionState>();
  private unsubscribe: (() => void) | null = null;
  private sweepTimer: NodeJS.Timeout | null = null;

  constructor(private readonly opts: SessionTrackerOptions) {}

  /** Attach to the bus and start sweep timer. */
  attach(bus: EventBus): void {
    this.unsubscribe = bus.subscribe((evt) => {
      try {
        this.apply(evt);
      } catch {
        /* fail-open */
      }
    });
    this.sweepTimer = setInterval(() => this.sweep(), this.opts.sweepIntervalMs);
    this.sweepTimer.unref?.();
  }

  detach(): void {
    this.unsubscribe?.();
    if (this.sweepTimer) clearInterval(this.sweepTimer);
  }

  /** Return a shallow snapshot, newest-active first. */
  snapshot(limit?: number): SessionState[] {
    const arr = Array.from(this.sessions.values());
    arr.sort((a, b) => b.lastSeen - a.lastSeen);
    return limit ? arr.slice(0, limit) : arr;
  }

  get(sessionKey: string): SessionState | undefined {
    return this.sessions.get(sessionKey);
  }

  size(): number {
    return this.sessions.size;
  }

  // ────────────────────────────────────────────────────────────────────

  private apply(evt: ObserverEvent): void {
    const key = evt.sessionKey;
    if (!key) return;

    let st = this.sessions.get(key);
    if (!st) {
      st = this.create(key, evt);
      // Enforce cap: evict oldest idle session if over cap
      if (this.sessions.size >= this.opts.maxSessions) {
        this.evictOldest();
      }
      this.sessions.set(key, st);
    }
    st.lastSeen = evt.ts;
    st.eventCount += 1;
    if (!st.agentId && evt.agentId) st.agentId = evt.agentId;
    if (!st.channel && evt.channel) st.channel = evt.channel;
    if (!st.parentSessionKey && evt.parentSessionKey) {
      st.parentSessionKey = evt.parentSessionKey;
    }

    // Status from diagnostic session state (highest priority when present).
    // This tracks runtime truth more closely for queue transitions such as
    // message_start/message_completed where hook events may be sparse.
    if (evt.category === "diag" && evt.type === "session.state") {
      const next = statusFromDiagPayload(evt.payload, st.currentAction);
      if (next) {
        st.status = next;
        if (next === "idle" || next === "done") {
          st.currentAction = undefined;
          st.currentActionStart = undefined;
        }
      }
    }

    // Hook-derived status remains as fallback when diag session.state is absent.
    if (evt.category === "hook" && STATUS_FROM_HOOK[evt.type]) {
      const next = STATUS_FROM_HOOK[evt.type] as SessionStatus;
      st.status = next;
      if (evt.type === "before_tool_call" && evt.toolName) {
        st.currentAction = `tool:${evt.toolName}`;
        st.currentActionStart = evt.ts;
      } else if (evt.type === "after_tool_call") {
        st.currentAction = undefined;
        st.currentActionStart = undefined;
      } else if (evt.type === "llm_input") {
        st.currentAction = `llm:${evt.model ?? ""}`;
        st.currentActionStart = evt.ts;
      } else if (evt.type === "llm_output") {
        st.currentAction = undefined;
        st.currentActionStart = undefined;
      } else if (evt.type === "session_end") {
        st.currentAction = undefined;
        st.currentActionStart = undefined;
      }
    }

    // Derive title lazily from first message_received payload
    if (!st.title && evt.type === "message_received") {
      const preview = firstTextPreview(evt.payload);
      if (preview) st.title = preview;
    }
  }

  private create(key: string, evt: ObserverEvent): SessionState {
    return {
      sessionKey: key,
      parentSessionKey: evt.parentSessionKey,
      agentId: evt.agentId,
      channel: evt.channel,
      firstSeen: evt.ts,
      lastSeen: evt.ts,
      status: "active",
      tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      eventCount: 0,
    };
  }

  private sweep(): void {
    const cutoff = Date.now() - this.opts.idleTtlMs;
    for (const [key, st] of this.sessions) {
      if (st.lastSeen < cutoff && st.status !== "active" && st.status !== "thinking") {
        this.sessions.delete(key);
      }
    }
  }

  private evictOldest(): void {
    let oldestKey: string | null = null;
    let oldestTs = Infinity;
    for (const [key, st] of this.sessions) {
      if (st.lastSeen < oldestTs) {
        oldestTs = st.lastSeen;
        oldestKey = key;
      }
    }
    if (oldestKey) this.sessions.delete(oldestKey);
  }
}

function statusFromDiagPayload(
  payload: Record<string, unknown>,
  currentAction: string | undefined,
): SessionStatus | undefined {
  const state = typeof payload["state"] === "string" ? payload["state"] : undefined;
  const reason = typeof payload["reason"] === "string" ? payload["reason"] : undefined;
  if (!state) return undefined;

  // Common runtime states emitted by diagnostic session.state events.
  if (state === "idle") return reason === "run_completed" ? "done" : "idle";
  if (state === "processing") {
    return currentAction?.startsWith("tool:") ? "tool" : "thinking";
  }

  // Accept direct status-like values when runtimes expose them.
  if (state === "thinking" || state === "tool" || state === "done" || state === "error") {
    return state;
  }
  if (state === "active" || state === "running") return "active";
  if (state === "completed") return "done";
  if (state === "failed") return "error";
  return undefined;
}

// ────────────────────────────────────────────────────────────────────────

/**
 * Try to extract a short text preview from a message_received payload
 * for use as a session title. Heuristic — fail silent.
 */
function firstTextPreview(payload: Record<string, unknown>): string | undefined {
  const candidates = ["text", "message", "content", "prompt", "body"];
  for (const k of candidates) {
    const v = payload[k];
    if (typeof v === "string" && v.trim().length > 0) {
      return v.slice(0, 80);
    }
  }
  // Fall back to nested `message.text` etc.
  const msg = payload["message"];
  if (msg && typeof msg === "object") {
    const text = (msg as Record<string, unknown>)["text"];
    if (typeof text === "string") return text.slice(0, 80);
  }
  return undefined;
}
