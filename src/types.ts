/**
 * Unified event type for the Observer.
 *
 * Every event (from Plugin Hooks or Diagnostic Events) is normalized into
 * an ObserverEvent before it enters the EventBus / Storage / WebSocket.
 *
 * Design: flatten high-frequency fields (tokens, toolName, durationMs, …)
 * as top-level columns so SQLite indexing and UI filters don't need to
 * parse the `payload` blob on every row.
 */

export type ObserverEventCategory = "hook" | "diag";

/** Fine-grained status of a tool call (only meaningful for tool.* events). */
export type ToolStatus = "started" | "completed" | "error" | "blocked";

export interface ObserverTokens {
  input?: number;
  output?: number;
  cacheRead?: number;
  cacheWrite?: number;
  total?: number;
}

export interface ObserverEvent {
  /** Unique id (uuid v4). */
  id: string;
  /** Epoch ms when the event was captured. */
  ts: number;
  /** Monotonically increasing within a process lifetime. */
  seq: number;

  category: ObserverEventCategory;
  /** e.g. "llm_input", "tool.execution.completed", "message.processed" */
  type: string;

  // ── correlation keys (all optional, picked best-effort from source event) ──
  runId?: string;
  sessionKey?: string;
  sessionId?: string;
  agentId?: string;
  channel?: string;
  traceId?: string;
  openId?: string;
  senderName?: string;
  /** For subagent_spawning: the parent session that spawned the child. */
  parentSessionKey?: string;

  // ── extracted high-frequency fields ──
  toolName?: string;
  toolCallId?: string;
  toolStatus?: ToolStatus;
  provider?: string;
  model?: string;
  durationMs?: number;
  tokens?: ObserverTokens;

  /**
   * Full original event payload. The UI may render this as a collapsible
   * JSON tree. Storage persists it as a JSON string column.
   *
   * May be redacted / truncated before it reaches this stage.
   */
  payload: Record<string, unknown>;
}

/** Session status machine — derived by SessionTracker, not stored per-event. */
export type SessionStatus =
  | "active"
  | "thinking"
  | "tool"
  | "idle"
  | "done"
  | "error";

export interface SessionState {
  sessionKey: string;
  parentSessionKey?: string;
  agentId?: string;
  channel?: string;
  firstSeen: number;
  lastSeen: number;
  status: SessionStatus;
  currentAction?: string;
  currentActionStart?: number;
  title?: string;
  tokens: Required<ObserverTokens>;
  eventCount: number;
}

/** A subscriber registered on the EventBus. */
export type ObserverEventListener = (evt: ObserverEvent) => void;
