/**
 * Subscribe to OpenClaw Diagnostic Events (35+ metadata event types such as
 * `model.call.started`, `model.usage`, `run.started`, `tool.execution.*`,
 * `queue.lane.*`, `session.state`, …).
 *
 * These events carry timing + structural metadata; they do NOT include raw
 * prompt / response / tool content (that flows through Plugin Hooks).
 *
 * The subscription is a no-op if the runtime does not expose
 * `onDiagnosticEvent` (older OpenClaw builds); we fail gracefully.
 */

import type { EventBus } from "./event-bus.js";
import type { ObserverEvent, ObserverTokens } from "./types.js";
import { now, pickNumber, pickObject, pickString } from "./util.js";

/** Unsubscribe function returned by the runtime. */
type Unsubscribe = () => void;

/**
 * Attempt to dynamically load the diagnostic-runtime module from the host
 * OpenClaw install. Returns null when unavailable (plugin still works).
 */
async function loadDiagnosticRuntime(): Promise<
  ((listener: (evt: unknown) => void) => Unsubscribe) | null
> {
  try {
    const mod = (await import("openclaw/plugin-sdk")) as unknown as Record<
      string,
      unknown
    >;
    const fn = mod["onDiagnosticEvent"];
    if (typeof fn === "function") {
      return fn as (listener: (evt: unknown) => void) => Unsubscribe;
    }
  } catch {
    // fall through to secondary probe
  }

  // Secondary probe — some builds expose it on a subpath.
  try {
    const mod = (await import("openclaw/plugin-sdk/diagnostic-events" as string)) as Record<
      string,
      unknown
    >;
    const fn = mod["onDiagnosticEvent"];
    if (typeof fn === "function") {
      return fn as (listener: (evt: unknown) => void) => Unsubscribe;
    }
  } catch {
    // not available
  }

  return null;
}

export async function subscribeDiagnosticEvents(
  bus: EventBus,
  logger: { info: (msg: string) => void; warn: (msg: string) => void },
): Promise<Unsubscribe | null> {
  const onDiag = await loadDiagnosticRuntime();
  if (!onDiag) {
    logger.warn(
      "[observer] onDiagnosticEvent not exposed by this OpenClaw build; diagnostic stream disabled",
    );
    return null;
  }

  const unsubscribe = onDiag((raw) => {
    try {
      const evt = normalizeDiag(raw);
      if (evt) bus.push(evt);
    } catch {
      // swallow
    }
  });

  logger.info("[observer] diagnostic events subscribed");
  return unsubscribe;
}

// ────────────────────────────────────────────────────────────────────────

function normalizeDiag(raw: unknown): Omit<ObserverEvent, "id" | "seq"> | null {
  if (!raw || typeof raw !== "object") return null;
  const src = raw as Record<string, unknown>;
  const type = pickString(src, "type");
  if (!type) return null;

  const trace = pickObject(src, "trace");
  const usage = pickObject(src, "usage");

  return {
    ts: pickNumber(src, "ts") ?? now(),
    category: "diag",
    type,
    runId: pickString(src, "runId"),
    sessionKey: pickString(src, "sessionKey"),
    sessionId: pickString(src, "sessionId"),
    agentId: pickString(src, "agentId"),
    channel: pickString(src, "channel"),
    traceId: pickString(trace, "traceId"),
    provider: pickString(src, "provider"),
    model: pickString(src, "model"),
    toolName: pickString(src, "toolName"),
    toolCallId: pickString(src, "toolCallId"),
    durationMs: pickNumber(src, "durationMs"),
    tokens: extractTokens(usage),
    payload: { ...src },
  };
}

function extractTokens(usage: Record<string, unknown> | undefined): ObserverTokens | undefined {
  if (!usage) return undefined;
  const tokens: ObserverTokens = {};
  const i = pickNumber(usage, "input");
  const o = pickNumber(usage, "output");
  const cr = pickNumber(usage, "cacheRead");
  const cw = pickNumber(usage, "cacheWrite");
  const t = pickNumber(usage, "total");
  if (i != null) tokens.input = i;
  if (o != null) tokens.output = o;
  if (cr != null) tokens.cacheRead = cr;
  if (cw != null) tokens.cacheWrite = cw;
  if (t != null) tokens.total = t;
  return Object.keys(tokens).length ? tokens : undefined;
}
