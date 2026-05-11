/**
 * Subscribe to OpenClaw Plugin Hooks and normalize each event into the
 * shape expected by `EventBus`.
 *
 * Design notes:
 * - Every handler is wrapped in try/catch; hook failures must not break
 *   the agent loop or the bus.
 * - We intentionally subscribe to observation-only hooks; we never
 *   return decisions (no block / no rewrite).
 * - Heavy payloads are passed through verbatim here — redaction and
 *   truncation happen in a later stage (M7) before storage/UI.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

import type { EventBus } from "./event-bus.js";
import type { ObserverEvent, ObserverTokens } from "./types.js";
import { now, pickNumber, pickObject, pickString } from "./util.js";

/** Hooks we care about. */
const OBSERVED_HOOKS = [
  "session_start",
  "session_end",
  "message_received",
  "message_sent",
  "llm_input",
  "llm_output",
  "before_tool_call",
  "after_tool_call",
  "subagent_spawning",
  "subagent_spawned",
  "subagent_ended",
  "before_compaction",
  "after_compaction",
] as const;

type HookName = (typeof OBSERVED_HOOKS)[number];

interface HookRegistrationReport {
  /** Names of hooks successfully registered. */
  registered: HookName[];
  /** Hook names that threw when registering (kept for visibility). */
  failed: { hook: HookName; error: string }[];
}

/**
 * Register all observer hooks on the given OpenClaw API instance.
 *
 * @returns report of which hooks succeeded / failed at registration time.
 */
export function registerObserverHooks(
  api: OpenClawPluginApi,
  bus: EventBus,
  logger: { debug: (msg: string) => void; warn: (msg: string) => void },
): HookRegistrationReport {
  const report: HookRegistrationReport = { registered: [], failed: [] };

  for (const hook of OBSERVED_HOOKS) {
    try {
      api.on(hook, async (event: unknown, ctx: unknown) => {
        try {
          const evt = normalize(hook, event, ctx);
          bus.push(evt);
        } catch (err) {
          // never let a handler exception escape
          logger.warn(`[observer] hook handler failed (${hook}): ${errMsg(err)}`);
        }
      });
      report.registered.push(hook);
    } catch (err) {
      report.failed.push({ hook, error: errMsg(err) });
    }
  }

  return report;
}

// ────────────────────────────────────────────────────────────────────────
// Normalization: Hook event + ctx  →  ObserverEvent
// ────────────────────────────────────────────────────────────────────────

function normalize(
  hook: HookName,
  rawEvent: unknown,
  rawCtx: unknown,
): Omit<ObserverEvent, "id" | "seq"> {
  const event = (rawEvent && typeof rawEvent === "object"
    ? (rawEvent as Record<string, unknown>)
    : {}) as Record<string, unknown>;
  const ctx = (rawCtx && typeof rawCtx === "object"
    ? (rawCtx as Record<string, unknown>)
    : {}) as Record<string, unknown>;

  const base: Omit<ObserverEvent, "id" | "seq"> = {
    ts: now(),
    category: "hook",
    type: hook,
    runId: pickString(event, "runId") ?? pickString(ctx, "runId"),
    sessionKey: pickString(event, "sessionKey") ?? pickString(ctx, "sessionKey"),
    sessionId: pickString(event, "sessionId") ?? pickString(ctx, "sessionId"),
    agentId: pickString(ctx, "agentId"),
    channel: pickString(ctx, "channelId") ?? pickString(ctx, "channel"),
    traceId: extractTraceId(event, ctx),
    payload: { ...event, __ctx: slimCtx(ctx) },
  };

  // hook-specific enrichment
  switch (hook) {
    case "llm_input":
      base.provider = pickString(event, "provider");
      base.model = pickString(event, "model");
      break;
    case "llm_output": {
      base.provider = pickString(event, "provider");
      base.model = pickString(event, "model");
      base.tokens = extractTokens(pickObject(event, "usage"));
      break;
    }
    case "before_tool_call":
      base.toolName = pickString(event, "toolName");
      base.toolCallId = pickString(event, "toolCallId");
      base.toolStatus = "started";
      break;
    case "after_tool_call":
      base.toolName = pickString(event, "toolName");
      base.toolCallId = pickString(event, "toolCallId");
      base.durationMs = pickNumber(event, "durationMs");
      base.toolStatus = typeof event["error"] === "string" ? "error" : "completed";
      break;
    case "subagent_spawning":
    case "subagent_spawned":
    case "subagent_ended":
      base.parentSessionKey = pickString(event, "parentSessionKey");
      break;
    default:
      break;
  }

  return base;
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

function extractTraceId(
  event: Record<string, unknown>,
  ctx: Record<string, unknown>,
): string | undefined {
  const fromCtx = pickObject(ctx, "trace");
  const fromEvt = pickObject(event, "trace");
  return pickString(fromCtx, "traceId") ?? pickString(fromEvt, "traceId");
}

/** Strip down ctx to small, safe fields — full ctx is usually huge. */
function slimCtx(ctx: Record<string, unknown>): Record<string, unknown> {
  const keep: Record<string, unknown> = {};
  for (const k of [
    "runId",
    "jobId",
    "agentId",
    "sessionKey",
    "sessionId",
    "workspaceDir",
    "modelProviderId",
    "modelId",
    "messageProvider",
    "trigger",
    "channelId",
  ]) {
    if (k in ctx) keep[k] = ctx[k];
  }
  return keep;
}

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  return String(e);
}
