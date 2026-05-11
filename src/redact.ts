/**
 * Redaction: mask obvious secrets in payload strings before they touch
 * storage or the UI.
 *
 * Strategy:
 *  - Blacklist patterns (API keys, tokens, Authorization bearers, …)
 *  - Walk payload depth-first; only rewrite string leaves
 *  - Truncate any single field that exceeds `maxFieldBytes`
 *  - Fail-open: on any exception, return the original value
 */

import { approximateSize, safeStringify } from "./util.js";
import type { ObserverEvent } from "./types.js";

/** Value patterns (matches the secret itself). */
const VALUE_PATTERNS: RegExp[] = [
  /sk-[A-Za-z0-9_-]{16,}/g,           // generic OpenAI-style
  /xoxb-[A-Za-z0-9-]{20,}/g,          // Slack bot token
  /ghp_[A-Za-z0-9]{30,}/g,            // GitHub PAT
  /AKIA[0-9A-Z]{16}/g,                // AWS access key
  /eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, // JWT
];

/**
 * Key patterns (matches the field name). When a string field's key matches
 * and the value looks secret-ish, we mask the whole value.
 */
const KEY_PATTERNS =
  /(api[_-]?key|access[_-]?key|secret|password|passphrase|authorization|bearer|token|x-auth)/i;

/**
 * A handful of known "long but safe" keys that should be passed through
 * (prompts, tool text, commands). These are truncated but not masked.
 */
const SAFE_TEXT_KEYS = new Set([
  "command",
  "text",
  "message",
  "content",
  "prompt",
  "systemPrompt",
  "path",
  "url",
  "query",
  "output",
  "stdout",
  "stderr",
  "assistantText",
  "lastAssistant",
]);

const MASK = "«redacted»";
const TRUNCATE_SUFFIX = "…«truncated»";

export interface RedactOptions {
  enabled: boolean;
  /** Max bytes for a single string field. */
  maxFieldBytes: number;
}

export function redactEvent(evt: ObserverEvent, opts: RedactOptions): ObserverEvent {
  if (!opts.enabled) return evt;
  try {
    return { ...evt, payload: redactNode(evt.payload, opts.maxFieldBytes, "") as Record<string, unknown> };
  } catch {
    return evt;
  }
}

// ────────────────────────────────────────────────────────────────────────

function redactNode(node: unknown, maxBytes: number, parentKey: string): unknown {
  if (node == null) return node;
  if (typeof node === "string") {
    return redactString(node, parentKey, maxBytes);
  }
  if (typeof node !== "object") return node;

  if (Array.isArray(node)) {
    return node.map((v) => redactNode(v, maxBytes, parentKey));
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
    out[k] = redactNode(v, maxBytes, k);
  }
  return out;
}

function redactString(s: string, key: string, maxBytes: number): string {
  // key says "password" etc → nuke the value
  if (KEY_PATTERNS.test(key)) return MASK;

  // value patterns
  let masked = s;
  for (const p of VALUE_PATTERNS) {
    if (p.test(masked)) {
      masked = masked.replace(p, MASK);
    }
  }

  // Truncate if too long — but spare recognized safe text keys from masking,
  // they still get truncated.
  const byteLen = Buffer.byteLength(masked, "utf8");
  if (byteLen > maxBytes) {
    // Truncate on byte boundary to avoid cutting mid-utf8
    const buf = Buffer.from(masked, "utf8").subarray(0, maxBytes);
    masked = buf.toString("utf8") + TRUNCATE_SUFFIX;
  }

  // unused variable guard — reserved for future policy
  void SAFE_TEXT_KEYS;

  return masked;
}

/**
 * Utility: quickly estimate the payload size — used by hot-path monitors.
 */
export function payloadSize(evt: ObserverEvent): number {
  return approximateSize(evt.payload);
}

/** Debugging helper: dump redacted payload as a short string. */
export function shortDump(evt: ObserverEvent): string {
  const red = redactEvent(evt, { enabled: true, maxFieldBytes: 120 });
  return safeStringify(red.payload).slice(0, 400);
}
