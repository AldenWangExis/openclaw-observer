/**
 * Small utilities: id generation, time formatting, safe-json, size helpers.
 * Pure functions only — no side effects.
 */

import { randomUUID } from "node:crypto";

/** Generate a random UUID v4. */
export function uuid(): string {
  return randomUUID();
}

/**
 * JSON stringify that never throws.
 * Falls back to `String(value)` for circular refs or unsupported types.
 */
export function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    try {
      return JSON.stringify(value, circularReplacer());
    } catch {
      return String(value);
    }
  }
}

/** Replacer that replaces circular refs with "[Circular]". */
function circularReplacer() {
  const seen = new WeakSet();
  return function (_key: string, v: unknown) {
    if (typeof v === "object" && v !== null) {
      if (seen.has(v as object)) return "[Circular]";
      seen.add(v as object);
    }
    return v;
  };
}

/** Return approximate byte size of a JSON-serializable value. */
export function approximateSize(value: unknown): number {
  if (value == null) return 0;
  if (typeof value === "string") return Buffer.byteLength(value, "utf8");
  return Buffer.byteLength(safeStringify(value), "utf8");
}

/** Current epoch ms. */
export function now(): number {
  return Date.now();
}

/** Try to read a string field; return undefined if not a string. */
export function pickString(obj: unknown, key: string): string | undefined {
  if (!obj || typeof obj !== "object") return undefined;
  const v = (obj as Record<string, unknown>)[key];
  return typeof v === "string" ? v : undefined;
}

/** Try to read a number field. */
export function pickNumber(obj: unknown, key: string): number | undefined {
  if (!obj || typeof obj !== "object") return undefined;
  const v = (obj as Record<string, unknown>)[key];
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

/** Try to read a nested object field. */
export function pickObject(
  obj: unknown,
  key: string,
): Record<string, unknown> | undefined {
  if (!obj || typeof obj !== "object") return undefined;
  const v = (obj as Record<string, unknown>)[key];
  return v && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : undefined;
}

/**
 * Parse open_id from direct-message session key:
 *   agent:<agent>:<channel>:direct:ou_xxx
 */
export function parseOpenIdFromDmSessionKey(sessionKey: string | undefined): string | undefined {
  if (!sessionKey) return undefined;
  const m = sessionKey.match(/^agent:[^:]+:[^:]+:direct:(ou_[A-Za-z0-9]+)$/);
  return m?.[1];
}

/**
 * Parse group chat_id from group session key:
 *   agent:<agent>:<channel>:group:oc_xxx
 */
export function parseGroupChatIdFromSessionKey(sessionKey: string | undefined): string | undefined {
  if (!sessionKey) return undefined;
  const m = sessionKey.match(/^agent:[^:]+:[^:]+:group:(oc_[A-Za-z0-9]+)$/);
  return m?.[1];
}
