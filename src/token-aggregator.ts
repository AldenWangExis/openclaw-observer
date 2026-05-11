/**
 * TokenAggregator — roll up token usage from llm_output events.
 *
 * Runs three rolling windows per (sessionKey | agentId | provider/model):
 *   - total         : lifetime of the process
 *   - lastHour      : 1-hour rolling (fixed bucket, drops the oldest when
 *                     the wall clock crosses an hour)
 *   - lastDay       : 24-hour rolling
 *
 * Keeps things simple and O(1) per event; no DB round-trip.
 */

import type { EventBus } from "./event-bus.js";
import type { ObserverEvent, ObserverTokens } from "./types.js";

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
  session: Map<string, TokenBucket>; // key: sessionKey
  model: Map<string, TokenBucket>;   // key: "provider/model"
  agent: Map<string, TokenBucket>;   // key: agentId
  overall: TokenBucket;
  lastHour: TokenWindowSnapshot;
  lastDay: TokenWindowSnapshot;
}

const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;

export class TokenAggregator {
  private unsubscribe: (() => void) | null = null;

  private readonly bySession = new Map<string, TokenBucket>();
  private readonly byModel = new Map<string, TokenBucket>();
  private readonly byAgent = new Map<string, TokenBucket>();
  private overall: TokenBucket = emptyBucket();

  private hourBucket: TokenBucket = emptyBucket();
  private hourStart = Date.now();
  private dayBucket: TokenBucket = emptyBucket();
  private dayStart = Date.now();

  /** Subscribe to bus; only llm_output events contribute. */
  attach(bus: EventBus): void {
    this.unsubscribe = bus.subscribe((evt) => {
      try {
        this.apply(evt);
      } catch {
        /* fail-open */
      }
    });
  }

  detach(): void {
    this.unsubscribe?.();
  }

  snapshot(): TokenSnapshot {
    // roll windows forward if stale
    this.rollWindows(Date.now());
    return {
      session: new Map(this.bySession),
      model: new Map(this.byModel),
      agent: new Map(this.byAgent),
      overall: { ...this.overall },
      lastHour: {
        totals: { ...this.hourBucket },
        windowStart: this.hourStart,
        windowEnd: this.hourStart + HOUR_MS,
      },
      lastDay: {
        totals: { ...this.dayBucket },
        windowStart: this.dayStart,
        windowEnd: this.dayStart + DAY_MS,
      },
    };
  }

  /** Plain-JSON friendly version for HTTP endpoints. */
  snapshotAsJson(): Record<string, unknown> {
    const snap = this.snapshot();
    return {
      overall: snap.overall,
      lastHour: snap.lastHour,
      lastDay: snap.lastDay,
      bySession: Object.fromEntries(snap.session),
      byModel: Object.fromEntries(snap.model),
      byAgent: Object.fromEntries(snap.agent),
    };
  }

  // ────────────────────────────────────────────────────────────────────

  private apply(evt: ObserverEvent): void {
    if (evt.type !== "llm_output") return;
    const tokens = evt.tokens;
    if (!tokens) return;

    this.rollWindows(evt.ts);

    addInto(this.overall, tokens);
    addInto(this.hourBucket, tokens);
    addInto(this.dayBucket, tokens);

    if (evt.sessionKey) {
      addInto(upsert(this.bySession, evt.sessionKey), tokens);
    }
    const modelKey = formatModelKey(evt.provider, evt.model);
    if (modelKey) {
      addInto(upsert(this.byModel, modelKey), tokens);
    }
    if (evt.agentId) {
      addInto(upsert(this.byAgent, evt.agentId), tokens);
    }
  }

  private rollWindows(now: number): void {
    if (now - this.hourStart >= HOUR_MS) {
      this.hourBucket = emptyBucket();
      this.hourStart = now;
    }
    if (now - this.dayStart >= DAY_MS) {
      this.dayBucket = emptyBucket();
      this.dayStart = now;
    }
  }
}

// ────────────────────────────────────────────────────────────────────────

function emptyBucket(): TokenBucket {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, calls: 0 };
}

function addInto(dst: TokenBucket, src: ObserverTokens): void {
  dst.input += src.input ?? 0;
  dst.output += src.output ?? 0;
  dst.cacheRead += src.cacheRead ?? 0;
  dst.cacheWrite += src.cacheWrite ?? 0;
  dst.total += src.total ?? (src.input ?? 0) + (src.output ?? 0);
  dst.calls += 1;
}

function upsert(map: Map<string, TokenBucket>, key: string): TokenBucket {
  let b = map.get(key);
  if (!b) {
    b = emptyBucket();
    map.set(key, b);
  }
  return b;
}

function formatModelKey(provider?: string, model?: string): string | undefined {
  if (!provider && !model) return undefined;
  if (provider && model) return `${provider}/${model}`;
  return provider ?? model;
}
