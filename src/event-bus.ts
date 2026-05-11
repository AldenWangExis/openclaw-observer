/**
 * In-memory ring buffer + pub/sub for ObserverEvents.
 *
 * Hot path: `push()` must be O(1) and never throw. All subscriber errors
 * are swallowed so that one broken listener cannot stall the stream.
 *
 * The buffer is bounded; when it fills, the oldest event is dropped.
 * Subscribers that need history should read `recent()` at subscribe time.
 */

import type { ObserverEvent, ObserverEventListener } from "./types.js";
import { uuid } from "./util.js";

export interface EventBusOptions {
  /** Max events retained in memory for fast replay on new subscribers. */
  bufferSize: number;
}

export class EventBus {
  private readonly bufferSize: number;
  private buffer: ObserverEvent[] = [];
  private subscribers = new Set<ObserverEventListener>();
  private seq = 0;

  constructor(opts: EventBusOptions) {
    this.bufferSize = Math.max(100, opts.bufferSize);
  }

  /**
   * Push an event. Fills in `id`, `seq`, and `ts` if not supplied.
   * Safe to call from hook handlers — never throws.
   */
  push(
    evt: Omit<ObserverEvent, "id" | "seq"> & Partial<Pick<ObserverEvent, "id" | "seq">>,
  ): ObserverEvent {
    const full: ObserverEvent = {
      ...evt,
      id: evt.id ?? uuid(),
      seq: ++this.seq,
    };

    this.buffer.push(full);
    if (this.buffer.length > this.bufferSize) {
      this.buffer.shift();
    }

    for (const sub of this.subscribers) {
      try {
        sub(full);
      } catch {
        // swallow: one broken subscriber must not kill the bus
      }
    }

    return full;
  }

  /** Subscribe to all future events. Returns an unsubscribe function. */
  subscribe(listener: ObserverEventListener): () => void {
    this.subscribers.add(listener);
    return () => this.subscribers.delete(listener);
  }

  /** Return the most recent N events (newest last). */
  recent(n = 200): ObserverEvent[] {
    if (n >= this.buffer.length) return this.buffer.slice();
    return this.buffer.slice(this.buffer.length - n);
  }

  /** Observability: current buffer size / subscriber count. */
  stats(): { bufferedEvents: number; subscribers: number; totalSeq: number } {
    return {
      bufferedEvents: this.buffer.length,
      subscribers: this.subscribers.size,
      totalSeq: this.seq,
    };
  }

  /** Clear buffer (used by tests only). */
  reset(): void {
    this.buffer = [];
    this.seq = 0;
    // Intentionally do not clear subscribers.
  }
}
