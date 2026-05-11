# Changelog

All notable changes to this plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] — 2026-05-11

### Added
- `Storage.querySessions(opts)` derives the same `SessionState[]` shape
  the in-memory `SessionTracker` produces, but directly from the
  persisted `events` table via a single composite SQL query
  (window-function based). Field semantics — `firstSeen` / `lastSeen` /
  `eventCount` / `tokens.*` aggregates, `parentSessionKey` / `agentId` /
  `channel` sticky identity, `status` / `currentAction` /
  `currentActionStart` derived from the latest status-bearing hook
  event, `title` from the earliest `message_received` payload — are
  kept in lock-step with `SessionTracker.apply()`.
- `/api/sessions` now accepts `?source=bus|db`, `?limit=` (1–1000) and
  `?since=<epoch-ms>`. Response gains a `source` field describing which
  backend served the request.

### Changed
- `/api/sessions` defaults to `source=db` (was: in-memory tracker).
  After a gateway restart the dashboard now sees historical sessions
  immediately instead of going blank until new traffic arrives.
  `source=bus` and the in-memory `SessionTracker` are still available
  for live debugging and as a fallback when storage is unavailable
  (memory-only mode).

## [0.2.0] — 2026-05-11

### Changed
- **BREAKING (default behaviour):** `bindHost` default flipped from
  `127.0.0.1` to `0.0.0.0`. The dashboard now listens on all
  interfaces out of the box, so a freshly installed Observer is
  reachable from the LAN/host network without configuration. Set
  `bindHost: "127.0.0.1"` (and use SSH tunnelling) to restore the
  previous loopback-only behaviour. README "Security notes" rewritten
  to cover the new threat surface and the four recommended
  lock-down options (loopback, NIC pinning, reverse proxy, firewall).

### Fixed
- `Storage` now tracks a `ready` flag so `stats()`, `rowCount()`,
  `enqueue()`, `flush()`, `sweepExpired()` and `close()` degrade
  safely when the better-sqlite3 native binding fails to load.
  Previously a missing binding caused an uncaught
  `TypeError: Cannot read properties of undefined (reading 'get')`
  from the periodic stats timer ~30 s after startup, taking the
  whole gateway down.
- The 30-second `statsTimer` callback is wrapped in try/catch so any
  future storage hiccup is logged as a warning instead of being
  promoted to an `uncaughtException`.

## [0.1.0] — 2026-05-11

Initial public release.

### Added
- Subscribes to OpenClaw Plugin Hooks (`llm_input`, `llm_output`,
  `before_tool_call`, `after_tool_call`, `message_received`, `message_sent`,
  `session_start`, `session_end`, `subagent_spawning`, `subagent_spawned`,
  `subagent_ended`, `before_compaction`, `after_compaction`) for full content.
- Subscribes to Gateway Diagnostic Events for timing/metadata.
- In-memory ring buffer (configurable `bufferSize`, default 5000).
- SQLite storage with batch writer, WAL mode, and configurable TTL
  (`retentionDays`, default 7).
- Built-in HTTP + WebSocket server (`bindHost:port`, default
  `127.0.0.1:10010`) serving a single-page dashboard:
  - left: live session tree with token totals
  - center: real-time event stream with filters & search
  - right: token panel + selected event detail (full payload)
- Token aggregator with input / output / cache_read / cache_write / total.
- Session tracker with status (`running`, `tool`, `idle`, …) and TTL
  housekeeping.
- PII redaction layer (configurable, on by default) that masks
  `api_key`, `token`, `secret`, `Authorization`, generic `sk-…` /
  `ghp_…` patterns and truncates oversized fields.
- REST API: `/api/sessions`, `/api/events`, `/api/session/:k`,
  `/api/tokens`, plus `/stream` WebSocket.
- Idempotent `register()` so plugin hot-reload does not duplicate hooks.
