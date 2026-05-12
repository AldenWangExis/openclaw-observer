# Changelog

All notable changes to this plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0-alpha.1] — 2026-05-12

Schema groundwork release for user-centric analytics. Adds SQLite-native
versioned migrations (`PRAGMA user_version`) and starts persisting `open_id`
plus `sender_name` on events. `open_id` is sourced from
`message_received.metadata.senderId` when present, with direct-message
`session_key` parsing as fallback so DM events can be correlated by user.

## [0.5.0] — 2026-05-12

Reliability hardening release. Fixes redaction correctness, bounds the
storage queue, improves shutdown/reload behavior, adds richer health and
Prometheus-style self metrics, and makes WebSocket reconnects recover short
event gaps from SQLite. Also removes the unused `captureContent` option.

## [0.4.0] — 2026-05-11

Made historical events and token usage DB-backed. The dashboard can now show
event records and token aggregates immediately after a gateway restart, with
the in-memory bus still available as a live/debug source.

## [0.3.0] — 2026-05-11

Made `/api/sessions` DB-backed so session summaries survive gateway restarts
instead of depending only on the process-local `SessionTracker`.

## [0.2.0] — 2026-05-11

Changed the default dashboard bind host to `0.0.0.0` for cross-host access,
and hardened storage initialization so a missing/broken SQLite native binding
falls back to memory-only mode instead of crashing the gateway.

## [0.1.0] — 2026-05-11

Initial release: OpenClaw hook and diagnostic-event capture, in-memory event
bus, SQLite persistence, redaction, session/token summaries, and a built-in
HTTP/WebSocket dashboard.
