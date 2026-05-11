# Changelog

All notable changes to this plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
