#!/usr/bin/env python3
"""Sync cleaned cron job aliases from cron/jobs.json into SQLite."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from pathlib import Path

DEFAULT_JOBS = Path("/home/admin/.openclaw/cron/jobs.json")
DEFAULT_DB = Path("/home/admin/.openclaw/extensions/openclaw-observer/data/observer.db")
SOURCE = "cron/jobs.json"

JOB_ID_RE = re.compile(r"^[A-Za-z0-9-]{6,}$")
MULTISPACE_RE = re.compile(r"\s+")


def clean_name(raw: object) -> str:
    if not isinstance(raw, str):
        return ""
    s = raw.strip().replace("`", "")
    return MULTISPACE_RE.sub(" ", s)


def parse_jobs_json(path: Path) -> tuple[list[tuple[str, str, int | None]], list[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        return [], ["jobs array missing"]

    warnings: list[str] = []
    rows: list[tuple[str, str, int | None]] = []

    for idx, job in enumerate(jobs):
        if not isinstance(job, dict):
            warnings.append(f"job[{idx}] not object, skipped")
            continue
        job_id = str(job.get("id", "")).strip()
        name = clean_name(job.get("name"))
        enabled_raw = job.get("enabled")
        enabled: int | None
        if isinstance(enabled_raw, bool):
            enabled = 1 if enabled_raw else 0
        elif enabled_raw is None:
            enabled = None
        else:
            enabled = None

        if not job_id or not JOB_ID_RE.match(job_id):
            warnings.append(f"job[{idx}] invalid id={job_id!r}, skipped")
            continue
        if not name:
            warnings.append(f"job[{idx}] empty name for id={job_id}, skipped")
            continue
        rows.append((job_id, name, enabled))

    dedup: dict[str, tuple[str, int | None]] = {}
    for job_id, name, enabled in rows:
        dedup[job_id] = (name, enabled)
    out = [(job_id, pair[0], pair[1]) for job_id, pair in dedup.items()]
    return out, warnings


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cron_job_alias (
          job_id      TEXT PRIMARY KEY,
          job_name    TEXT NOT NULL,
          enabled     INTEGER,
          source      TEXT NOT NULL DEFAULT 'derived:session_key',
          updated_at  INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cron_job_alias_updated_at ON cron_job_alias(updated_at);
        """
    )


def upsert_rows(conn: sqlite3.Connection, rows: list[tuple[str, str, int | None]]) -> tuple[int, int]:
    before = conn.execute("SELECT COUNT(*) FROM cron_job_alias").fetchone()[0]
    now_ms = int(time.time() * 1000)
    conn.executemany(
        """
        INSERT INTO cron_job_alias (job_id, job_name, enabled, source, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
          job_name = excluded.job_name,
          enabled = excluded.enabled,
          source = excluded.source,
          updated_at = excluded.updated_at
        """,
        [(job_id, name, enabled, SOURCE, now_ms) for job_id, name, enabled in rows],
    )
    after = conn.execute("SELECT COUNT(*) FROM cron_job_alias").fetchone()[0]
    return before, after


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync cron_job_alias from jobs.json")
    ap.add_argument("--jobs", type=Path, default=DEFAULT_JOBS, help="Path to cron jobs json")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to observer sqlite db")
    ap.add_argument("--dry-run", action="store_true", help="Parse/clean only, do not write db")
    args = ap.parse_args()

    if not args.jobs.exists():
        raise SystemExit(f"jobs json not found: {args.jobs}")
    if not args.db.exists() and not args.dry_run:
        raise SystemExit(f"db not found: {args.db}")

    rows, warnings = parse_jobs_json(args.jobs)
    print(f"parsed_jobs={len(rows)}")
    print(f"warnings={len(warnings)}")
    for w in warnings[:20]:
        print(f"  - {w}")

    if rows:
        print("sample_rows:")
        for job_id, name, enabled in rows[:5]:
            print(f"  {job_id} -> {name} (enabled={enabled})")
    else:
        print("nothing_to_sync=1")
        return 0

    if args.dry_run:
        print("dry_run=1 (db unchanged)")
        return 0

    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        before, after = upsert_rows(conn, rows)
        conn.commit()
    finally:
        conn.close()

    print(f"db_before={before}")
    print(f"db_after={after}")
    print(f"upserted_rows={len(rows)}")
    print("sync_done=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
