import type { Database as SqliteDatabase } from "better-sqlite3";

export interface MigrationLogger {
  info: (msg: string) => void;
  error: (msg: string) => void;
}

interface Migration {
  version: number;
  name: string;
  up: (db: SqliteDatabase) => void;
}

const MIGRATIONS: Migration[] = [
  {
    version: 1,
    name: "add_open_id_and_sender_name",
    up: (db) => {
      if (!columnExists(db, "events", "open_id")) {
        db.exec(`ALTER TABLE events ADD COLUMN open_id TEXT;`);
      }
      if (!columnExists(db, "events", "sender_name")) {
        db.exec(`ALTER TABLE events ADD COLUMN sender_name TEXT;`);
      }
      db.exec(`CREATE INDEX IF NOT EXISTS idx_events_open_ts ON events(open_id, ts);`);

      db.exec(`
        UPDATE events
        SET
          open_id = json_extract(payload, '$.metadata.senderId'),
          sender_name = json_extract(payload, '$.metadata.senderName')
        WHERE type = 'message_received'
          AND json_extract(payload, '$.metadata.senderId') IS NOT NULL;
      `);

      db.exec(`
        UPDATE events
        SET open_id = SUBSTR(session_key, INSTR(session_key, ':direct:') + 8)
        WHERE open_id IS NULL
          AND session_key GLOB 'agent:*:direct:ou_*';
      `);
    },
  },
];

export function runMigrations(db: SqliteDatabase, logger: MigrationLogger): void {
  const currentVersion = Number(db.pragma("user_version", { simple: true }) ?? 0);
  const pending = MIGRATIONS.filter((m) => m.version > currentVersion).sort(
    (a, b) => a.version - b.version,
  );
  if (pending.length === 0) {
    return;
  }

  for (const migration of pending) {
    const tx = db.transaction(() => {
      migration.up(db);
      db.pragma(`user_version = ${migration.version}`);
    });
    try {
      tx();
      logger.info(`[observer] migration applied: v${migration.version} ${migration.name}`);
    } catch (err) {
      logger.error(
        `[observer] migration failed: v${migration.version} ${migration.name} · ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
      throw err;
    }
  }
}

function columnExists(db: SqliteDatabase, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((r) => r.name === column);
}
