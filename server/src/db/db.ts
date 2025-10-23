import Database from 'better-sqlite3';
import { config } from '../config/env';

export type PromptRecord = {
  id: number;
  title: string;
  prompt: string;
  enhanced_prompt: string | null;
  tags_json: string | null; // JSON string array
  created_at: string; // ISO string
  updated_at: string; // ISO string
};

export type SessionRecord = {
  id: number;
  prompt_id: number | null; // nullable for ad-hoc runs
  model: string | null;
  run_prompt: string;
  response_text: string | null;
  response_json: string | null;
  created_at: string; // ISO
};

let db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (!db) {
    db = new Database(config.databasePath);
    db.pragma('journal_mode = WAL');
    migrate(db);
  }
  return db;
}

function migrate(database: Database.Database) {
  database.exec(`
    CREATE TABLE IF NOT EXISTS prompts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      prompt TEXT NOT NULL,
      enhanced_prompt TEXT,
      tags_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      prompt_id INTEGER,
      model TEXT,
      run_prompt TEXT NOT NULL,
      response_text TEXT,
      response_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(prompt_id) REFERENCES prompts(id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_prompts_updated_at ON prompts(updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at DESC);
  `);
}

export function nowIso(): string {
  return new Date().toISOString();
}
