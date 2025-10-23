import { Router } from 'express';
import { z } from 'zod';
import { enhancePrompt } from '../services/enhancer';
import { runCompletion } from '../services/openai';
import { getDb, nowIso, PromptRecord } from '../db/db';

const router = Router();

const enhanceSchema = z.object({
  input: z.string().min(1),
  domain: z.string().optional(),
  tone: z.string().optional(),
  format: z.string().optional()
});

router.post('/enhance', async (req, res) => {
  const parsed = enhanceSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', details: parsed.error.flatten() });
  }
  const enhanced = await enhancePrompt(parsed.data);
  res.json({ enhanced });
});

const runSchema = z.object({
  prompt: z.string().min(1),
  model: z.string().optional(),
  promptId: z.number().optional()
});

router.post('/run', async (req, res) => {
  const parsed = runSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', details: parsed.error.flatten() });
  }
  try {
    const output = await runCompletion(parsed.data.prompt, parsed.data.model);
    const db = getDb();
    const createdAt = nowIso();
    const stmt = db.prepare('INSERT INTO sessions (prompt_id, model, run_prompt, response_text, response_json, created_at) VALUES (?, ?, ?, ?, ?, ?)');
    stmt.run(parsed.data.promptId ?? null, parsed.data.model ?? null, parsed.data.prompt, output, null, createdAt);
    res.json({ output });
  } catch (err: any) {
    res.status(500).json({ error: 'OpenAI call failed', details: err?.message || String(err) });
  }
});

const saveSchema = z.object({
  title: z.string().min(1),
  prompt: z.string().min(1),
  enhancedPrompt: z.string().optional(),
  tags: z.array(z.string()).optional()
});

router.post('/save', (req, res) => {
  const parsed = saveSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', details: parsed.error.flatten() });
  }
  const db = getDb();
  const createdAt = nowIso();
  const updatedAt = createdAt;
  const stmt = db.prepare('INSERT INTO prompts (title, prompt, enhanced_prompt, tags_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)');
  const info = stmt.run(parsed.data.title, parsed.data.prompt, parsed.data.enhancedPrompt ?? null, JSON.stringify(parsed.data.tags ?? []), createdAt, updatedAt);
  res.json({ id: Number(info.lastInsertRowid) });
});

router.get('/history', (req, res) => {
  const db = getDb();
  const rows = db.prepare('SELECT id, title, prompt, enhanced_prompt, tags_json, created_at, updated_at FROM prompts ORDER BY updated_at DESC LIMIT 200').all() as PromptRecord[];
  const prompts = rows.map(r => ({
    id: r.id,
    title: r.title,
    prompt: r.prompt,
    enhancedPrompt: r.enhanced_prompt,
    tags: safeParseJsonArray(r.tags_json),
    createdAt: r.created_at,
    updatedAt: r.updated_at
  }));
  res.json({ prompts });
});

router.get('/prompts/:id', (req, res) => {
  const id = Number(req.params.id);
  const db = getDb();
  const row = db.prepare('SELECT id, title, prompt, enhanced_prompt, tags_json, created_at, updated_at FROM prompts WHERE id = ?').get(id) as PromptRecord | undefined;
  if (!row) return res.status(404).json({ error: 'Not found' });
  res.json({
    id: row.id,
    title: row.title,
    prompt: row.prompt,
    enhancedPrompt: row.enhanced_prompt,
    tags: safeParseJsonArray(row.tags_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  });
});

const updateSchema = z.object({
  title: z.string().optional(),
  prompt: z.string().optional(),
  enhancedPrompt: z.string().optional(),
  tags: z.array(z.string()).optional()
});

router.put('/prompts/:id', (req, res) => {
  const id = Number(req.params.id);
  const parsed = updateSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid request', details: parsed.error.flatten() });
  }
  const db = getDb();
  const current = db.prepare('SELECT * FROM prompts WHERE id = ?').get(id) as PromptRecord | undefined;
  if (!current) return res.status(404).json({ error: 'Not found' });

  const next = {
    title: parsed.data.title ?? current.title,
    prompt: parsed.data.prompt ?? current.prompt,
    enhanced_prompt: parsed.data.enhancedPrompt ?? current.enhanced_prompt,
    tags_json: JSON.stringify(parsed.data.tags ?? safeParseJsonArray(current.tags_json)),
    updated_at: nowIso()
  };

  db.prepare('UPDATE prompts SET title = ?, prompt = ?, enhanced_prompt = ?, tags_json = ?, updated_at = ? WHERE id = ?')
    .run(next.title, next.prompt, next.enhanced_prompt, next.tags_json, next.updated_at, id);

  res.json({ ok: true });
});

function safeParseJsonArray(value: string | null): string[] {
  if (!value) return [];
  try { return Array.isArray(JSON.parse(value)) ? JSON.parse(value) : []; } catch { return []; }
}

export default router;
