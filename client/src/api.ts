export const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:4000/api';

export async function apiEnhance(payload: { input: string; domain?: string; tone?: string; format?: string }) {
  const res = await fetch(`${API_BASE}/enhance`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!res.ok) throw new Error('Enhance failed');
  return res.json();
}

export async function apiRun(payload: { prompt: string; model?: string; promptId?: number }) {
  const res = await fetch(`${API_BASE}/run`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!res.ok) throw new Error('Run failed');
  return res.json();
}

export async function apiSave(payload: { title: string; prompt: string; enhancedPrompt?: string; tags?: string[] }) {
  const res = await fetch(`${API_BASE}/save`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!res.ok) throw new Error('Save failed');
  return res.json();
}

export async function apiHistory() {
  const res = await fetch(`${API_BASE}/history`);
  if (!res.ok) throw new Error('History failed');
  return res.json();
}
