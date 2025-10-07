import { useEffect, useMemo, useState } from 'react';
import { EnhancedPreview } from './EnhancedPreview';
import { HistoryPanel } from './HistoryPanel';
import { ResponsePanel } from './ResponsePanel';
import { TopBar } from './TopBar';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:4000/api';

type HistoryItem = {
  id: number;
  title: string;
  prompt: string;
  enhancedPrompt?: string | null;
  tags?: string[];
  createdAt: string;
  updatedAt: string;
};

export function App() {
  const [input, setInput] = useState('');
  const [domain, setDomain] = useState('');
  const [tone, setTone] = useState('');
  const [format, setFormat] = useState('');

  const [enhanced, setEnhanced] = useState('');
  const [output, setOutput] = useState('');
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [loading, setLoading] = useState(false);

  const canEnhance = useMemo(() => input.trim().length > 0, [input]);
  const canRun = useMemo(() => enhanced.trim().length > 0, [enhanced]);

  useEffect(() => {
    loadHistory();
  }, []);

  async function loadHistory() {
    const res = await fetch(`${API_BASE}/history`);
    const data = await res.json();
    setHistory(data.prompts as HistoryItem[]);
  }

  async function onEnhance() {
    if (!canEnhance) return;
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/enhance`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ input, domain: emptyToUndef(domain), tone: emptyToUndef(tone), format: emptyToUndef(format) })
      });
      const data = await res.json();
      setEnhanced(data.enhanced || '');
    } finally {
      setLoading(false);
    }
  }

  async function onRun() {
    if (!canRun) return;
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: enhanced })
      });
      const data = await res.json();
      setOutput(data.output || '');
    } finally {
      setLoading(false);
    }
  }

  async function onSave() {
    if (!enhanced.trim()) return;
    const title = prompt('Enter a title for this prompt:') || new Date().toLocaleString();
    const tagsInput = prompt('Optional tags (comma-separated):') || '';
    const tags = tagsInput.split(',').map(t => t.trim()).filter(Boolean);

    await fetch(`${API_BASE}/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, prompt: input, enhancedPrompt: enhanced, tags })
    });
    await loadHistory();
  }

  return (
    <div className="container">
      <TopBar />
      <div className="layout">
        <section className="panel input">
          <h2>Input Box</h2>
          <textarea
            placeholder="Describe your goal... e.g., Write a social media post about AI in healthcare."
            value={input}
            onChange={e => setInput(e.target.value)}
          />

          <div className="fields">
            <input placeholder="Domain (writing, coding, marketing...)" value={domain} onChange={e => setDomain(e.target.value)} />
            <input placeholder="Tone (professional, friendly, persuasive...)" value={tone} onChange={e => setTone(e.target.value)} />
            <input placeholder="Format (sections, numbered list...)" value={format} onChange={e => setFormat(e.target.value)} />
          </div>

          <div className="actions">
            <button onClick={onEnhance} disabled={!canEnhance || loading}>Enhance</button>
            <button onClick={onEnhance} disabled={!canEnhance || loading}>Generate</button>
            <button onClick={onRun} disabled={!canRun || loading}>Run</button>
            <button onClick={onSave} disabled={!canRun || loading}>Save</button>
            <button onClick={loadHistory}>History</button>
          </div>
        </section>

        <EnhancedPreview enhanced={enhanced} />
        <ResponsePanel output={output} />
        <HistoryPanel items={history} onSelect={item => {
          setInput(item.prompt);
          setEnhanced(item.enhancedPrompt || '');
        }} />
      </div>
    </div>
  );
}

function emptyToUndef(v: string) { return v.trim() ? v.trim() : undefined; }
