type HistoryItem = {
  id: number;
  title: string;
  prompt: string;
  enhancedPrompt?: string | null;
  tags?: string[];
  createdAt: string;
  updatedAt: string;
};

export function HistoryPanel({ items, onSelect }: { items: HistoryItem[]; onSelect: (item: HistoryItem) => void }) {
  return (
    <section className="panel history">
      <h2>History</h2>
      <div className="history-list">
        {items.map(item => (
          <button key={item.id} className="history-item" onClick={() => onSelect(item)}>
            <div className="title">{item.title}</div>
            <div className="meta">
              <span>{new Date(item.updatedAt).toLocaleString()}</span>
              {item.tags && item.tags.length > 0 && (
                <span className="tags">{item.tags.join(', ')}</span>
              )}
            </div>
          </button>
        ))}
      </div>
    </section>
  );
}
