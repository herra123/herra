export function ResponsePanel({ output }: { output: string }) {
  return (
    <section className="panel response">
      <h2>API Response</h2>
      {output ? (
        <div className="response-content">
          {output.split('\n\n').map((block, idx) => (
            <div key={idx} className="card">{block}</div>
          ))}
        </div>
      ) : (
        <p className="muted">Run the enhanced prompt to see results here.</p>
      )}
    </section>
  );
}
