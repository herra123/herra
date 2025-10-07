export function EnhancedPreview({ enhanced }: { enhanced: string }) {
  return (
    <section className="panel preview">
      <h2>Enhanced Prompt Preview</h2>
      <pre className="codeblock">{enhanced || 'Your enhanced prompt will appear here.'}</pre>
    </section>
  );
}
