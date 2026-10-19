// A unified diff, line by line, with added and removed lines tinted.
export function DiffView({ diff }: { diff: string }) {
  if (!diff) return <p className="text-[13.5px] text-ink-3">No changes in this attempt.</p>;
  const lines = diff.replace(/\n$/, "").split("\n");
  return (
    <div className="diff max-h-[460px] overflow-auto rounded-lg border border-line bg-panel-2">
      <pre className="min-w-max py-2">
        {lines.map((l, i) => {
          const cls = l.startsWith("+++") || l.startsWith("---") ? "text-ink-3 font-semibold"
            : l.startsWith("@@") ? "hunk"
            : l.startsWith("+") ? "add"
            : l.startsWith("-") ? "del" : "text-ink-2";
          return <div key={i} className={`px-3 ${cls}`}>{l || " "}</div>;
        })}
      </pre>
    </div>
  );
}
