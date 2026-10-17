import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Panel } from "../components/bits";
import { fmtSecs, scenarioName } from "../format";

type Run = Record<string, any>;

const median = (xs: number[]) => {
  const s = xs.filter((x) => x != null && !Number.isNaN(x)).sort((a, b) => a - b);
  if (!s.length) return null;
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const pct = (n: number, d: number) => (d ? `${Math.round((n / d) * 100)}%` : "–");

export function BenchPage() {
  const { data, isLoading } = useQuery({ queryKey: ["bench"], queryFn: api.bench });
  const runs: Run[] = data?.runs ?? [];
  const configs = useMemo(() => [...new Set(runs.map((r) => `${r.mode ?? "batch"}|${r.label}`))], [runs]);
  const [pick, setPick] = useState<string | null>(null);
  const current = pick ?? configs[0] ?? null;
  const view = runs.filter((r) => `${r.mode ?? "batch"}|${r.label}` === current);
  const stream = (current ?? "").startsWith("stream|");

  const byScenario = useMemo(() => {
    const m = new Map<string, Run[]>();
    view.forEach((r) => m.set(r.scenario, [...(m.get(r.scenario) ?? []), r]));
    return [...m.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [view]);

  const repairs = view.filter((r) => r.expected_outcome === "repair");
  const okRepairs = repairs.filter((r) => r.success);

  if (isLoading) return <p className="text-ink-2">Loading results</p>;
  if (!runs.length) {
    return (
      <Panel title="Benchmark">
        <p className="max-w-[70ch] text-[14.5px] text-ink-2">
          No results yet. Run <code className="rounded bg-panel-2 px-1">python -m bench.stream_harness --runs 3</code> to
          break the live stream on purpose across every scenario and measure detection, repair and recovery,
          or <code className="rounded bg-panel-2 px-1">python -m bench.harness</code> for the batch version.
          Add <code className="rounded bg-panel-2 px-1">--oracle</code> to check the plumbing without an API key.
        </p>
      </Panel>
    );
  }

  return (
    <div className="space-y-5">
      <Panel title="Benchmark"
             actions={
               <label className="flex items-center gap-2 text-[14px] text-ink-2">
                 Configuration
                 <select value={current ?? ""} onChange={(e) => setPick(e.target.value)}
                         className="rounded-md border border-line-strong bg-panel px-2 py-1 text-ink">
                   {configs.map((c) => { const [mode, label] = c.split("|"); return <option key={c} value={c}>{label} ({mode})</option>; })}
                 </select>
               </label>}>
        {current?.includes("|oracle") && (
          <p className="mb-3 text-[13.5px] text-quarantine-ink">Oracle runs use a scripted stand-in for the model. They test the harness, not a model.</p>
        )}
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-5">
          {[
            ["Runs", String(view.length)],
            ["Succeeded", pct(view.filter((r) => r.success).length, view.length)],
            ["Median time to verified PR", fmtSecs(median(okRepairs.map((r) => r.mttr_s)))],
            ["False fixes", pct(repairs.filter((r) => r.false_fix).length, repairs.length)],
            stream ? ["Median detection", fmtSecs(median(view.map((r) => r.time_to_detect_s)))]
                   : ["Model cost per repair", okRepairs.length ? `$${(okRepairs.reduce((a, r) => a + (r.llm_cost_usd ?? 0), 0) / okRepairs.length).toFixed(4)}` : "–"],
          ].map(([l, v]) => (
            <div key={l}><div className="text-[13px] text-ink-3">{l}</div><div className="font-cond text-[24px] font-semibold">{v}</div></div>
          ))}
        </div>
      </Panel>

      <Panel title="By scenario">
        <div className="-mx-1 overflow-x-auto">
          <table className="num w-full min-w-[640px] text-[14px]">
            <thead className="text-left text-[13px] text-ink-3">
              <tr>
                <th className="px-1 pb-1.5 font-medium">Scenario</th><th className="px-1 font-medium">Succeeded</th>
                <th className="px-1 font-medium">False fixes</th><th className="px-1 font-medium">Attempts</th>
                {stream && <th className="px-1 font-medium">Detected in</th>}
                <th className="px-1 font-medium">Verified PR in</th>
                {stream && <th className="px-1 font-medium">Records recovered</th>}
              </tr>
            </thead>
            <tbody>
              {byScenario.map(([sc, rs]) => {
                const ok = rs.filter((r) => r.success);
                const q = rs.reduce((a, r) => a + (r.quarantined ?? 0), 0);
                const rec = rs.reduce((a, r) => a + (r.recovered ?? 0), 0);
                return (
                  <tr key={sc} className="border-t border-line">
                    <td className="px-1 py-1.5 font-sans">{scenarioName(sc)}</td>
                    <td className="px-1">{ok.length} of {rs.length}</td>
                    <td className="px-1">{rs.filter((r) => r.false_fix).length}</td>
                    <td className="px-1">{(rs.reduce((a, r) => a + (r.attempts ?? 0), 0) / rs.length).toFixed(1)}</td>
                    {stream && <td className="px-1">{fmtSecs(median(rs.map((r) => r.time_to_detect_s)))}</td>}
                    <td className="px-1">{rs[0].expected_outcome === "repair" ? fmtSecs(median(ok.map((r) => r.mttr_s))) : "–"}</td>
                    {stream && <td className="px-1">{q ? `${rec} of ${q}` : "–"}</td>}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
