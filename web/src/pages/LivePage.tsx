import { useQuery } from "@tanstack/react-query";
import { api, TERMINAL, type IncidentRow, type StoredEvent } from "../api";
import { ChaosPanel } from "../components/ChaosPanel";
import { Decision } from "../components/Decision";
import { Schematic } from "../components/Schematic";
import { StageTrack } from "../components/StageTrack";
import { Throughput } from "../components/Throughput";
import { Panel, Pill } from "../components/bits";
import { fmtAgo, fmtClock, fmtInt, fmtSecs, scenarioName, STATUS_LABEL, statusTone } from "../format";
import { useLive } from "../live";

function activityText(e: StoredEvent): string | null {
  const d = e.data;
  switch (e.kind) {
    case "drift_changed":
      return d.scenario ? `Upstream started sending: ${scenarioName(d.scenario).toLowerCase()}` : "Upstream back to its original format";
    case "code_reloaded": return `Processor switched to code v${d.version}`;
    case "code_reload_failed": return `New code failed to load, kept the previous version: ${d.error}`;
    case "schema_absorbed": {
      const added = d.diff?.added ?? [];
      const changed = Object.keys(d.diff?.type_changed ?? {});
      return `Clean records arrived in a new shape, now accepted as normal: ${[
        ...added.map((f: string) => `new field ${f}`), ...changed.map((f: string) => `${f} has a new type`)].join(", ")}`;
    }
    case "replay_done": return `Replayed ${fmtInt(d.replayed)} quarantined records: ${fmtInt(d.loaded)} loaded, ${fmtInt(d.filtered)} dropped by the fix, ${fmtInt(d.still_failing)} still failing`;
    case "producer_rate": return `Upstream rate set to ${d.rate}/s`;
    case "sink_unavailable": return `Warehouse unreachable, holding the stream: ${d.error}`;
    case "sink_recovered": return `Warehouse back after ${d.down_s} s`;
    case "runtime_started": return `Pipeline started on ${d.bus}`;
    case "detector_error": return `Detector error: ${d.error}`;
    default: return null;
  }
}

function CurrentIncident({ rows, waiting, prMode, demo = true }: { rows: IncidentRow[]; waiting: number; prMode?: string; demo?: boolean }) {
  const now = Date.now() / 1000;
  const current = rows.find((r) => !TERMINAL.has(r.status)) ?? rows.find((r) => now - r.updated_at < 120);
  const { data: inc } = useQuery({
    queryKey: ["incident", current?.id], queryFn: () => api.incident(current!.id), enabled: !!current,
  });

  if (!current) {
    return (
      <Panel>
        <p className="font-cond text-[17px] font-semibold">No open incidents</p>
        <p className="mt-0.5 text-[14px] text-ink-2">
          {demo ? "Inject a change from the list on the right and watch the agents detect it, write a fix and test it."
            : "Watching the stream. When upstream changes and records start failing the contract, the agents take it from here."}
        </p>
      </Panel>
    );
  }
  return (
    <Panel>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 max-w-[80ch]">
          <div className="flex flex-wrap items-center gap-2">
            <Pill tone={statusTone(current.status)} live={!TERMINAL.has(current.status) && current.status !== "pr_open"}>
              {STATUS_LABEL[current.status] ?? current.status}
            </Pill>
            <span className="text-[13px] text-ink-3">Opened {fmtAgo(current.created_at, now)}</span>
          </div>
          <p className="mt-2 text-[15px] text-ink">{current.summary}</p>
        </div>
        <a href={`#/incidents/${current.id}`} className="shrink-0 text-[14px] font-medium text-flow underline underline-offset-2">
          See the full incident
        </a>
      </div>
      {inc && <div className="mt-4"><StageTrack inc={inc} compact /></div>}
      {inc?.waiting_for_decision && <div className="mt-4"><Decision inc={inc} waiting={waiting} prMode={prMode} /></div>}
      {inc?.status === "error" && (
        <p className="mt-3 text-[14px] text-fault">
          The agent crashed: {[...inc.events].reverse().find((e) => e.kind === "agent_error")?.data.error ?? "see the full incident"}
        </p>
      )}
      {inc?.pr_error && (
        <p className="mt-3 text-[14px] text-quarantine-ink">
          Couldn't open a GitHub PR, so it was written locally instead. You can still approve it here. ({inc.pr_error})
        </p>
      )}
      {inc?.status === "escalated" && (
        <p className="mt-3 text-[14px] text-fault">Handed to a human: {inc.escalation_reason}</p>
      )}
    </Panel>
  );
}

export function LivePage() {
  const { status, series, events } = useLive();
  const { data: rows = [] } = useQuery({ queryKey: ["incidents"], queryFn: api.incidents, refetchInterval: 15000 });
  // shares ChaosPanel's cache: "demo" (fake upstream), "pipeline" (its own drift switch) or "none"
  const { data: chaos } = useQuery({ queryKey: ["chaos"], queryFn: api.chaos });
  const canBreak = !!chaos && chaos.mode !== "none" && chaos.scenarios.length > 0;
  const activity = events.filter((e) => !e.incident_id).map((e) => ({ e, text: activityText(e) }))
    .filter((x) => x.text).slice(-12).reverse();

  return (
    <div className="space-y-5">
      <Schematic status={status} series={series} />
      <CurrentIncident rows={rows} waiting={status?.dlq_depth ?? 0} prMode={status?.pr_mode}
        demo={canBreak} />

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1fr)_340px]">
        <div className="min-w-0 space-y-5">
          <Throughput series={series} events={events} />
          <Panel title="Incident history">
            {rows.length === 0 ? (
              <p className="text-[14px] text-ink-2">Nothing yet. Every incident the agents handle is listed here.</p>
            ) : (
              <div className="-mx-1 overflow-x-auto">
                <table className="w-full min-w-[560px] text-[14px]">
                  <thead className="text-left text-[13px] text-ink-3">
                    <tr><th className="px-1 pb-1.5 font-medium">Opened</th><th className="px-1 pb-1.5 font-medium">What happened</th>
                      <th className="px-1 pb-1.5 font-medium">Outcome</th><th className="px-1 pb-1.5 text-right font-medium">Open for</th></tr>
                  </thead>
                  <tbody>
                    {rows.slice(0, 12).map((r) => (
                      <tr key={r.id} className="border-t border-line align-top hover:bg-panel-2">
                        <td className="num whitespace-nowrap px-1 py-2 text-ink-2">{fmtClock(r.created_at)}</td>
                        <td className="px-1 py-2">
                          <a href={`#/incidents/${r.id}`} className="line-clamp-2 text-ink hover:underline">{r.summary}</a>
                          {r.scenario && <span className="text-[12.5px] text-ink-3">Injected: {scenarioName(r.scenario).toLowerCase()}</span>}
                        </td>
                        <td className="whitespace-nowrap px-1 py-2"><Pill tone={statusTone(r.status)}>{STATUS_LABEL[r.status] ?? r.status}</Pill></td>
                        <td className="num whitespace-nowrap px-1 py-2 text-right text-ink-2">{fmtSecs(r.updated_at - r.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </div>
        <div className="space-y-5">
          {/* breaking the upstream needs the demo producer or the pipeline's own drift switch;
              a real topic with neither isn't ours to break */}
          {canBreak && <ChaosPanel status={status} />}
          <Panel title="Pipeline log">
            {activity.length === 0 ? <p className="text-[14px] text-ink-2">Quiet so far.</p> : (
              <ul className="space-y-2">
                {activity.map(({ e, text }) => (
                  <li key={e.id} className="text-[13.5px] leading-snug">
                    <span className="num mr-2 text-ink-3">{fmtClock(e.ts)}</span>
                    <span className="text-ink-2">{text}</span>
                  </li>
                ))}
              </ul>
            )}
          </Panel>
        </div>
      </div>
    </div>
  );
}
