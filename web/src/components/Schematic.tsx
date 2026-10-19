// The plant diagram: upstream feeds the processor, which feeds the warehouse or
// diverts into the quarantine tank. Pipe speed follows the live rate; the tank's
// level follows how many records are waiting to be replayed.

import { useMemo, useState } from "react";
import { api, type Point, type Status } from "../api";
import { fmtInt, scenarioName } from "../format";

function ratePerSec(series: Point[], key: keyof Point, secs = 5): number {
  const recent = series.slice(-secs - 1, -1);   // skip the second still being filled
  if (!recent.length) return 0;
  return recent.reduce((a, p) => a + (p[key] as number), 0) / recent.length;
}

function Pipe({ rate, tone, vertical = false, label }: {
  rate: number; tone: "flow" | "quarantine"; vertical?: boolean; label?: string;
}) {
  const active = rate > 0.05;
  const color = tone === "flow" ? "var(--flow)" : "var(--quarantine)";
  // faster flow, shorter cycle; clamped so it never strobes
  const dur = active ? Math.max(0.35, Math.min(2.4, 12 / rate)) : 0;
  return (
    <div className={`relative flex items-center justify-center ${vertical ? "h-14 w-full flex-col" : "h-full min-h-10 w-full"}`}>
      <svg className={vertical ? "h-full w-4" : "h-4 w-full"} preserveAspectRatio="none"
           viewBox={vertical ? "0 0 16 100" : "0 0 100 16"} aria-hidden>
        {vertical ? (
          <>
            <line x1="8" y1="0" x2="8" y2="100" stroke="var(--pipe)" strokeWidth="8" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
            {active && <line x1="8" y1="0" x2="8" y2="100" stroke={color} strokeWidth="3" vectorEffect="non-scaling-stroke"
                              className="pipe-flow" style={{ animationDuration: `${dur}s` }} />}
          </>
        ) : (
          <>
            <line x1="0" y1="8" x2="100" y2="8" stroke="var(--pipe)" strokeWidth="8" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
            {active && <line x1="0" y1="8" x2="100" y2="8" stroke={color} strokeWidth="3" vectorEffect="non-scaling-stroke"
                              className="pipe-flow" style={{ animationDuration: `${dur}s` }} />}
          </>
        )}
      </svg>
      {label && (
        <span className={`num absolute whitespace-nowrap rounded bg-panel px-1.5 text-[13px] font-medium ${
          tone === "quarantine" && active ? "text-quarantine-ink" : "text-ink-2"} ${
          vertical ? "left-1/2 ml-4" : "top-1/2 -mt-7"}`}>
          {label}
        </span>
      )}
    </div>
  );
}

function Station({ title, children, tone = "line" }: { title: string; children: React.ReactNode; tone?: "line" | "fault" | "agent" }) {
  const border = tone === "fault" ? "border-fault" : tone === "agent" ? "border-agent" : "border-line-strong";
  return (
    <div className={`rounded-lg border-2 ${border} bg-panel px-4 py-3`}>
      <div className="font-cond text-[15px] font-semibold text-ink">{title}</div>
      <div className="mt-1 text-[13.5px] leading-snug text-ink-2">{children}</div>
    </div>
  );
}

export function Schematic({ status, series }: { status: Status | null; series: Point[] }) {
  const [replaying, setReplaying] = useState(false);
  const [replayMsg, setReplayMsg] = useState<string | null>(null);
  const inRate = ratePerSec(series, "in");
  const loadRate = ratePerSec(series, "loaded");
  const dlqRate = ratePerSec(series, "dlq");
  const depth = status?.dlq_depth ?? 0;
  // log scale: 1 record shows, a few thousand fills the tank
  const level = depth <= 0 ? 0 : Math.min(1, Math.log10(depth + 1) / Math.log10(3000));
  const drift = status?.drift.scenario;
  const silent = status?.firing.includes("silence:upstream");
  // the demo talks about orders; any other pipeline is named after its own topic and sink
  const demo = !status?.pipeline || status.pipeline === "orders";
  const sinkTable = status?.sink?.split(":").slice(1).join(":");

  const upstreamNote = useMemo(() => {
    if (silent) return <span className="text-fault">Silent for {Math.round(status?.last_record_age_s ?? 0)} s</span>;
    if (drift) return <span className="text-quarantine-ink">Now sending: {scenarioName(drift).toLowerCase()}</span>;
    return demo ? <>Sending orders in pages</> : <>Reading {status?.source_topic}</>;
  }, [drift, silent, status?.last_record_age_s, demo, status?.source_topic]);

  async function replay() {
    setReplaying(true);
    setReplayMsg(null);
    try {
      const r = await api.replay();
      setReplayMsg(`${fmtInt(r.replayed)} replayed, ${fmtInt(r.loaded)} loaded, ${fmtInt(r.still_failing)} still failing`);
    } catch (e) {
      setReplayMsg(`Replay failed: ${(e as Error).message}`);
    } finally {
      setReplaying(false);
    }
  }

  return (
    <section aria-label="Pipeline diagram" className="rounded-xl border border-line bg-panel-2 p-4 sm:p-6">
      <div className="grid grid-cols-1 items-stretch gap-0 md:grid-cols-[1fr_minmax(70px,1fr)_1.15fr_minmax(70px,1fr)_1fr]">
        <Station title={demo ? "Upstream orders API" : `Upstream: ${status?.pipeline}`} tone={silent ? "fault" : "line"}>{upstreamNote}</Station>
        <div className="md:hidden"><Pipe vertical rate={inRate} tone="flow" label={`${inRate.toFixed(0)}/s`} /></div>
        <div className="hidden md:block"><Pipe rate={inRate} tone="flow" label={`${inRate.toFixed(0)}/s`} /></div>

        <Station title="Stream processor" tone={status?.code_error ? "fault" : status?.busy ? "agent" : "line"}>
          Transform, then check the contract
          <div className="text-ink-3">Running code v{status?.code_version ?? "–"}</div>
        </Station>
        <div className="md:hidden"><Pipe vertical rate={loadRate} tone="flow" label={`${loadRate.toFixed(0)}/s loaded`} /></div>
        <div className="hidden md:block"><Pipe rate={loadRate} tone="flow" label={`${loadRate.toFixed(0)}/s`} /></div>

        <Station title="Warehouse">
          <span className="num font-cond text-[22px] font-semibold text-ink">{fmtInt(status?.warehouse_rows)}</span>{" "}
          {demo ? "clean orders" : "clean rows"}
          {!demo && sinkTable && <div className="text-ink-3">{status?.sink?.split(":")[0]} · {sinkTable}</div>}
        </Station>
      </div>

      {/* the quarantine branch hangs off the processor */}
      <div className="grid grid-cols-1 md:grid-cols-[1fr_minmax(70px,1fr)_1.15fr_minmax(70px,1fr)_1fr]">
        <div className="md:col-span-2 md:col-start-3">
          <Pipe vertical rate={dlqRate} tone="quarantine" label={dlqRate > 0.05 ? `${dlqRate.toFixed(1)}/s diverted` : "nothing diverted"} />
          <div className="flex items-stretch gap-3">
            <div className="relative h-24 w-20 shrink-0 overflow-hidden rounded-b-xl rounded-t-md border-2 border-line-strong bg-panel"
                 role="meter" aria-label="Records waiting in quarantine" aria-valuenow={depth} aria-valuemin={0}>
              <div className="tank-level absolute inset-x-0 bottom-0 bg-quarantine transition-[height] duration-700 ease-out"
                   style={{ height: `${level * 100}%`, opacity: 0.85 }} />
              {[0.25, 0.5, 0.75].map((y) => (
                <div key={y} className="absolute left-0 h-px w-2 bg-line-strong" style={{ bottom: `${y * 100}%` }} />
              ))}
            </div>
            <div className="min-w-0 py-1 md:max-w-[260px]">
              <div className="font-cond text-[15px] font-semibold">Quarantine</div>
              <div className="text-[13.5px] text-ink-2">
                <span className="num font-cond text-[22px] font-semibold text-ink">{fmtInt(depth)}</span>{" "}
                records waiting for a fix
              </div>
              <button onClick={replay} disabled={replaying || depth === 0}
                      className="mt-1.5 rounded-md border border-line-strong bg-panel px-2.5 py-1 text-[13px] font-medium text-ink hover:border-ink-3">
                {replaying ? "Replaying" : "Replay now"}
              </button>
            </div>
          </div>
          {replayMsg && <p className="mt-2 text-[13px] text-ink-2" role="status">{replayMsg}</p>}
        </div>
      </div>
    </section>
  );
}
