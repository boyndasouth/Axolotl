// Records per second over the last five minutes: loaded vs quarantined, with the
// moments that explain the shape (chaos injected, fix went live) marked on it.

import { useEffect, useMemo, useRef, useState } from "react";
import type { Point, StoredEvent } from "../api";
import { fmtClock, scenarioName } from "../format";

const BIN = 5;
const H = 236;
const PAD = { top: 52, right: 12, bottom: 24, left: 36 };

type Bin = { t: number; loaded: number; dlq: number };

function niceMax(v: number) {
  if (v <= 5) return 5;
  const pow = 10 ** Math.floor(Math.log10(v));
  const n = v / pow;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * pow;
}

export function Throughput({ series, events }: { series: Point[]; events: StoredEvent[] }) {
  const wrap = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(640);
  const [hover, setHover] = useState<number | null>(null);
  const [table, setTable] = useState(false);

  useEffect(() => {
    if (!wrap.current) return;
    const ro = new ResizeObserver(([e]) => setWidth(Math.max(280, e.contentRect.width)));
    ro.observe(wrap.current);
    return () => ro.disconnect();
  }, []);

  const bins: Bin[] = useMemo(() => {
    const done = series.slice(0, -1);          // the current second is still filling
    if (!done.length) return [];
    const end = done[done.length - 1].t;
    const out: Bin[] = [];
    for (let start = end - 300 + 1; start <= end; start += BIN) {
      const pts = done.filter((p) => p.t >= start && p.t < start + BIN);
      out.push({ t: start + BIN, loaded: pts.reduce((a, p) => a + p.loaded, 0) / BIN,
                 dlq: pts.reduce((a, p) => a + p.dlq, 0) / BIN });
    }
    return out;
  }, [series]);

  const innerW = width - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;
  const t0 = bins.length ? bins[0].t : 0;
  const t1 = bins.length ? bins[bins.length - 1].t : 1;
  const yMax = niceMax(Math.max(1, ...bins.map((b) => Math.max(b.loaded, b.dlq))));
  const x = (t: number) => PAD.left + ((t - t0) / Math.max(1, t1 - t0)) * innerW;
  const y = (v: number) => PAD.top + innerH - (v / yMax) * innerH;
  const line = (k: "loaded" | "dlq") => bins.map((b, i) => `${i ? "L" : "M"}${x(b.t).toFixed(1)},${y(b[k]).toFixed(1)}`).join("");
  const area = (k: "loaded" | "dlq") => bins.length
    ? `${line(k)}L${x(t1).toFixed(1)},${y(0)}L${x(t0).toFixed(1)},${y(0)}Z` : "";
  const ticks = [0, yMax / 2, yMax];

  // event markers; labels that would overlap drop to the next row (at most three rows)
  const rowEnds = [-Infinity, -Infinity, -Infinity];
  const marks = events
    .filter((e) => (e.kind === "drift_changed" || e.kind === "fix_applied") && e.ts >= t0 - BIN && e.ts <= t1 + 1)
    .sort((a, b) => a.ts - b.ts)
    .map((e) => {
      const label = e.kind === "fix_applied" ? "Fix live"
        : e.data.scenario ? scenarioName(e.data.scenario) : "Upstream back to normal";
      const w = label.length * 6.3 + 8;
      const mx = x(e.ts);
      const anchorEnd = mx + w > width - PAD.right;
      const left = anchorEnd ? mx - w : mx;
      let row = rowEnds.findIndex((end) => left > end + 6);
      if (row === -1) row = rowEnds.indexOf(Math.min(...rowEnds));
      rowEnds[row] = left + w;
      return { t: e.ts, id: e.id, label, row, anchorEnd };
    });

  const hb = hover != null ? bins[hover] : null;
  const last = bins[bins.length - 1];

  function onMove(ev: React.PointerEvent<SVGSVGElement>) {
    if (!bins.length) return;
    const r = ev.currentTarget.getBoundingClientRect();
    const px = ev.clientX - r.left;
    const i = Math.round(((px - PAD.left) / innerW) * (bins.length - 1));
    setHover(Math.max(0, Math.min(bins.length - 1, i)));
  }

  return (
    <section className="rounded-xl border border-line bg-panel p-4 sm:p-5" aria-labelledby="tp-title">
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
        <h2 id="tp-title" className="font-cond text-lg font-semibold">Records per second, last 5 minutes</h2>
        <div className="flex items-center gap-4 text-[13px] text-ink-2">
          <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 rounded bg-flow" />Loaded</span>
          <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 rounded bg-quarantine" />Quarantined</span>
          <button className="underline decoration-line-strong underline-offset-2 hover:text-ink"
                  onClick={() => setTable((v) => !v)}>{table ? "Show chart" : "Show table"}</button>
        </div>
      </div>

      <div ref={wrap} className="relative mt-2">
        {table ? (
          <div className="max-h-[210px] overflow-y-auto">
            <table className="num w-full text-[13px]">
              <thead className="sticky top-0 bg-panel text-left text-ink-3">
                <tr><th className="py-1 font-medium">Time</th><th className="font-medium">Loaded per s</th><th className="font-medium">Quarantined per s</th></tr>
              </thead>
              <tbody>
                {[...bins].reverse().map((b) => (
                  <tr key={b.t} className="border-t border-line">
                    <td className="py-0.5 text-ink-2">{fmtClock(b.t)}</td><td>{b.loaded.toFixed(1)}</td><td>{b.dlq.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <svg width={width} height={H} className="block touch-none select-none" onPointerMove={onMove}
               onPointerLeave={() => setHover(null)} role="img"
               aria-label={last ? `Now ${last.loaded.toFixed(0)} loaded and ${last.dlq.toFixed(1)} quarantined per second` : "Waiting for data"}>
            {ticks.map((v) => (
              <g key={v}>
                <line x1={PAD.left} x2={width - PAD.right} y1={y(v)} y2={y(v)} stroke="var(--line)" strokeWidth={1} />
                <text x={PAD.left - 6} y={y(v) + 4} textAnchor="end" className="num fill-ink-3 text-[11px]">{Math.round(v)}</text>
              </g>
            ))}
            {bins.length > 1 && [0, 0.5, 1].map((f) => {
              const t = t0 + f * (t1 - t0);
              return <text key={f} x={x(t)} y={H - 6} textAnchor={f === 0 ? "start" : f === 1 ? "end" : "middle"}
                           className="num fill-ink-3 text-[11px]">{fmtClock(t)}</text>;
            })}
            {marks.map((m) => (
              <g key={m.id}>
                <line x1={x(m.t)} x2={x(m.t)} y1={PAD.top - 6 - m.row * 15} y2={y(0)} stroke="var(--ink-3)" strokeWidth={1} />
                <text x={m.anchorEnd ? x(m.t) - 4 : x(m.t) + 4} y={PAD.top - 10 - m.row * 15}
                      textAnchor={m.anchorEnd ? "end" : "start"} className="fill-ink-2 text-[11.5px]">{m.label}</text>
              </g>
            ))}
            <path d={area("loaded")} fill="var(--flow-wash)" />
            <path d={area("dlq")} fill="var(--quarantine-wash)" />
            <path d={line("loaded")} fill="none" stroke="var(--flow)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
            <path d={line("dlq")} fill="none" stroke="var(--quarantine)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
            {hb && (
              <g>
                <line x1={x(hb.t)} x2={x(hb.t)} y1={PAD.top} y2={y(0)} stroke="var(--line-strong)" strokeWidth={1} />
                <circle cx={x(hb.t)} cy={y(hb.loaded)} r={4} fill="var(--flow)" stroke="var(--panel)" strokeWidth={2} />
                <circle cx={x(hb.t)} cy={y(hb.dlq)} r={4} fill="var(--quarantine)" stroke="var(--panel)" strokeWidth={2} />
              </g>
            )}
          </svg>
        )}
        {hb && !table && (
          <div className="pointer-events-none absolute top-6 rounded-md border border-line bg-panel px-2.5 py-1.5 text-[12.5px] shadow-sm"
               style={{ left: Math.min(x(hb.t) + 10, width - 170) }}>
            <div className="text-ink-3">{fmtClock(hb.t - BIN)} to {fmtClock(hb.t)}</div>
            <div className="num flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-flow" />{hb.loaded.toFixed(1)} loaded per s</div>
            <div className="num flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-quarantine" />{hb.dlq.toFixed(1)} quarantined per s</div>
          </div>
        )}
      </div>
    </section>
  );
}
