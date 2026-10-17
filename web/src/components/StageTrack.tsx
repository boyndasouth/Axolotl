import type { Incident } from "../api";
import { fmtSecs, stagesFor } from "../format";

// The incident's journey, in order. Each step shows when it happened relative to detection.
export function StageTrack({ inc, compact = false }: { inc: Incident; compact?: boolean }) {
  const stages = stagesFor(inc);
  const t0 = inc.timestamps?.detected;
  return (
    <ol className={`grid gap-x-2 gap-y-3 ${compact ? "grid-cols-2 sm:grid-cols-4 lg:grid-cols-7" : "grid-cols-2 sm:grid-cols-4 lg:grid-cols-7"}`}>
      {stages.map((s) => {
        const ring =
          s.state === "done" ? "border-ok bg-ok text-white"
          : s.state === "active" ? "border-agent bg-agent-wash text-agent"
          : s.state === "you" ? "border-quarantine bg-quarantine-wash text-quarantine-ink"
          : s.state === "failed" ? "border-fault bg-fault-wash text-fault"
          : "border-line-strong bg-panel text-ink-3";
        return (
          <li key={s.key} className="min-w-0">
            <div className="flex items-center gap-2">
              <span className={`relative flex h-5 w-5 shrink-0 items-center justify-center rounded-full border-2 text-[11px] font-semibold ${ring} ${s.state === "active" ? "pulse" : ""}`}>
                {s.state === "done" ? "✓" : s.state === "failed" ? "!" : ""}
              </span>
              <span className={`truncate font-cond text-[15px] font-semibold ${s.state === "todo" ? "text-ink-3" : "text-ink"}`}>
                {s.state === "you" ? "Waiting for you" : s.label}
              </span>
            </div>
            <div className="mt-0.5 pl-7 text-[12.5px] leading-snug text-ink-2">
              {s.at && t0 && s.key !== "detected" ? <span className="num">+{fmtSecs(s.at - t0)}</span> : null}
              {s.at && t0 && s.key !== "detected" && s.detail ? <br /> : null}
              {!compact || s.state !== "todo" ? s.detail : null}
            </div>
          </li>
        );
      })}
    </ol>
  );
}
