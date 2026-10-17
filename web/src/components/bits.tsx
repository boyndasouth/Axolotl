import type { ReactNode } from "react";
import type { Tone } from "../format";

const TONE: Record<Tone, string> = {
  flow: "text-flow bg-flow-wash",
  quarantine: "text-quarantine-ink bg-quarantine-wash",
  fault: "text-fault bg-fault-wash",
  agent: "text-agent bg-agent-wash",
  ok: "text-ok bg-ok-wash",
  muted: "text-ink-2 bg-panel-2",
};
const DOT: Record<Tone, string> = {
  flow: "text-flow", quarantine: "text-quarantine", fault: "text-fault",
  agent: "text-agent", ok: "text-ok", muted: "text-ink-3",
};

export function Pill({ tone, children, live = false }: { tone: Tone; children: ReactNode; live?: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-[13px] font-medium ${TONE[tone]}`}>
      <span className={`relative inline-block h-2 w-2 rounded-full bg-current ${DOT[tone]} ${live ? "pulse" : ""}`} />
      {children}
    </span>
  );
}

export function Panel({ title, children, actions, className = "" }:
  { title?: ReactNode; children: ReactNode; actions?: ReactNode; className?: string }) {
  return (
    <section className={`rounded-xl border border-line bg-panel p-4 sm:p-5 ${className}`}>
      {(title || actions) && (
        <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
          {title && <h2 className="font-cond text-lg font-semibold">{title}</h2>}
          {actions}
        </div>
      )}
      {children}
    </section>
  );
}

export function Button({ children, onClick, kind = "plain", disabled, type = "button" }: {
  children: ReactNode; onClick?: () => void; kind?: "primary" | "plain" | "danger";
  disabled?: boolean; type?: "button" | "submit";
}) {
  const cls = kind === "primary"
    ? "bg-flow text-white border-flow hover:brightness-110"
    : kind === "danger"
      ? "bg-panel text-fault border-line-strong hover:border-fault"
      : "bg-panel text-ink border-line-strong hover:border-ink-3";
  return (
    <button type={type} onClick={onClick} disabled={disabled}
            className={`rounded-md border px-3 py-1.5 text-[14px] font-medium ${cls}`}>
      {children}
    </button>
  );
}
