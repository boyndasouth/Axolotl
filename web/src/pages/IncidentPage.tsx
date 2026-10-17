import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type Attempt, type Incident, type StoredEvent } from "../api";
import { Decision } from "../components/Decision";
import { DiffView } from "../components/DiffView";
import { StageTrack } from "../components/StageTrack";
import { Panel, Pill } from "../components/bits";
import { fmtClock, fmtInt, fmtSecs, scenarioName, STATUS_LABEL, statusTone } from "../format";
import { useLive } from "../live";

function Stat({ label, value, note }: { label: string; value: string; note?: string }) {
  return (
    <div>
      <div className="text-[13px] text-ink-3">{label}</div>
      <div className="font-cond text-[24px] font-semibold leading-tight text-ink">{value}</div>
      {note && <div className="text-[12.5px] text-ink-3">{note}</div>}
    </div>
  );
}

function SchemaDiff({ d }: { d: any }) {
  if (!d || d.baseline === "none") return null;
  const items: string[] = [];
  (d.added ?? []).forEach((f: string) => items.push(`New field "${f}"`));
  (d.removed ?? []).forEach((f: string) => items.push(`Field "${f}" missing`));
  Object.entries(d.type_changed ?? {}).forEach(([f, v]: [string, any]) =>
    items.push(`"${f}" was ${v.before.join(" or ")}, now ${v.after.join(" or ")}`));
  if (!items.length) return <p className="text-[14px] text-ink-2">The fields and their types look the same as before.</p>;
  return (
    <ul className="space-y-0.5 text-[14px]">
      {items.map((t) => <li key={t} className="flex gap-2"><span className="text-quarantine">◆</span><span>{t}</span></li>)}
    </ul>
  );
}

function WhatBroke({ inc }: { inc: Incident }) {
  const r = inc.report;
  const silent = r.failed_check === "min_rows";
  return (
    <Panel title="What broke">
      {silent && <p className="max-w-[75ch] text-[15px]">No records arrived for {r.silence_s} s after the stream had been flowing.</p>}
      {!silent && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
          <Stat label={`Arrived in ${r.window_s ?? "?"} s window`} value={fmtInt(r.rows_in)} />
          <Stat label="Quarantined" value={fmtInt(r.quarantined)} note={r.dlq_rate != null ? `${(r.dlq_rate * 100).toFixed(0)}% of traffic` : undefined} />
          <Stat label="Still loaded" value={fmtInt(r.rows_out)} />
        </div>
      )}
      {r.schema_diff && (
        <>
          <h3 className="mt-5 font-cond text-[15px] font-semibold">How the records changed</h3>
          <div className="mt-1"><SchemaDiff d={r.schema_diff} /></div>
        </>
      )}
      {r.error && (
        <>
          <h3 className="mt-5 font-cond text-[15px] font-semibold">Error from the transform</h3>
          <p className="mt-1 font-mono text-[13px] text-fault">{r.error.type}: {r.error.message.split("\n")[0]}</p>
          {r.error.traceback && (
            <details className="mt-1"><summary className="cursor-pointer text-[13px] text-ink-2">Traceback</summary>
              <pre className="mt-1 overflow-x-auto rounded-md bg-panel-2 p-2 font-mono text-[12px] text-ink-2">{r.error.traceback}</pre>
            </details>
          )}
        </>
      )}
      {r.failures?.length > 0 && (
        <>
          <h3 className="mt-5 font-cond text-[15px] font-semibold">Contract checks that failed</h3>
          <table className="mt-1 w-full text-[14px]">
            <thead className="text-left text-[13px] text-ink-3"><tr><th className="pb-1 font-medium">Column</th><th className="font-medium">Check</th><th className="text-right font-medium">Records</th><th className="pl-4 font-medium">Examples</th></tr></thead>
            <tbody>
              {r.failures.map((f) => (
                <tr key={`${f.column}${f.check}`} className="border-t border-line align-top">
                  <td className="py-1 font-medium">{f.column}</td><td className="break-all font-mono text-[12.5px] text-ink-2">{f.check}</td>
                  <td className="num text-right">{f.count}</td><td className="pl-4 font-mono text-[12.5px] text-ink-2">{f.examples.slice(0, 3).join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </Panel>
  );
}

function Diagnosis({ inc }: { inc: Incident }) {
  const d = inc.diagnosis;
  if (!d) return <Panel title="Diagnosis"><p className="text-[14px] text-ink-2">The diagnosis agent is reading the logs, schema diff, git history and sample records.</p></Panel>;
  return (
    <Panel title="Diagnosis">
      <p className="max-w-[75ch] text-[15px]">{d.root_cause}</p>
      <div className="mt-4 flex flex-wrap items-center gap-x-8 gap-y-3">
        <div className="w-56">
          <div className="text-[13px] text-ink-3">Confidence</div>
          <div className="mt-1 h-2 rounded-full bg-agent-wash">
            <div className="h-2 rounded-full bg-agent" style={{ width: `${d.confidence * 100}%` }} />
          </div>
          <div className="num mt-0.5 text-[13px] text-ink-2">{(d.confidence * 100).toFixed(0)}%, {d.repairable ? "fixable in code" : "not fixable in code"}</div>
        </div>
        <div><div className="text-[13px] text-ink-3">Category</div><div className="text-[14.5px]">{d.category.replace(/_/g, " ")}</div></div>
      </div>
      {d.fix_plan && <><h3 className="mt-4 font-cond text-[15px] font-semibold">Plan</h3><p className="max-w-[75ch] text-[14.5px] text-ink-2">{d.fix_plan}</p></>}
      {d.evidence?.length > 0 && (
        <><h3 className="mt-4 font-cond text-[15px] font-semibold">Evidence it relied on</h3>
          <ul className="mt-1 max-w-[80ch] list-disc space-y-0.5 pl-5 text-[14px] text-ink-2">{d.evidence.map((e) => <li key={e}>{e}</li>)}</ul></>
      )}
    </Panel>
  );
}

function Check({ ok, label, detail }: { ok: boolean | null; label: string; detail: string }) {
  return (
    <li className="flex gap-2.5 py-1">
      <span className={`mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full text-[10px] font-bold text-white ${ok == null ? "bg-line-strong" : ok ? "bg-ok" : "bg-fault"}`}>
        {ok == null ? "–" : ok ? "✓" : "✕"}
      </span>
      <span className="min-w-0 text-[14px]"><span className="font-medium">{label}</span> <span className="text-ink-2">{detail}</span></span>
    </li>
  );
}

function Attempts({ inc }: { inc: Incident }) {
  const [sel, setSel] = useState<number | null>(null);
  if (!inc.attempts.length) return null;
  const a: Attempt = inc.attempts[sel ?? inc.attempts.length - 1];
  const v = a.verification;
  const incident = v.schema_checks?.["incident" as keyof typeof v.schema_checks];
  const hold = Object.entries(v.schema_checks ?? {}).find(([k]) => k === "holdout")?.[1];
  return (
    <Panel title="Fix attempts"
           actions={inc.attempts.length > 1 ? (
             <div role="tablist" className="flex gap-1">
               {inc.attempts.map((x, i) => (
                 <button key={x.attempt} role="tab" aria-selected={a.attempt === x.attempt}
                         onClick={() => setSel(i)}
                         className={`rounded-md px-2.5 py-1 text-[13px] font-medium ${a.attempt === x.attempt ? "bg-ink text-panel" : "text-ink-2 hover:bg-panel-2"}`}>
                   Attempt {x.attempt} {x.verification.passed ? "passed" : "failed"}
                 </button>
               ))}
             </div>) : null}>
      {a.patch.error ? (
        <p className="text-[14px] text-fault">{a.patch.error}</p>
      ) : (
        <>
          {a.patch.rationale && <p className="mb-3 max-w-[75ch] text-[14.5px]">{a.patch.rationale}</p>}
          <DiffView diff={a.patch.diff} />
        </>
      )}
      <h3 className="mt-4 font-cond text-[15px] font-semibold">
        Sandbox verdict: <span className={v.passed ? "text-ok" : "text-fault"}>{v.passed ? "passed" : "failed"}</span>
      </h3>
      <ul className="mt-1">
        <Check ok={v.unit_tests?.failed === 0 && (v.unit_tests?.passed ?? 0) > 0 ? true : v.unit_tests?.passed == null ? null : false}
               label="Unit tests" detail={v.unit_tests?.passed != null ? `${v.unit_tests.passed} passed, ${v.unit_tests.failed} failed` : "not run"} />
        <Check ok={incident ? incident.ok : null} label="The live batch that broke"
               detail={incident ? (incident.ok ? `meets the contract, ${incident.rows_out} of ${incident.rows_in} rows kept` : incident.error ?? "still fails") : "not run"} />
        <Check ok={hold ? hold.ok && v.golden_diff_rows === 0 : null} label="Held-out data vs expected output"
               detail={hold ? (v.golden_diff_rows === 0 ? "matches row for row" : hold.ok ? `${v.golden_diff_rows} rows differ` : hold.error ?? "fails") : "no fixture for this incident"} />
        <Check ok={v.regressions ? v.regressions.length === 0 : null} label="Regressions"
               detail={v.regressions?.length ? v.regressions.join(", ") : "none on previously passing inputs"} />
      </ul>
      {!v.passed && v.summary && (
        <details className="mt-2"><summary className="cursor-pointer text-[13px] text-ink-2">What the verifier told the repair agent</summary>
          <pre className="mt-1 whitespace-pre-wrap rounded-md bg-panel-2 p-2 font-mono text-[12px] text-ink-2">{v.summary}</pre>
        </details>
      )}
      <p className="mt-2 text-[12.5px] text-ink-3">Ran in the {v.backend} sandbox in {fmtSecs(v.duration_s)}.</p>
    </Panel>
  );
}

function Recovery({ inc }: { inc: Incident }) {
  const r = inc.rollout;
  if (!r) return null;
  const rp = r.replay;
  const lc = r.live_check;
  return (
    <Panel title="Deploy and recovery">
      <p className={`text-[15px] font-medium ${r.healthy ? "text-ok" : "text-fault"}`}>
        {r.healthy ? "The fix is live and every quarantined record was dealt with." : r.error ?? "The fix is live but records are still failing."}
      </p>
      {r.applied && <p className="mt-1 text-[14px] text-ink-2">Patch {r.applied.method}: {r.applied.files.join(", ")}.</p>}
      {rp && (
        <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
          <Stat label="Replayed" value={fmtInt(rp.replayed)} />
          <Stat label="Loaded" value={fmtInt(rp.loaded)} note={rp.already_loaded ? `${rp.already_loaded} already there` : undefined} />
          <Stat label="Dropped by the fix" value={fmtInt(rp.filtered)} note="invalid or duplicate" />
          <Stat label="Still failing" value={fmtInt(rp.still_failing)} />
        </div>
      )}
      {lc && (
        <p className="mt-4 text-[14px] text-ink-2">
          Live check after deploy: {fmtInt(lc.in)} new records in {lc.seconds} s, {fmtInt(lc.loaded)} loaded, {fmtInt(lc.quarantined)} quarantined.
        </p>
      )}
    </Panel>
  );
}

function eventText(e: StoredEvent): string {
  const d = e.data;
  switch (e.kind) {
    case "incident_opened": return "Incident opened";
    case "progress":
      if (d.stage === "llm_call") return d.role === "diagnosis" ? `Asking ${d.model} for a diagnosis` : `Asking ${d.model} for fix attempt ${d.attempt}`;
      if (d.stage === "sandbox_started") return `Testing attempt ${d.attempt} in the sandbox`;
      if (d.stage === "rollout_started") return "Deploying the fix";
      return d.stage ?? "Progress";
    case "node":
      if (d.node === "monitor") return "Anomaly confirmed";
      if (d.node === "diagnose") return "Diagnosis written";
      if (d.node === "repair") return "Fix written";
      if (d.node === "verify") return d.passed ? "Sandbox: passed every check" : "Sandbox: failed, feeding the errors back";
      if (d.node === "open_pr") return "Pull request opened";
      if (d.node === "escalate") return "Handed to a human";
      if (d.node === "await_merge") return d.status === "merged" ? "Approved" : "Rejected";
      if (d.node === "rollout") return d.status === "resolved" ? "Recovered" : "Deployed, still failing";
      return d.node;
    case "awaiting_decision": return "Waiting for your review";
    case "decision": return d.merged ? "You approved the fix" : "You rejected the fix";
    case "fix_applied": return `Patch ${d.method}`;
    case "dlq_replayed": return `Replayed ${fmtInt(d.replayed)} quarantined records`;
    case "live_check": return d.healthy ? "Live stream clean on the new code" : `${d.quarantined} records still quarantined after deploy`;
    case "status": return `Status: ${STATUS_LABEL[d.status] ?? d.status}`;
    case "agent_error": return `Agent error: ${d.error}`;
    default: return e.kind.replace(/_/g, " ");
  }
}

export function IncidentPage({ id }: { id: string }) {
  const { status } = useLive();
  const { data: inc, error, isLoading } = useQuery({ queryKey: ["incident", id], queryFn: () => api.incident(id) });

  if (isLoading) return <p className="text-ink-2">Loading the incident</p>;
  if (error || !inc) return <p className="text-fault">Couldn't load this incident: {(error as Error)?.message}</p>;

  const ts = inc.timestamps;
  const t0 = ts.detected;
  return (
    <div className="space-y-5">
      <div>
        <a href="#/" className="text-[14px] text-flow underline underline-offset-2">Back to the live view</a>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Pill tone={statusTone(inc.status)} live={!["resolved", "rejected", "escalated", "rollout_failed", "pr_open", "error", "interrupted"].includes(inc.status)}>
            {STATUS_LABEL[inc.status] ?? inc.status}
          </Pill>
          <span className="text-[13px] text-ink-3">Opened {fmtClock(inc.created_at)}</span>
          {inc.scenario && <span className="text-[13px] text-ink-3">Injected on purpose: {scenarioName(inc.scenario).toLowerCase()}</span>}
        </div>
        <h1 className="mt-2 max-w-[60ch] font-cond text-[26px] font-semibold leading-tight">{inc.anomaly?.summary ?? inc.summary}</h1>
      </div>

      <Panel><StageTrack inc={inc} /></Panel>
      {inc.waiting_for_decision && <Decision inc={inc} waiting={status?.dlq_depth ?? 0} prMode={status?.pr_mode} />}

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1fr)_330px]">
        <div className="min-w-0 space-y-5">
          <WhatBroke inc={inc} />
          <Diagnosis inc={inc} />
          <Attempts inc={inc} />
          <Recovery inc={inc} />
          {inc.status === "escalated" && (
            <Panel title="Why a human is needed"><p className="text-[15px]">{inc.escalation_reason}</p></Panel>
          )}
        </div>
        <div className="space-y-5">
          <Panel title="Timing">
            <div className="grid grid-cols-2 gap-4">
              <Stat label="Detected after" value={ts.first_failure ? fmtSecs(ts.detected - ts.first_failure) : "–"}
                    note={inc.report?.failed_check === "min_rows" ? "last record arrived" : "first bad record"} />
              <Stat label="Verified PR in" value={ts.pr_opened ? fmtSecs(ts.pr_opened - t0) : "–"} note="mean time to repair" />
              <Stat label="Recovered in" value={ts.recovered ? fmtSecs(ts.recovered - t0) : "–"} note="includes your review" />
              <Stat label="Model calls" value={String(inc.llm_calls)} note={`$${inc.llm_cost_usd.toFixed(4)}`} />
            </div>
          </Panel>
          <Panel title="Timeline">
            <ol className="space-y-1.5">
              {inc.events.filter((e) => e.kind !== "status" && !(e.kind === "node" && e.data.node === "await_merge")).map((e) => (
                <li key={e.id} className="flex gap-3 text-[13.5px] leading-snug">
                  <span className="num w-14 shrink-0 text-right text-ink-3">{t0 ? `+${fmtSecs(Math.max(0, e.ts - t0))}` : fmtClock(e.ts)}</span>
                  <span className="text-ink-2">{eventText(e)}</span>
                </li>
              ))}
            </ol>
          </Panel>
          {(inc.pr_body || inc.pr_url) && (
            <Panel title="Pull request">
              {inc.pr_url?.startsWith("http")
                ? <a href={inc.pr_url} target="_blank" rel="noreferrer" className="text-[14px] text-flow underline">Open on GitHub</a>
                : <p className="text-[13.5px] text-ink-2">Dry-run mode: the PR was written to reports/prs/{inc.id}/PR.md.</p>}
              {inc.pr_body && (
                <details className="mt-2"><summary className="cursor-pointer text-[13.5px] text-ink-2">Read the PR description</summary>
                  <pre className="mt-2 max-h-[420px] overflow-auto whitespace-pre-wrap rounded-md bg-panel-2 p-2 font-mono text-[12px] text-ink-2">{inc.pr_body}</pre>
                </details>
              )}
            </Panel>
          )}
        </div>
      </div>
    </div>
  );
}
