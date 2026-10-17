import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Status } from "../api";
import { EXPECTED, scenarioName } from "../format";
import { Panel } from "./bits";

const ORDER = { repair: 0, escalate: 1, no_incident: 2 } as const;

// Make upstream misbehave on purpose. One scenario at a time, starting mid-stream.
export function ChaosPanel({ status }: { status: Status | null }) {
  const qc = useQueryClient();
  const { data } = useQuery({ queryKey: ["chaos"], queryFn: api.chaos });
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const active = status?.drift.scenario ?? data?.active.scenario ?? null;
  const [rate, setRate] = useState<number>(status?.producer.rate ?? 25);

  useEffect(() => { if (status?.producer.rate) setRate(status.producer.rate); }, [status?.producer.rate]);

  async function run(fn: () => Promise<unknown>, key: string) {
    setPending(key);
    setError(null);
    try {
      await fn();
      qc.invalidateQueries({ queryKey: ["chaos"] });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setPending(null);
    }
  }

  return (
    <Panel title="Break the upstream"
           actions={active ? (
             <button onClick={() => run(api.clearChaos, "clear")} disabled={pending !== null}
                     className="rounded-md border border-line-strong px-2.5 py-1 text-[13px] font-medium hover:border-ink-3">
               Back to normal
             </button>) : null}>
      <p className="mb-3 text-[13.5px] text-ink-2">
        Pick a change and upstream starts sending it with the next page of orders.
      </p>
      <ul className="divide-y divide-line">
        {[...(data?.scenarios ?? [])].sort((a, b) => ORDER[a.expected_outcome] - ORDER[b.expected_outcome]).map((s) => {
          const on = active === s.id;
          return (
            <li key={s.id} className="flex items-center gap-3 py-2">
              <div className="min-w-0 flex-1">
                <div className={`text-[14.5px] font-medium ${on ? "text-quarantine-ink" : "text-ink"}`}>{scenarioName(s.id)}</div>
                <div className="truncate text-[12.5px] text-ink-3" title={s.description}>{EXPECTED[s.expected_outcome]}</div>
              </div>
              {on ? (
                <span className="text-[13px] font-medium text-quarantine-ink">Sending now</span>
              ) : (
                <button onClick={() => run(() => api.inject(s.id), s.id)} disabled={pending !== null}
                        aria-label={`Inject: ${scenarioName(s.id)}`}
                        className="rounded-md border border-line-strong px-2.5 py-1 text-[13px] font-medium hover:border-quarantine hover:text-quarantine-ink">
                  {pending === s.id ? "Injecting" : "Inject"}
                </button>
              )}
            </li>
          );
        })}
      </ul>
      {status?.producer.embedded && (
        <label className="mt-3 flex items-center gap-3 border-t border-line pt-3 text-[13.5px] text-ink-2">
          <span className="shrink-0">Upstream rate</span>
          <input type="range" min={5} max={150} step={5} value={rate} className="w-full accent-[var(--flow)]"
                 onChange={(e) => setRate(Number(e.target.value))}
                 onPointerUp={() => run(() => api.setRate(rate), "rate")}
                 onKeyUp={() => run(() => api.setRate(rate), "rate")} />
          <span className="num w-16 shrink-0 text-right text-ink">{rate}/s</span>
        </label>
      )}
      {error && <p className="mt-2 text-[13px] text-fault" role="alert">{error}</p>}
    </Panel>
  );
}
