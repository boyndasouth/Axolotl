// Types and calls for the control plane (stream/server.py).

export type Window = {
  produced: number; in: number; loaded: number; dlq: number; filtered: number; sink_dupes: number;
  reasons: Record<string, number>; dlq_rate: number;
};

export type Status = {
  bus: string; bus_detail: string;
  producer: { embedded: boolean; rate: number | null };
  drift: { scenario: string | null; since: number | null };
  window_s: number; window: Window; firing: string[]; disarmed: string[];
  totals: Record<string, number>;
  dlq_depth: number; warehouse_rows: number; topics: Record<string, number>;
  code_version: number; code_error: string | null; busy: boolean;
  pr_mode: string; uptime_s: number; last_record_age_s: number | null;
  models?: { diagnosis: string; repair: string };
};

export type Point = { t: number; produced: number; in: number; loaded: number; dlq: number; filtered: number; sink_dupes: number };

export type StoredEvent = {
  id: number; ts: number; incident_id: string | null; kind: string; data: Record<string, any>;
};

export type IncidentRow = {
  id: string; created_at: number; updated_at: number; status: string; kind: string | null;
  summary: string | null; signatures: string[]; scenario: string | null;
};

export type Attempt = {
  attempt: number;
  patch: { diff: string; rationale: string; files_changed: string[]; error: string | null };
  verification: {
    passed: boolean; summary: string; unit_tests: { passed?: number; failed?: number };
    schema_checks: Record<string, { ok: boolean; rows_in: number; rows_out: number; error: string | null; diff_rows: number | null }>;
    golden_diff_rows: number; regressions: string[]; duration_s: number; backend: string;
  };
};

export type Rollout = {
  healthy: boolean; error?: string;
  applied?: { method: string; files: string[] };
  replay?: { replayed: number; loaded: number; already_loaded: number; filtered: number; still_failing: number; still_failing_related: number; seconds: number };
  live_check?: { seconds: number; in: number; loaded: number; quarantined: number };
  code_version?: number;
};

export type Incident = IncidentRow & {
  waiting_for_decision: boolean;
  anomaly: { kind: string; failed_check: string; summary: string; run_id: string };
  report: {
    rows_in: number; rows_out: number; quarantined?: number; dlq_rate?: number; window_s?: number;
    signatures?: string[]; schema_diff: any; error: { type: string; message: string; traceback?: string } | null;
    failures: { column: string; check: string; count: number; examples: string[] }[];
    failed_check: string; silence_s?: number;
  };
  diagnosis: null | {
    root_cause: string; category: string; repairable: boolean; confidence: number;
    evidence: string[]; target_files: string[]; fix_plan: string;
  };
  attempts: Attempt[]; max_attempts: number;
  pr_url: string | null; pr_error?: string | null; pr_body: string | null;
  escalation_reason: string | null; escalation_body: string | null;
  rollout: Rollout | null;
  timestamps: Record<string, number>;
  llm_calls: number; llm_cost_usd: number;
  events: StoredEvent[];
};

export type Scenario = { id: string; description: string; expected_outcome: "repair" | "escalate" | "no_incident" };

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { "content-type": "application/json", ...(init?.headers || {}) },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail ?? detail; } catch { /* not json */ }
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return res.json();
}

export const api = {
  status: () => call<Status>("/api/status"),
  metrics: (seconds = 300) => call<Point[]>(`/api/metrics?seconds=${seconds}`),
  incidents: () => call<IncidentRow[]>("/api/incidents"),
  incident: (id: string) => call<Incident>(`/api/incidents/${id}`),
  events: (after = 0) => call<StoredEvent[]>(`/api/events?after=${after}&limit=200`),
  chaos: () => call<{ active: Status["drift"]; scenarios: Scenario[] }>("/api/chaos"),
  inject: (scenario: string) => call("/api/chaos", { method: "POST", body: JSON.stringify({ scenario }) }),
  clearChaos: () => call("/api/chaos", { method: "DELETE" }),
  decide: (id: string, merged: boolean) =>
    call(`/api/incidents/${id}/decision`, { method: "POST", body: JSON.stringify({ merged }) }),
  setRate: (rate: number) => call("/api/producer", { method: "POST", body: JSON.stringify({ rate }) }),
  replay: () => call<Record<string, number>>("/api/replay", { method: "POST" }),
  dlq: () => call<{ failed_at: number; record: Record<string, unknown>; error: Record<string, any> }[]>("/api/dlq?limit=40"),
  bench: () => call<{ runs: Record<string, any>[]; error?: string }>("/api/bench"),
};

// "merged" is not final: in streaming mode the fix is deployed and the DLQ replayed right after
export const TERMINAL = new Set(["healthy", "resolved", "rollout_failed", "rejected", "escalated", "interrupted", "error"]);
