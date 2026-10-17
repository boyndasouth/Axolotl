import type { Incident, IncidentRow, Status } from "./api";

export const fmtInt = (n: number | null | undefined) =>
  n == null ? "–" : Math.round(n).toLocaleString("en-US");

export const fmtCompact = (n: number) =>
  n >= 10_000 ? `${(n / 1000).toFixed(0)}K` : n >= 1000 ? `${(n / 1000).toFixed(1)}K` : `${Math.round(n)}`;

export function fmtSecs(s: number | null | undefined): string {
  if (s == null || Number.isNaN(s)) return "–";
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  if (s < 60) return `${s < 10 ? s.toFixed(1) : Math.round(s)} s`;
  const m = Math.floor(s / 60);
  return `${m} min ${Math.round(s % 60)} s`;
}

export function fmtAgo(ts: number, now = Date.now() / 1000): string {
  const s = Math.max(0, now - ts);
  if (s < 5) return "just now";
  if (s < 60) return `${Math.round(s)} s ago`;
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  return new Date(ts * 1000).toLocaleString([], { hour: "2-digit", minute: "2-digit", month: "short", day: "numeric" });
}

export const fmtClock = (ts: number) =>
  new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });

// Plain-language names for the chaos scenarios
export const SCENARIO_NAMES: Record<string, string> = {
  rename_column: "Field renamed",
  currency_string: "Amounts sent as text",
  date_format: "Dates switch to US format",
  nested_amount: "Amount becomes an object",
  status_case: "Status values upper-cased",
  bad_rows: "Some rows invalid",
  duplicate_rows: "Rows resent",
  upstream_outage: "Upstream goes silent",
  additive_field: "Harmless new field",
};
export const scenarioName = (id: string | null | undefined) => (id ? SCENARIO_NAMES[id] ?? id : "");

export const EXPECTED: Record<string, string> = {
  repair: "Agent should fix it",
  escalate: "Agent should hand it to you",
  no_incident: "Should not raise an incident",
};

// Incident status in the words a person uses
export const STATUS_LABEL: Record<string, string> = {
  detected: "Detected",
  diagnosing: "Diagnosing",
  repairing: "Writing a fix",
  verifying: "Testing the fix",
  pr_open: "Waiting for your review",
  merging: "Deploying",
  rejecting: "Closing",
  merged: "Approved, deploying",
  resolved: "Fixed and recovered",
  rollout_failed: "Deployed, still failing",
  rejected: "Fix rejected",
  escalated: "Needs a human",
  interrupted: "Interrupted",
  error: "Agent error",
  healthy: "No incident",
};

export type Tone = "flow" | "quarantine" | "fault" | "agent" | "ok" | "muted";

export function statusTone(status: string): Tone {
  if (["diagnosing", "repairing", "verifying", "detected", "merging", "merged", "rejecting"].includes(status)) return "agent";
  if (status === "pr_open") return "quarantine";
  if (status === "resolved") return "ok";
  if (["rollout_failed", "escalated", "error"].includes(status)) return "fault";
  return "muted";
}

export function pipelineState(status: Status | null, open: IncidentRow | undefined):
  { label: string; tone: Tone; detail: string } {
  if (!status) return { label: "Connecting", tone: "muted", detail: "" };
  if (status.code_error) return { label: "Code failed to load", tone: "fault", detail: status.code_error };
  if (open?.status === "pr_open") return { label: "Waiting for your review", tone: "quarantine", detail: "A verified fix is ready" };
  if (open) return { label: "Agent working", tone: "agent", detail: STATUS_LABEL[open.status] ?? open.status };
  if (status.firing.includes("silence:upstream")) return { label: "Upstream silent", tone: "fault", detail: "No records arriving" };
  if (status.window.dlq > 0) return { label: "Quarantining", tone: "quarantine", detail: `${status.window.dlq} records held in the last ${status.window_s}s` };
  return { label: "Flowing", tone: "ok", detail: "Every record meets the contract" };
}

export type Stage = {
  key: string; label: string; at?: number; detail?: string;
  state: "done" | "active" | "you" | "failed" | "todo";
};

export function stagesFor(inc: Incident): Stage[] {
  const ts = inc.timestamps || {};
  const st = inc.status;
  const attempts = inc.attempts.length;
  const passed = inc.attempts.find((a) => a.verification.passed);
  const detectLatency = ts.first_failure && ts.detected ? ts.detected - ts.first_failure : undefined;

  const s: Stage[] = [
    { key: "detected", label: "Detected", at: ts.detected, state: "done",
      detail: detectLatency != null
        ? `${fmtSecs(detectLatency)} after ${inc.report?.failed_check === "min_rows" ? "the last record arrived" : "the first bad record"}`
        : undefined },
    { key: "diagnosed", label: "Diagnosed", at: ts.diagnosed, state: ts.diagnosed ? "done" : "active",
      detail: inc.diagnosis ? `${inc.diagnosis.category.replace(/_/g, " ")}, ${(inc.diagnosis.confidence * 100).toFixed(0)}% sure` : undefined },
    { key: "verified", label: "Fix verified", at: ts.verified,
      state: ts.verified ? "done" : "todo",
      detail: attempts ? `attempt ${passed ? passed.attempt : attempts} of ${inc.max_attempts ?? 3}` : undefined },
    { key: "pr", label: "PR opened", at: ts.pr_opened, state: ts.pr_opened ? "done" : "todo" },
    { key: "review", label: "Your review", at: ts.resolved, state: ts.resolved ? "done" : "todo" },
    { key: "live", label: "Fix live", at: ts.deployed, state: ts.deployed ? "done" : "todo",
      detail: inc.rollout?.code_version ? `processor on code v${inc.rollout.code_version}` : undefined },
    { key: "recovered", label: "Data recovered", at: ts.recovered, state: ts.recovered ? "done" : "todo",
      detail: inc.rollout?.replay ? `${fmtInt(inc.rollout.replay.replayed - inc.rollout.replay.still_failing)} of ${fmtInt(inc.rollout.replay.replayed)} replayed` : undefined },
  ];

  if (st === "escalated" || st === "error" || st === "interrupted") {
    const at = s.findIndex((x) => x.state !== "done");
    const idx = at === -1 ? s.length - 1 : at;
    s[idx] = { key: "escalated", label: st === "escalated" ? "Handed to you" : STATUS_LABEL[st],
               state: "failed", at: ts.escalated, detail: inc.escalation_reason ?? undefined };
    return s.slice(0, idx + 1);
  }
  if (st === "rejected") {
    s[4] = { ...s[4], label: "You rejected it", state: "failed" };
    return s.slice(0, 5);
  }
  if (st === "rollout_failed") {
    s[6] = { ...s[6], label: "Still failing after deploy", state: "failed" };
    return s;
  }
  if (st === "pr_open") {
    s[4] = { ...s[4], state: "you" };
    return s;
  }
  if (!["resolved", "merged"].includes(st)) {
    const i = s.findIndex((x) => x.state !== "done");
    if (i >= 0) s[i] = { ...s[i], state: "active" };
  }
  return s;
}
