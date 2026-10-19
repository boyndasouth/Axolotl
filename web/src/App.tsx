import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, TERMINAL } from "./api";
import { Pill } from "./components/bits";
import { pipelineState } from "./format";
import { useLive } from "./live";
import { BenchPage } from "./pages/BenchPage";
import { IncidentPage } from "./pages/IncidentPage";
import { LivePage } from "./pages/LivePage";

function useHashRoute() {
  const [hash, setHash] = useState(window.location.hash || "#/");
  useEffect(() => {
    const on = () => { setHash(window.location.hash || "#/"); window.scrollTo(0, 0); };
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return hash.replace(/^#/, "");
}

export function App() {
  const route = useHashRoute();
  const { status, connected } = useLive();
  const { data: rows = [] } = useQuery({ queryKey: ["incidents"], queryFn: api.incidents });
  const open = rows.find((r) => !TERMINAL.has(r.status));
  const ps = pipelineState(status, open);
  const name = status?.pipeline ?? "orders";
  const pretty = name.replace(/[_-]+/g, " ");
  const title = `${pretty.charAt(0).toUpperCase()}${pretty.slice(1)} pipeline`;
  const incidentId = route.startsWith("/incidents/") ? route.slice("/incidents/".length) : null;

  useEffect(() => {
    document.title = open?.status === "pr_open" ? `Review needed: ${name} pipeline` : `${title} console`;
  }, [open?.status, name, title]);

  const tab = (href: string, label: string, active: boolean) => (
    <a href={href} aria-current={active ? "page" : undefined}
       className={`rounded-md px-2.5 py-1 text-[14px] font-medium ${active ? "bg-panel text-ink shadow-[inset_0_0_0_1px_var(--line)]" : "text-ink-2 hover:text-ink"}`}>
      {label}
    </a>
  );

  return (
    <div className="mx-auto max-w-[1280px] px-4 pb-16 pt-5 sm:px-6">
      <header className="mb-5 flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <a href="#/" className="font-cond text-[24px] font-bold leading-none tracking-tight text-ink">{title}</a>
          <Pill tone={ps.tone} live={ps.tone === "agent"}>{ps.label}</Pill>
          <span className="hidden text-[13.5px] text-ink-3 sm:inline">{ps.detail}</span>
        </div>
        <nav className="flex items-center gap-1">
          {tab("#/", "Live", !incidentId && route !== "/benchmark")}
          {tab("#/benchmark", "Benchmark", route === "/benchmark")}
        </nav>
      </header>

      {!connected && status && (
        <p className="mb-4 rounded-md bg-fault-wash px-3 py-2 text-[14px] text-fault" role="status">
          Lost the live connection to the control plane. Reconnecting.
        </p>
      )}
      {!status && (
        <p className="mb-4 text-[14px] text-ink-2">
          Connecting to the control plane. Start it with <code className="rounded bg-panel px-1">python -m stream.server</code>.
        </p>
      )}

      <main>
        {incidentId ? <IncidentPage id={incidentId} /> : route === "/benchmark" ? <BenchPage /> : <LivePage />}
      </main>

      {status && (
        <footer className="mt-10 flex flex-wrap gap-x-6 gap-y-1 border-t border-line pt-4 text-[13px] text-ink-3">
          <span>Message bus: {status.bus_detail}</span>
          {status.models && <span>Diagnosis model: {status.models.diagnosis}</span>}
          {status.models && <span>Repair model: {status.models.repair}</span>}
          <span>Pull requests: {status.pr_mode === "github" ? "opened on GitHub" : "written locally (dry run)"}</span>
        </footer>
      )}
    </div>
  );
}
