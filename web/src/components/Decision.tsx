import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api, type Incident } from "../api";
import { fmtInt } from "../format";
import { Button } from "./bits";

// The one place a human acts: approve the verified fix (deploy + replay) or reject it.
export function Decision({ inc, waiting, prMode }: { inc: Incident; waiting: number; prMode?: string }) {
  const qc = useQueryClient();
  const [busy, setBusy] = useState<null | "approve" | "reject">(null);
  const [error, setError] = useState<string | null>(null);
  const passed = inc.attempts.find((a) => a.verification.passed);

  async function decide(merged: boolean) {
    setBusy(merged ? "approve" : "reject");
    setError(null);
    try {
      await api.decide(inc.id, merged);
      qc.invalidateQueries({ queryKey: ["incident", inc.id] });
    } catch (e) {
      setError((e as Error).message);
      setBusy(null);
    }
  }

  return (
    <div className="rounded-lg border-2 border-quarantine bg-quarantine-wash p-4">
      <p className="font-cond text-[17px] font-semibold text-ink">A verified fix is ready for your review</p>
      <p className="mt-1 max-w-[70ch] text-[14px] text-ink-2">
        {passed ? `Attempt ${passed.attempt} passed the unit tests, the live batch that broke, the held-out data and the regression suite. ` : ""}
        Approving it deploys the fix to the running processor, then replays the{" "}
        <span className="num font-medium text-ink">{fmtInt(waiting)}</span> quarantined records through it.
        {prMode === "github" ? " Merging the pull request on GitHub does the same." : ""}
      </p>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button kind="primary" onClick={() => decide(true)} disabled={!!busy}>
          {busy === "approve" ? "Deploying" : "Approve and deploy"}
        </Button>
        <Button kind="danger" onClick={() => decide(false)} disabled={!!busy}>
          {busy === "reject" ? "Rejecting" : "Reject"}
        </Button>
        {inc.pr_url?.startsWith("http") && (
          <a className="text-[14px] text-flow underline underline-offset-2" href={inc.pr_url} target="_blank" rel="noreferrer">
            Open the pull request
          </a>
        )}
      </div>
      {error && <p className="mt-2 text-[13.5px] text-fault" role="alert">{error}</p>}
    </div>
  );
}
