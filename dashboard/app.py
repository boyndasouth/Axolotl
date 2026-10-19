"""Benchmark dashboard.

    streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DB = Path(__file__).resolve().parents[1] / "bench" / "results.duckdb"
# Categorical slots, fixed order (validated palette): one color per model label, never cycled.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]

st.set_page_config(page_title="Self-heal benchmark", page_icon="🩹", layout="wide")
st.title("Self-healing pipeline: mean time-to-repair")


@st.cache_data(ttl=10)
def load() -> pd.DataFrame:
    if not DB.exists():
        return pd.DataFrame()
    con = duckdb.connect(str(DB), read_only=True)
    try:
        return con.execute("SELECT * FROM runs ORDER BY started_at").df()
    finally:
        con.close()


df = load()
if df.empty:
    st.info("No benchmark results yet. Run `python -m bench.harness --runs 3` "
            "(or `--oracle` to smoke-test without an API key).")
    st.stop()

# ---- filters, one row above everything -------------------------------------------
df["mode"] = df["mode"].fillna("batch") if "mode" in df.columns else "batch"
labels = list(dict.fromkeys(df["label"]))            # first-seen order: stable colors
color_of = {lab: SERIES[i % len(SERIES)] for i, lab in enumerate(labels[:len(SERIES)])}
c0, c1, c2 = st.columns([1, 2, 1])
mode = c0.selectbox("Pipeline", sorted(df["mode"].unique(), reverse=True),
                    help="stream: break a live stream (bench.stream_harness). batch: one bad batch (bench.harness).")
df = df[df["mode"] == mode]
picked = c1.multiselect("Model configuration", labels, default=[l for l in labels if l != "oracle"] or labels)
batches = ["all"] + sorted(df["batch_id"].unique(), reverse=True)
batch = c2.selectbox("Benchmark batch", batches)
view = df[df["label"].isin(picked)]
if batch != "all":
    view = view[view["batch_id"] == batch]
if view.empty:
    st.warning("Nothing matches these filters.")
    st.stop()
if "oracle" in picked:
    st.caption("⚠️ **oracle** runs use a scripted stand-in for the LLM. They test the harness, not a model.")

# ---- headline numbers ---------------------------------------------------------------
repairs = view[view["expected_outcome"] == "repair"]
ok_repairs = repairs[repairs["success"]]
k = st.columns(5)
k[0].metric("Runs", len(view))
k[1].metric("Success rate", f"{view['success'].mean():.0%}")
k[2].metric("MTTR (median, repairs)",
            f"{ok_repairs['mttr_s'].median():.1f}s" if len(ok_repairs) else "–")
k[3].metric("False-fix rate", f"{repairs['false_fix'].mean():.0%}" if len(repairs) else "–",
            help="Patches that passed the sandbox but failed the hidden set.")
k[4].metric("LLM cost per repair",
            f"${ok_repairs['llm_cost_usd'].mean():.4f}" if len(ok_repairs) else "–")

if mode == "stream":
    s = st.columns(4)
    s[0].metric("Time to detect (median)", f"{view['time_to_detect_s'].median():.1f}s"
                if view["time_to_detect_s"].notna().any() else "–", help="Drift injected to incident opened.")
    s[1].metric("Time to recover (median)", f"{ok_repairs['time_to_recover_s'].median():.1f}s"
                if len(ok_repairs) and ok_repairs["time_to_recover_s"].notna().any() else "–",
                help="Drift injected to quarantined data replayed, excluding human review time.")
    q, rec = view["quarantined"].fillna(0).sum(), view["recovered"].fillna(0).sum()
    s[2].metric("Records recovered", f"{int(rec):,} of {int(q):,}" if q else "–")
    s[3].metric("Quarantined after deploy", f"{int(view['post_fix_dlq'].fillna(0).sum())}",
                help="Records still failing in the live check right after each fix went live.")

# ---- per-scenario charts ----------------------------------------------------------
by = (view.groupby(["label", "scenario", "expected_outcome"])
      .agg(runs=("success", "size"), success_rate=("success", "mean"),
           false_fixes=("false_fix", "sum"), attempts=("attempts", "mean"),
           mttr_p50=("mttr_s", lambda s: s[view.loc[s.index, "success"]].median()))
      .reset_index())
order = sorted(by["scenario"].unique())
multi = len(picked) > 1
enc_color = (alt.Color("label:N", scale=alt.Scale(domain=list(color_of), range=list(color_of.values())),
                       legend=alt.Legend(title=None, orient="top"))
             if multi else alt.value(color_of.get(picked[0], SERIES[0])))
bar = dict(cornerRadiusEnd=4, height={"band": 0.7})


def hbar(data: pd.DataFrame, x: str, title: str, fmt: str, tip: list):
    return (alt.Chart(data).mark_bar(**bar)
            .encode(y=alt.Y("scenario:N", sort=order, title=None),
                    yOffset=alt.YOffset("label:N") if multi else alt.Undefined,
                    x=alt.X(x, title=title, axis=alt.Axis(format=fmt, grid=True, gridOpacity=0.15)),
                    color=enc_color, tooltip=tip)
            .properties(height=max(220, 34 * len(order) * (len(picked) if multi else 1))))


left, right = st.columns(2)
with left:
    st.subheader("Time to repair, per scenario")
    st.caption("Median seconds from detection to a verified PR (or to escalation), successful runs only.")
    d = by.dropna(subset=["mttr_p50"])
    st.altair_chart(hbar(d, "mttr_p50:Q", "seconds", ".1f",
                         ["scenario", "label", alt.Tooltip("mttr_p50:Q", format=".1f", title="median s"),
                          alt.Tooltip("attempts:Q", format=".1f", title="avg attempts"), "runs"]),
                    width="stretch")
with right:
    st.subheader("Success rate, per scenario")
    st.caption("Repair: PR opened and the hidden set passes. Escalate / no-incident: the agent made that call.")
    st.altair_chart(hbar(by, "success_rate:Q", "success rate", ".0%",
                         ["scenario", "label", "expected_outcome",
                          alt.Tooltip("success_rate:Q", format=".0%"), "false_fixes", "runs"])
                    .encode(x=alt.X("success_rate:Q", title="success rate",
                                    scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format=".0%", gridOpacity=0.15))),
                    width="stretch")

st.subheader("Per-scenario table")
st.dataframe(by.rename(columns={"mttr_p50": "MTTR p50 (s)", "success_rate": "success"})
             .style.format({"success": "{:.0%}", "MTTR p50 (s)": "{:.1f}", "attempts": "{:.1f}"}),
             width="stretch", hide_index=True)

# ---- individual runs ----------------------------------------------------------------
st.subheader("Runs")
cols = ["started_at", "label", "scenario", "run_idx", "outcome", "success", "false_fix", "attempts",
        "mttr_s", "llm_calls", "llm_cost_usd", "sandbox_backend", "run_id"]
st.dataframe(view[cols].sort_values("started_at", ascending=False), width="stretch", hide_index=True)

run_id = st.selectbox("Inspect a run", view.sort_values("started_at", ascending=False)["run_id"])
r = view[view["run_id"] == run_id].iloc[0]
st.markdown(f"**{r['scenario']}** → `{r['outcome']}` after {r['attempts']} attempt(s). "
            f"Verifier: {r['final_summary'] or '–'}")
if r["escalation_reason"]:
    st.markdown(f"Escalated because: {r['escalation_reason']}")
if r["patch_diff"]:
    st.code(r["patch_diff"], language="diff")
