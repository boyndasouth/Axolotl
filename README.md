# Self-Healing Data Pipeline Agent

Data pipelines break silently: an upstream API renames a field, changes a type, or starts sending bad rows. This project runs an orders pipeline **as a live stream** and heals it while it runs. It notices a problem within about a second, quarantines the bad records instead of stopping, diagnoses the cause, writes a fix, **proves the fix in a sandbox before anyone sees it**, and opens a pull request with the evidence. When a human approves it, the fix goes live without a restart and every quarantined record is replayed through it. No data is lost.

It also measures itself: a chaos suite breaks the stream on purpose in nine ways, and a benchmark reports time to detect, **time to a verified fix**, time to recover the data, success rate and **false-fix rate** for whichever models you plug in.

```mermaid
flowchart LR
    U[Upstream<br/>producer] -->|orders topic| P[Stream processor<br/>transform + contract]
    P -->|valid| W[(Warehouse<br/>idempotent upsert)]
    P -->|invalid| Q[(Quarantine<br/>orders.dlq)]
    P -. rolling window .-> M[Detector<br/>no LLM]
    M -->|one incident<br/>per root cause| D[Diagnosis agent]
    D -->|outage or<br/>low confidence| X[Escalate]
    D --> R[Repair agent]
    R --> V[Sandbox verify<br/>tests · live batch ·<br/>held-out · regressions]
    V -->|fails + feedback| R
    V -->|passes| PR[Pull request]
    PR --> H{Human<br/>review}
    H -->|approve| L[Hot reload]
    L --> RP[Replay quarantine] --> C[Live check]
```

## What makes it more than a demo

- **It never stops on bad data.** Each micro-batch is transformed and checked against the contract. Valid rows load, invalid rows go to a dead-letter queue (`orders.dlq`) with the reason they failed. The pipeline keeps flowing while the agents work.
- **Real-time detection, one incident per root cause.** A deterministic detector watches a rolling 10-second window: enough records quarantined, or upstream gone silent. A rename that breaks thousands of records opens one incident, not thousands, and the same failure can't re-alert until it has cleared.
- **Verification before action.** Pass/fail is decided by code, never by an LLM. A patch reaches a PR only if the unit tests pass, the live batch that broke now meets the contract, a held-out batch matches the expected output row for row, and nothing that worked before regresses.
- **Sandboxed execution.** Each candidate runs in a throwaway Docker container with no network, capped memory, CPU and processes, and a timeout.
- **The agent can't cheat.** It may only edit the files `healer.yaml` lists as `editable` (in the demo, `pipeline/transforms/` and `pipeline/mappings.yaml`). The data contract and the tests are off-limits, so it can't loosen the rules until the data passes. A `healer.yaml` whose editable patterns reach the contract or the tests is rejected at startup. It never sees the fixtures it's graded on.
- **Real traffic is the regression suite.** Clean micro-batches feed a live sample: real records and the rows the current code made from them. A fix must reproduce every one, so a patch that passes the tests but quietly changes rows that were already right never reaches a PR.
- **Works on your pipeline, not just the demo.** Everything about the pipeline (topics, transform, contract, key, editable files, sink) comes from `healer.yaml`. DuckDB and Postgres sinks are built in.
- **Zero data loss, and proof of it.** After a merge the processor hot-reloads the code, replays the quarantine through it, and watches the live stream for a few seconds. The incident is only "resolved" when nothing related is still failing. The sink upserts on `order_id`, so replays and upstream resends never double-load.
- **It knows when not to fix, and when a fix is just dropping rows.** Upstream going silent is an outage, so it escalates. A new harmless field isn't an incident at all. A few records missing a required value can't be repaired one by one, but that isn't a reason to page a human: the fix drops them so the rest keep loading. The verifier refuses a fix that drops more than 20% of the batch that broke. All of these are scored.
- **Measured, including the bad case.** A hidden test set, never shown to the sandbox, catches patches that pass verification but don't generalize. That's the false-fix rate.

## Quick start

Python 3.12 recommended. On Windows, keep the project **outside OneDrive**; synced folders and virtual environments don't mix.

```powershell
# Windows PowerShell (macOS/Linux: python3 -m venv .venv && source .venv/bin/activate)
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env          # then add an API key, e.g. GROQ_API_KEY
docker build -t shp-sandbox:latest sandbox/
docker compose up -d            # Redpanda (Kafka API) + Redpanda Console
git init; git add .; git commit -m "initial commit"   # the diagnosis agent reads git history
python -m stream.server         # open http://localhost:8100
```

That's it: the console at **http://localhost:8100** shows the stream flowing. Pick a change under **Break the upstream**, watch the quarantine tank fill and the agents work through the incident, then approve the fix and watch the tank drain. **http://localhost:8080** is Redpanda Console, where you can watch the `orders` and `orders.dlq` topics directly.

No API key yet? Set `SHP_FAKE_LLM=oracle` and a scripted stand-in plays both agents, so you can see the whole loop. No Docker? Set `SHP_BUS=memory` (everything runs in one process, no broker) and `SHP_SANDBOX_BACKEND=local` (same checks, no isolation):

```powershell
$env:SHP_FAKE_LLM="oracle"; $env:SHP_BUS="memory"; $env:SHP_SANDBOX_BACKEND="local"
python -m stream.server
```

The console is prebuilt in `web/dist`, so you don't need Node to run it. To change it: `cd web && npm install && npm run dev` serves it on :5173 with live reload, talking to the server on :8100; `npm run build` updates `web/dist`.

Run the tests with `python -m pytest`: about 65 tests, a couple of minutes. They cover the batch agent, the streaming pipeline, the Kafka code path (against librdkafka's built-in mock broker), the control-plane API, `healer.yaml` and its safety rules, PR fallback, the whole live loop on a second, non-demo pipeline, and the expected repair for the Wikipedia example's `missing_titles` drift. Set `SHP_TEST_POSTGRES_DSN=postgresql://...` to run the Postgres sink tests too (CI does).

Merged self-heal PRs change `pipeline/`, and the running app rewrites `data/schema_snapshots/`. Tests and the benchmark always start from the original, unhealed pipeline anyway: every throwaway working copy is overlaid with the frozen copy in `chaos/baseline/`. Don't let the agent edit that directory, and don't update it when you merge a heal. The Wikipedia example's test keeps its own frozen transform in `tests/healer/wiki_baseline/` for the same reason.

## How the live loop works

1. **Upstream** (`stream/producer.py`) publishes a page of orders about once a second to the `orders` topic. The drift switch is checked before every page, so an injected change starts mid-stream, like a real upstream deploy.
2. **The processor** (`stream/processor.py`) consumes micro-batches and runs the same transform and contract as the batch pipeline. Valid rows are upserted into DuckDB; invalid ones go to `orders.dlq` with their failure reason. Offsets are committed after the sink write and the DLQ publish, so delivery is at-least-once and the upsert makes it effectively exactly-once.
3. **The detector** (`stream/detector.py`) checks the rolling window every half second. When it fires, it snapshots the most recent raw records (old and new shapes mixed, exactly as they arrived) and builds the same run report the batch pipeline produces. The diagnosis and repair agents work unchanged.
4. **The agents** (`agent/`, LangGraph) diagnose, repair and verify in the sandbox, retrying with the verifier's feedback, then open a PR (or write it to `reports/prs/` in dry-run mode) and pause for a human. If GitHub fails (auth, remote, network), the verified fix isn't lost: the PR is written locally instead and the console says why.
5. **The decision** comes from the console's *Approve and deploy* button, or from GitHub when the PR is merged (the server's `/github` webhook route).
6. **Rollout** (`agent/nodes/rollout.py` plus the runtime's hook): the patch is applied (in dry-run mode, committed to your local repo; in GitHub mode, `git pull`), the processor hot-reloads it, the DLQ is replayed, and the live stream is watched for a few seconds. Only then is the incident resolved.

Only one incident is worked at a time, so two repairs never edit the same files at once. Other failures keep being quarantined meanwhile, and nothing is lost.

## Heal your own pipeline

The healer doesn't own your pipeline; it sits next to it. Describe the pipeline in a `healer.yaml` at the root of the repo that holds your transform code. `examples/healer.postgres.yaml` is a starting point, and the demo's own `healer.yaml` is fully commented.

| Key | What it is |
|---|---|
| `transform` | `path/to/file.py:function`, called as `function(records, mappings)`, returns a DataFrame |
| `mappings` | optional YAML passed to the transform: renames, formats |
| `contract` | `path/to/file.py:Schema`, a pandera `DataFrameSchema` the output must meet |
| `key` | the column that identifies a record; the sink upserts on it |
| `editable` | glob patterns for the files the repair agent may change |
| `read_only`, `tests` | shown to the agents, never editable; `tests` runs in the sandbox on every patch |
| `code` | the paths copied into the sandbox |
| `source`, `quarantine` | the Kafka topic to consume and the dead-letter topic |
| `sink` | `duckdb` (a local file) or `postgres` (`dsn: ${WAREHOUSE_DSN}`); the table is created from the contract |
| `holdout` | `live_sample` rows from real traffic, plus `fixtures` if you have hand-made ones |

```bash
python -m healer.spec check --root /path/to/your/repo   # validates it and prints what the agent may touch
export WAREHOUSE_DSN=postgresql://user:pass@host:5432/analytics KAFKA_BOOTSTRAP=broker:9092
SHP_WORKDIR=/path/to/your/repo SHP_BUS=kafka python -m stream.server
```

**A worked example on live data:** `examples/wiki-edits-pipeline/` heals a pipeline that ingests real Wikipedia edits from Wikimedia's public stream into Postgres, with a producer that can break the real feed in six realistic ways. Its README walks through it step by step.

**Breaking your own upstream from the console.** By default the console's *Break the upstream* panel only appears with the demo producer, because a real topic isn't the console's to break. A pipeline can opt in by shipping a drift switch at `producer/drifts.py` with `SCENARIOS` (id to a description and the expected outcome), `current()` and `set_drift(name)`. The console then lists those scenarios and flips the switch, and the pipeline's own producer applies it. The Wikipedia example does this.

Two things the healer is careful about on real data. The built-in fake upstream only runs when `source.demo_producer: true`, so nothing fake is ever published to your topic. And if the warehouse goes away, the processor waits and retries instead of moving past rows it couldn't write; offsets are committed only once the rows have landed.

## Benchmark

```powershell
python -m bench.stream_harness --runs 3 --label "groq llama-3.3-70b"   # break a live stream
python -m bench.harness --runs 3 --label "groq llama-3.3-70b"          # batch version
```

The console's **Benchmark** tab shows the results; `streamlit run dashboard/app.py` has more charts.

Each streaming run gets a private copy of everything (checkout, in-memory bus, producer, processor, detector, agents), warms up on clean traffic, then injects the scenario mid-stream. It approves PRs instantly as the "human" and reports:

| Metric | From | To |
|---|---|---|
| Time to detect | drift injected | incident opened |
| Time to verified PR (MTTR) | incident opened | verified PR opened |
| Time to recover | drift injected | quarantine replayed through the fix, review time excluded |
| Records recovered | quarantined | loaded, or correctly dropped by the fix |
| Quarantined after deploy | | should be 0 |

`--oracle` swaps the LLM for a script so you can check the plumbing without an API key. Oracle numbers measure the harness, not a model. As a reference point, an oracle run here detected drift in about 1.3 s (mostly waiting for upstream's next page), reached a verified PR in about 3 s, and recovered every quarantined record in all seven repair scenarios.

| Scenario | What upstream does | Expected outcome |
|---|---|---|
| `rename_column` | `cust_id` becomes `client_id` | repair |
| `currency_string` | `12.5` becomes `"€12.50"` | repair |
| `date_format` | `2026-10-01` becomes `10/01/2026` | repair |
| `nested_amount` | amount and currency become `{"value", "currency"}` | repair |
| `status_case` | `shipped` becomes `SHIPPED` | repair |
| `bad_rows` | ~5% null or negative amounts | repair |
| `duplicate_rows` | ~10% of rows resent | repair |
| `upstream_outage` | upstream goes silent | escalate |
| `additive_field` | new `channel` field | no incident |

## Configuration

Everything is an environment variable; `.env.example` lists them all. The ones you're most likely to change:

| Variable | Default | What it does |
|---|---|---|
| `SHP_BUS` | `auto` | `kafka` (Redpanda), `memory` (one process, no broker) or `auto` |
| `DIAGNOSIS_MODEL`, `REPAIR_MODEL` | Groq Llama 3.3 70B | any LiteLLM model string |
| `SHP_FAKE_LLM` | empty | `oracle` for the scripted stand-in |
| `SHP_RATE` | `25` | upstream records per second (also a slider in the console) |
| `SHP_WINDOW_S`, `SHP_MIN_BAD`, `SHP_MIN_DLQ_RATE`, `SHP_SILENCE_S` | 10, 5, 0.02, 12 | detection thresholds |
| `GITHUB_TOKEN`, `GITHUB_REPO` | empty | real pull requests instead of dry-run files |

### Real pull requests

Set `GITHUB_TOKEN` (fine-grained: contents and pull requests, read and write) and `GITHUB_REPO=owner/name` in `.env`. The agent branches as `self-heal/<incident>`, commits only the verified patch, pushes, and opens the PR. CI (`.github/workflows/ci.yml`) then re-runs every test on it, outside anything the agent controls.

The agent refuses to branch only if you have uncommitted edits to the files the patch touches. Other local changes don't block it, including the schema snapshot the running stream rewrites. Finish any merge in progress first, though. If anything on the GitHub side fails, the PR is written to `reports/prs/` instead, the console shows the error, and you can still approve the fix there.

To deploy on merge, add a GitHub webhook for "Pull requests" pointing at `http://<your tunnel>/github` (ngrok or smee.io in front of port 8100), with `GITHUB_WEBHOOK_SECRET` as the secret. The *Approve and deploy* button still works too.

## Batch mode

The original batch pipeline is still here, and the batch benchmark still uses it:

```powershell
python -m chaos.inject rename_column      # writes a drifted batch to data/incoming/orders.json
python -m agent.cli run --source file:data/incoming/orders.json --scenario rename_column
python -m agent.cli resume <incident_id> --merged
```

`docker compose --profile batch up` starts the fake upstream API and the Streamlit dashboard. Dagster is optional now (`pip install -r requirements-dagster.txt`, `dagster dev -f pipeline/definitions.py`); its 5-minute schedule is gone, since polling is the opposite of real time.

## Layout

```
stream/          the live system: bus (Kafka or in-memory), producer, processor + DLQ + replay +
                 live sample, sinks (DuckDB, Postgres), detector, metrics, runtime, server
web/             the live console: React, TypeScript, Vite, Tailwind (prebuilt in web/dist)
healer/          the pipeline spec: loads healer.yaml, enforces what the agent may edit,
                 `python -m healer.spec check`
pipeline/        the demo pipeline being healed: transforms (editable), mappings (editable), schemas
                 (the contract), batch run, DuckDB load, optional Dagster definitions
agent/           LangGraph graph and state, nodes (monitor, diagnose, repair, verify, PR,
                 rollout), prompts, context tools, LiteLLM wrapper, CLI, GitHub webhook
sandbox/         Docker image, runner (decides pass/fail), in-sandbox evaluator
chaos/           scenarios, drift functions, the shared drift switch, injector, fixtures, and
                 baseline/: the frozen, unhealed demo pipeline that tests and benchmarks start from
bench/           streaming and batch harnesses, oracle, throwaway working copies
upstream_api/    fake HTTP source for batch mode
dashboard/       Streamlit benchmark dashboard
examples/        healer.yaml for your own pipeline (Kafka in, Postgres out), and a complete
                 live-data example: Wikipedia edits -> Redpanda -> Postgres
tests/           pipeline unit tests (also run inside the sandbox), agent and streaming tests
```

## Design decisions

- **Quarantine, don't halt.** A streaming pipeline that stops on bad data loses or delays everything behind it. Routing failures to a DLQ keeps the good data flowing and turns recovery into a replay.
- **Redpanda for the broker.** It speaks the Kafka API, so the code uses the standard `confluent-kafka` client, but it's one container with no JVM or ZooKeeper, which matters on a laptop. Every topic has one partition, which keeps ordering simple at this scale.
- **The same agents for batch and stream.** The detector produces the same run report the batch monitor does, so diagnosis, repair and verification didn't change. The only new graph node is the rollout after a merge, and it's a no-op without a streaming runtime.
- **Live data makes verification stronger.** The incident batch is the stream's most recent records, old and new shapes mixed. That caught a reference fix that renamed whole columns (it broke on mixed batches), and an overfitted currency fix (live amounts include thousands separators).
- **Monitor and verify have no LLM.** Detection must be cheap and predictable, and the gate must be impossible to talk past.
- **LangGraph over a crew framework.** The workflow is a state machine with hard gates, a retry loop that feeds verifier output back to the repair agent, and a pause for a human. Explicit edges and `interrupt()` fit that better than role-playing agents.
- **Server-Sent Events for the console.** Data only flows server to browser, the browser reconnects on its own, and missed events are replayed from the event log by id. Actions are ordinary POSTs.
- **Whole-file patches, not diffs.** Models produce complete files more reliably than applicable diffs. The diff for the PR is computed afterwards.

## Limits and next steps

- The `local` sandbox backend is not isolated. Use Docker for anything you didn't write.
- One control-plane process (run uvicorn with a single worker). Scaling out would mean partitioning `orders` by `order_id`, a consumer group of processors, and Redis or Postgres for the event fan-out.
- Duplicates are caught by the contract within a micro-batch; across micro-batches the sink silently ignores them. A stateful dedupe window would make cross-batch duplicates visible too.
- Held-out golden checks need a known scenario. On real, un-injected incidents, verification relies on the tests, the contract, the live batch, regressions and the live sample.
- The live sample trusts the code that produced it: if the current code silently produces wrong-but-valid rows, the sample keeps them. It guards against changing what works, not against errors nobody has noticed.
- One pipeline per control-plane process (`SHP_PIPELINE` picks it). Hot reload covers the transform file and mappings; helper modules the transform imports are not reloaded.
- The demo producer, the chaos scenarios and the benchmark are orders-specific; the healer itself isn't.
- Ideas: canary deploys (run the fix on the DLQ into a staging table before merge), a rule-based repair baseline to compare against the LLM, tool-calling diagnosis, and Langfuse traces of each repair.
