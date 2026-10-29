# Wikipedia edits pipeline: healing a pipeline on live data

A small pipeline the healer doesn't own: real Wikipedia edits, from Wikimedia's public
EventStreams feed, into Redpanda, transformed and checked against a contract, and loaded
into Postgres. A producer relays the live feed and can break it in six realistic ways,
so the agents face real-world records with a realistic upstream change in them.

```
Wikimedia EventStreams ──► producer/ ──► wiki.edits ──► healer (transform + contract) ──► Postgres
   (live, ~20-60/s)       (drift switch)   (Redpanda)          │                      analytics.wiki_changes
                                                              └─► wiki.edits.dlq (quarantine)
```

| Path | What it is | Agent can edit? |
|---|---|---|
| `etl/wiki/transform.py` | raw event -> one clean row | yes |
| `etl/wiki/mappings.yaml` | field renames, timestamp unit | yes |
| `etl/contracts/wiki.py` | the pandera contract | no (protected) |
| `tests/wiki/` | unit tests + recorded sample events, run in the sandbox | no |
| `producer/` | the upstream: live feed -> Redpanda, plus the drift switch | never sees it |
| `healer.yaml` | tells the healer all of the above | no |

## Run it (Windows PowerShell; macOS/Linux commands are the same apart from paths)

You need the healer project set up already (its venv, `docker build -t shp-sandbox:latest sandbox/`).
You'll use four terminals; activate the healer's venv in each one that runs Python.

### 1. Copy this folder out and make it its own repo

Outside OneDrive. The agent reads this repo's git history and opens PRs against it.

```powershell
Copy-Item -Recurse <healer project>\examples\wiki-edits-pipeline C:\dev\wiki-edits-pipeline
cd C:\dev\wiki-edits-pipeline
git init; git add .; git commit -m "wiki edits pipeline"
git tag baseline              # so you can reset to the original code between experiments
```

### 2. Terminal 1: Redpanda and Postgres

```powershell
cd <healer project>
docker compose up -d          # Redpanda :19092, Redpanda Console :8080, Postgres :5433
```

### 3. Point the healer at the new repo

Add to the healer project's `.env`:

```
WAREHOUSE_DSN=postgresql://healer:healer@localhost:5433/warehouse
SHP_WORKDIR=C:\dev\wiki-edits-pipeline
SHP_BUS=kafka
```

Remove `SHP_FAKE_LLM` (or leave it empty) so a real model does the work. Then check the config:

```powershell
cd <healer project>
python -m healer.spec check --root C:\dev\wiki-edits-pipeline
```

It should list `etl/wiki/mappings.yaml, etl/wiki/transform.py` as editable and end with `ok`.

### 4. Terminal 2: the healer

```powershell
cd <healer project>
python -m stream.server       # console on http://localhost:8100
```

The console now says "Wiki edits pipeline" and shows the Postgres table under Warehouse. Its
"Break the upstream" panel lists this pipeline's six drifts: the pipeline ships its own drift
switch (`producer/drifts.py`), so the console can flip it, and the producer in step 5 applies it.

### 5. Terminal 3: the live feed

```powershell
cd C:\dev\wiki-edits-pipeline
python producer\wiki_producer.py --dry-run --max-events 5 --contact you@example.com   # see the feed works
python producer\wiki_producer.py --contact you@example.com                            # then run it for real
```

Wikimedia asks every client to identify itself, so pass a real email or your GitHub URL.
Watch the console: records flow into the warehouse. Give it a minute before breaking anything,
so the live sample (real records and the rows they produced) fills up.

Too many events per second? `--wikis enwiki` keeps only English Wikipedia.

### 6. Terminal 4: break the upstream

```powershell
cd C:\dev\wiki-edits-pipeline
python producer\drift.py list
python producer\drift.py timestamp_ms
```

Or pick the drift in the console's "Break the upstream" panel; it flips the same switch.
Either way, the producer from step 5 has to be running for anything to change.

Within a few seconds the quarantine fills and an incident opens. The agents diagnose it, write
a fix, prove it in the sandbox against your real recent traffic, and open a PR (in dry-run mode
it's written to the healer project's `reports/prs/`). Approve it in the console and watch the
quarantine drain into Postgres. You can leave the drift on: a good fix handles both shapes.

To start the next experiment from the original code:

```powershell
python producer\drift.py off
git reset --hard baseline     # back to the original code (the healer hot-reloads it)
```

**Running it in place instead?** If you skipped step 1 and pointed `SHP_WORKDIR` at
`examples\wiki-edits-pipeline` inside the healer project, there is no separate repo here, and
`git reset --hard baseline` would reset the **whole healer project**. Restore only this
pipeline's editable files from the healer repo's `baseline` tag:

```powershell
python producer\drift.py off
git checkout baseline -- etl/wiki     # run from this folder; only the transform and mappings
```

If a heal was merged on GitHub and pulled, commit that restore so the next PR branches from it.

## The six drifts

| Scenario | What upstream does | What the healer should do |
|---|---|---|
| `rename_user` | `user` becomes `performer` | repair |
| `timestamp_ms` | Unix seconds become milliseconds | repair |
| `timestamp_iso` | `timestamp` becomes an ISO 8601 string | repair |
| `length_flattened` | `length: {old, new}` becomes `length_old` / `length_new` | repair |
| `missing_titles` | about 5% of events arrive without a title | repair: drop rows that can't be fixed; the rest keep loading (category `bad_rows`, not an escalation) |
| `bot_as_string` | `bot` becomes the strings `"true"` / `"false"` | **nothing**: every value still passes the contract |

`missing_titles` tests judgement more than code. No fix can bring a missing title back, so a
diagnosis that reasons record by record calls it unrepairable and hands it to a human. The
right call is to look at the rate: a few percent of unusable records is bad rows, and the fix
drops them. The verifier refuses a fix that drops more than 20% of the batch that broke, so
a much heavier version of this drift would still be escalated.

`bot_as_string` is there on purpose. `bool("false")` is `True` in Python, so every human edit
silently becomes a bot edit, and no contract check or regression test catches it. It's the
honest limit of contract-based healing, and a good thing to write about: the fix is a stricter
contract (or a distribution check, such as "the share of bot edits jumped from 15% to 100%").

Two more things to try:
- **Outage:** stop the producer (Ctrl+C). After about 12 seconds of silence the healer escalates
  instead of trying to fix code, because there's nothing in the code to fix.
- **Real surprises:** if an incident opens without any drift switched on, you've found a real
  quirk in Wikipedia's data. That's the most interesting result of all; keep the PR.

## Real pull requests

Create a GitHub repo for this pipeline, push to it (`git remote add origin ...; git push -u origin main`),
and set `GITHUB_TOKEN` and `GITHUB_REPO=<you>/wiki-edits-pipeline` in the healer's `.env`. PRs then open
against this repo, not the healer's. If GitHub fails (bad token, wrong repo name, no remote),
the PR is written to the healer's `reports/prs/` instead, the console shows why, and you can
still approve the fix from there.

## Offline and repeatable runs

Record real traffic once, then replay it whenever you like (demos, benchmarks, no network):

```powershell
python producer\wiki_producer.py --record recordings\edits.jsonl --max-events 5000 --contact you@example.com
python producer\wiki_producer.py --replay recordings\edits.jsonl --rate 30
```

The drift switch works the same during a replay. Each pass through the recording gets fresh ids,
so the sink doesn't ignore them as duplicates.

## Tests

```powershell
python -m pytest tests\wiki producer
```
