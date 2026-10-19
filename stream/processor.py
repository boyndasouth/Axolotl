"""The streaming pipeline: consume records in micro-batches, transform, check the contract,
load the good rows, quarantine the bad ones. It never stops on bad data.

    source topic ──► transform ──► contract ──┬─► sink (idempotent upsert on the key)
                                              └─► quarantine topic (the record + why it failed)

What the pipeline is (topics, transform, contract, key, sink) comes from healer.yaml
in the working directory; with none, the demo's orders pipeline.

Delivery is at-least-once: offsets are committed only after the sink write and the
DLQ publish, and the sink ignores keys it already has, so a crash and replay never
double-loads. If the sink is down, the processor waits and retries rather than move
past rows it couldn't write. The transform is read from the working directory and
hot-reloaded when it changes, which is how a merged fix goes live without a restart.

Clean micro-batches also feed the live sample: real raw records and the rows the
current code made from them. The sandbox requires every fix to reproduce those rows.
"""

from __future__ import annotations

import importlib.util
import json
import os
import random
import re
import threading
import time
import traceback
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
import yaml

from healer.spec import PipelineSpec, load_spec
from pipeline import fingerprint
from stream.jsonlog import JsonLog
from stream.sinks import SinkUnavailable, make_sink

PROCESSOR_GROUP = "shp-processor"
REPLAY_GROUP = "shp-dlq-replay"
LIVE_MODULE_PREFIX = "shp_live_"


# ---- failure signatures -----------------------------------------------------------

def _message(e: BaseException) -> str:
    if e.args and isinstance(e.args[0], str):
        return e.args[0]
    return str(e)


def normalize_message(msg: str, limit: int = 70) -> str:
    """Stable text for grouping: keep the prose, drop the values that vary per record."""
    head = re.sub(r'"[^"]*"', "…", msg)
    head = re.sub(r"(?<![A-Za-z])'[^']*'", "…", head)      # quoted values, not "doesn't"
    head = re.split(r": |, |\. |\n", head, maxsplit=1)[0]
    head = re.sub(r"\d+", "N", head)
    return head.strip()[:limit]


def transform_signature(e: BaseException) -> str:
    return f"transform:{type(e).__name__}:{normalize_message(_message(e))}"


def contract_signature(column: str | None) -> str:
    return f"contract:{column or 'table'}"


# ---- live code --------------------------------------------------------------------

class LiveCode:
    """The transform and mappings, loaded from the working directory by file path.
    The contract is loaded once: it is protected, so a merge can never change it."""

    def __init__(self, workdir: Path, spec: PipelineSpec | None = None):
        self.workdir = Path(workdir)
        self.spec = spec or load_spec(self.workdir)
        self.transform_path = self.workdir / self.spec.transform.path
        self.transform_attr = self.spec.transform.attr
        self.mappings_path = self.workdir / self.spec.mappings if self.spec.mappings else None
        self.contract = self.spec.load_contract(self.workdir)
        self.key = self.spec.key
        self.version = 0
        self.loaded_at = 0.0
        self.load_error: str | None = None
        self._sig = None
        self.transform = None
        self.mappings: dict = {}
        self.load()

    def _signature(self):
        paths = {self.transform_path, *(self.workdir / f for f in self.spec.editable_files())}
        if self.mappings_path:
            paths.add(self.mappings_path)
        return tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in sorted(paths) if p.exists())

    def changed(self) -> bool:
        return self._signature() != self._sig

    def load(self) -> bool:
        sig = self._signature()
        try:
            spec = importlib.util.spec_from_file_location(
                f"{LIVE_MODULE_PREFIX}{self.spec.name.replace('-', '_').replace('.', '_')}_v{self.version + 1}",
                self.transform_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mappings = (yaml.safe_load(self.mappings_path.read_text(encoding="utf-8")) or {}) \
                if self.mappings_path else {}
            self.transform, self.mappings = getattr(mod, self.transform_attr), mappings
            self.version += 1
            self.loaded_at = time.time()
            self.load_error = None
            ok = True
        except Exception as e:      # a broken merge: keep running the last good code
            self.load_error = f"{type(e).__name__}: {e}"
            ok = False
        self._sig = sig
        return ok


# ---- micro-batch processing -------------------------------------------------------

@dataclass
class BatchResult:
    rows_in: int
    good: pd.DataFrame | None
    failed: list[tuple[dict, dict]] = field(default_factory=list)   # (raw record, reason)
    filtered: int = 0                                               # dropped by the transform on purpose
    transform_error: dict | None = None
    contract_failures: list[dict] = field(default_factory=list)


def _workdir_traceback(e: BaseException, workdir: Path) -> str:
    frames = [f for f in traceback.extract_tb(e.__traceback__)
              if str(workdir) in f.filename or LIVE_MODULE_PREFIX in f.filename]
    frames = [f for f in frames if "site-packages" not in f.filename]
    return "".join(traceback.format_list(frames[-5:]))


def process_records(records: list[dict], code: LiveCode) -> BatchResult:
    """Pure function of (records, code): what loads, what is quarantined, and why."""
    res = BatchResult(rows_in=len(records), good=None)
    if not records:
        return res
    try:
        df = code.transform(records, code.mappings)
    except Exception as e:
        tb = _workdir_traceback(e, code.workdir)
        sig = transform_signature(e)
        res.transform_error = {"type": type(e).__name__, "message": _message(e)[:500],
                               "traceback": tb, "signature": sig}
        reason = {"stage": "transform", "signature": sig, "type": type(e).__name__,
                  "message": _message(e)[:300]}
        res.failed = [(r, reason) for r in records]
        return res

    res.filtered = max(0, len(records) - len(df))
    key = code.key
    try:
        code.contract.validate(df, lazy=True)
        res.good = df
        return res
    except pa.errors.SchemaErrors as e:
        fc = e.failure_cases
    except Exception as e:          # the transform returned something that isn't a table
        sig = transform_signature(e)
        reason = {"stage": "transform", "signature": sig, "type": type(e).__name__,
                  "message": _message(e)[:300]}
        res.transform_error = {"type": type(e).__name__, "message": _message(e)[:500],
                               "traceback": "", "signature": sig}
        res.failed = [(r, reason) for r in records]
        return res

    for (col, chk), grp in fc.groupby(["column", "check"], dropna=False):
        res.contract_failures.append({
            "column": None if col != col else str(col), "check": str(chk), "count": int(len(grp)),
            "examples": [str(x) for x in grp["failure_case"].head(5).tolist()]})

    row_level = fc["index"].notna().all() and key in df.columns
    raw_ids = [str(r.get(key)) for r in records]
    if not row_level or any(i == "None" for i in raw_ids):
        # a table-level failure (wrong columns, wrong types): the whole micro-batch is suspect
        first = fc.iloc[0]
        col = None if first["column"] != first["column"] else str(first["column"])
        reason = {"stage": "contract", "signature": contract_signature(col), "column": col,
                  "check": str(first["check"]), "value": str(first["failure_case"])[:200]}
        res.failed = [(r, reason) for r in records]
        return res

    bad_idx = set(int(i) for i in fc["index"].dropna().tolist())
    reasons_by_id: dict[str, dict] = {}
    for _, row in fc.iterrows():
        oid = str(df.loc[int(row["index"]), key])
        col = None if row["column"] != row["column"] else str(row["column"])
        reasons_by_id.setdefault(oid, {"stage": "contract", "signature": contract_signature(col),
                                        "column": col, "check": str(row["check"]),
                                        "value": str(row["failure_case"])[:200]})
    res.good = df.drop(index=[i for i in bad_idx if i in df.index])
    res.failed = [(r, reasons_by_id[str(r.get(key))]) for r in records
                  if str(r.get(key)) in reasons_by_id]
    return res


# ---- live sample -------------------------------------------------------------------------

class LiveSample:
    """A reservoir of real records from clean micro-batches, with the rows the code
    produced from them. A uniform sample over time, so formats from weeks ago stay
    represented (during a migration upstream often sends both shapes)."""

    def __init__(self, path: Path, key: str, rows: int, seed: int = 0):
        self.dir, self.key, self.cap = Path(path), key, rows
        self.raw: list[dict] = []
        self.expected: list[dict] = []
        self.seen = 0
        self._keys: set[str] = set()
        self._rng = random.Random(seed)
        self._dirty = False
        self._lock = threading.Lock()
        try:
            raw = json.loads((self.dir / "raw.json").read_text(encoding="utf-8"))
            exp = json.loads((self.dir / "expected.json").read_text(encoding="utf-8"))
            meta = json.loads((self.dir / "meta.json").read_text(encoding="utf-8"))
            if len(raw) == len(exp):
                self.raw, self.expected = raw[:rows], exp[:rows]
                self.seen = int(meta.get("seen", len(self.raw)))
                self._keys = {str(r.get(key)) for r in self.raw}
        except (OSError, ValueError):
            pass

    def add(self, records: list[dict], clean: pd.DataFrame) -> int:
        """Pair each raw record with its clean row by key; returns how many were paired."""
        if not self.cap or clean is None or clean.empty or self.key not in clean.columns:
            return 0
        rows = json.loads(clean.to_json(orient="records", date_format="iso"))
        by_key = {str(r[self.key]): r for r in rows}
        n = 0
        with self._lock:
            for rec in records:
                k = str(rec.get(self.key))
                row = by_key.get(k)
                if row is None or k in self._keys:
                    continue
                self.seen += 1
                n += 1
                if len(self.raw) < self.cap:
                    self.raw.append(rec)
                    self.expected.append(row)
                    self._keys.add(k)
                else:
                    j = self._rng.randrange(self.seen)
                    if j < self.cap:
                        self._keys.discard(str(self.raw[j].get(self.key)))
                        self.raw[j], self.expected[j] = rec, row
                        self._keys.add(k)
            self._dirty = self._dirty or n > 0
        return n

    def save(self) -> Path | None:
        """Write raw.json / expected.json (atomically) and return the directory, or None if empty."""
        with self._lock:
            if not self.raw:
                return None
            if self._dirty or not (self.dir / "raw.json").exists():
                self.dir.mkdir(parents=True, exist_ok=True)
                for name, data in (("raw", self.raw), ("expected", self.expected),
                                   ("meta", {"seen": self.seen, "rows": len(self.raw), "key": self.key,
                                             "saved_at": time.time()})):
                    tmp = self.dir / f".{name}.json.tmp"
                    tmp.write_text(json.dumps(data, default=str), encoding="utf-8")
                    os.replace(tmp, self.dir / f"{name}.json")
                self._dirty = False
        return self.dir

    def __len__(self) -> int:
        return len(self.raw)


# ---- the processor ----------------------------------------------------------------

class StreamProcessor:
    def __init__(self, bus, workdir: Path, metrics, data_dir: Path | None = None,
                 max_batch: int = 500, poll_timeout: float = 0.5, on_event=None):
        self.bus = bus
        self.workdir = Path(workdir)
        self.metrics = metrics
        self.max_batch = max_batch
        self.poll_timeout = poll_timeout
        self.on_event = on_event or (lambda kind, data: None)
        self.spec = load_spec(self.workdir)
        self.topic, self.dlq_topic = self.spec.source_topic, self.spec.dlq_topic
        self.code = LiveCode(self.workdir, self.spec)
        data_dir = Path(data_dir or self.workdir / "data")
        self.sink = make_sink(self.spec, data_dir)
        self.log = JsonLog(self.workdir / "logs" / "pipeline.jsonl")   # where the diagnosis agent reads
        self.snapshot_path = data_dir / "schema_snapshots" / f"{self.spec.name}.json"
        self.known_fp = fingerprint.load_snapshot(self.snapshot_path)
        self.live_sample = LiveSample(data_dir / "holdout" / self.spec.name, self.spec.key,
                                      self.spec.live_sample_rows)
        self.sink_down_since: float | None = None

        self.recent_raw: deque = deque(maxlen=1000)      # (ts, record) as they arrived
        self.recent_dlq: deque = deque(maxlen=3000)      # (ts, record, reason)
        self.last_transform_error: dict[str, dict] = {}  # signature -> error with traceback
        self.lock = threading.RLock()                    # one micro-batch, reload or replay at a time
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._consumer = None
        self._replayer = None
        self.batches = 0

    # -- lifecycle --
    def start(self) -> "StreamProcessor":
        self._consumer = self.bus.consumer(PROCESSOR_GROUP, self.topic)
        self._replayer = self.bus.consumer(REPLAY_GROUP, self.dlq_topic)
        self._thread = threading.Thread(target=self._loop, name="processor", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        for c in (self._consumer, self._replayer):
            try:
                c and c.close()
            except Exception:
                pass
        try:
            self.live_sample.save()
        except OSError:
            pass
        self.sink.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                msgs = self._consumer.poll(max_messages=self.max_batch, timeout=self.poll_timeout)
                if msgs:
                    with self.lock:
                        self._handle(msgs)
                    self._consumer.commit()
            except Exception as e:      # never die: log it and keep consuming
                self.log.error("processor_error", throttle_key="processor_error",
                               error=f"{type(e).__name__}: {e}")
                time.sleep(0.5)

    # -- code --
    def reload_if_changed(self) -> bool:
        with self.lock:
            if not self.code.changed():
                return False
            ok = self.code.load()
            if ok:
                self.log.info("code_reloaded", version=self.code.version)
                self.on_event("code_reloaded", {"version": self.code.version})
            else:
                self.log.error("code_reload_failed", error=self.code.load_error)
                self.on_event("code_reload_failed", {"error": self.code.load_error})
            return ok

    # -- the sink, which may be a network database --
    def _write(self, df) -> tuple[int, int]:
        """Retry until the sink takes the rows. Moving on would commit the offset past rows
        that never landed; waiting keeps them in the stream until the warehouse is back."""
        delay = 0.5
        while True:
            try:
                out = self.sink.write(df)
                if self.sink_down_since is not None:
                    down = round(time.time() - self.sink_down_since, 1)
                    self.sink_down_since = None
                    self.log.info("sink_recovered", down_s=down)
                    self.on_event("sink_recovered", {"down_s": down})
                return out
            except SinkUnavailable as e:
                if self.sink_down_since is None:
                    self.sink_down_since = time.time()
                    self.on_event("sink_unavailable", {"error": str(e)})
                self.log.error("sink_unavailable", throttle_key="sink_unavailable", every_s=10, error=str(e))
                if self._stop.wait(delay):
                    raise
                delay = min(delay * 2, 10.0)

    # -- one micro-batch --
    def _handle(self, msgs) -> None:
        self.reload_if_changed()
        now = time.time()
        records = [m.value for m in msgs]
        res = process_records(records, self.code)
        inserted, ignored = self._write(res.good)

        dlq = [{"record": rec, "error": reason, "first_signature": reason["signature"],
                "failed_at": now, "replays": 0, "code_version": self.code.version}
               for rec, reason in res.failed]
        reasons = Counter(reason["signature"] for _, reason in res.failed)
        if dlq:
            self.bus.publish_many(self.dlq_topic, dlq)
        self.bus.flush()

        for r in records:
            self.recent_raw.append((now, r))
        for rec, reason in res.failed:
            self.recent_dlq.append((now, rec, reason))
        if res.transform_error:
            self.last_transform_error[res.transform_error["signature"]] = res.transform_error

        self.metrics.add(ts=now, reasons=reasons, **{"in": len(records)}, loaded=inserted,
                         dlq=len(dlq), filtered=res.filtered, sink_dupes=ignored)
        self.batches += 1
        self._log_batch(res, len(dlq))
        if not dlq:
            self._track_schema(records)
            if not res.filtered and not self.code.load_error:
                self.live_sample.add(records, res.good)
                if self.batches % 20 == 0:
                    self.live_sample.save()

    def _log_batch(self, res: BatchResult, n_dlq: int) -> None:
        if res.transform_error:
            e = res.transform_error
            self.log.error("transform_failed", throttle_key=e["signature"], every_s=5,
                           type=e["type"], message=e["message"], quarantined=n_dlq,
                           traceback=e["traceback"])
        elif res.contract_failures:
            for f in res.contract_failures:
                self.log.error("validation_failed", throttle_key=f"{f['column']}:{f['check']}",
                               every_s=5, column=f["column"], check=f["check"], count=f["count"],
                               examples=f["examples"][:3])

    def _track_schema(self, records: list[dict]) -> None:
        """A clean micro-batch with a new shape (say, an added field) becomes the new known-good shape."""
        fp = fingerprint.fingerprint(records)
        if self.known_fp is None:
            self.known_fp = fp
            fingerprint.save_snapshot(fp, self.snapshot_path)
            return
        d = fingerprint.diff(self.known_fp, fp)
        if d.get("added") or d.get("type_changed"):
            merged = {k: sorted(set(self.known_fp.get(k, [])) | set(fp.get(k, [])))
                      for k in set(self.known_fp) | set(fp)}
            self.known_fp = merged
            fingerprint.save_snapshot(merged, self.snapshot_path)
            self.log.warning("schema_changed", throttle_key="schema_changed", diff=d, absorbed=True)
            self.on_event("schema_absorbed", {"diff": d})

    # -- replay --
    def dlq_depth(self) -> int:
        try:
            return max(0, self.bus.high_watermark(self.dlq_topic) - self._replayer.position())
        except Exception:
            return 0

    def replay(self, signatures: set[str] | None = None, timeout_s: float = 60.0) -> dict:
        """Re-run everything waiting in the DLQ through the current code.

        Rows that now pass are loaded; rows the code now drops on purpose are counted
        as filtered; rows that still fail go back to the end of the DLQ. When
        `signatures` is given, `still_failing_related` counts only rows that were
        first quarantined for one of those reasons (the incident being rolled out)."""
        t0 = time.time()
        stats = {"replayed": 0, "loaded": 0, "already_loaded": 0, "filtered": 0,
                 "still_failing": 0, "still_failing_related": 0, "code_version": None, "seconds": 0.0}
        with self.lock:
            self.reload_if_changed()
            stats["code_version"] = self.code.version
            target = self.bus.high_watermark(self.dlq_topic)
            while self._replayer.position() < target and time.time() - t0 < timeout_s:
                want = min(self.max_batch, target - self._replayer.position())
                msgs = self._replayer.poll(max_messages=want, timeout=1.0, linger=0)
                if not msgs:
                    continue
                entries = [m.value for m in msgs]
                records = [e.get("record") or {} for e in entries]
                by_record = {id(r): e for r, e in zip(records, entries)}
                res = process_records(records, self.code)
                inserted, ignored = self._write(res.good)
                requeue = []
                for rec, reason in res.failed:
                    entry = by_record.get(id(rec), {})
                    first = entry.get("first_signature") or (entry.get("error") or {}).get("signature")
                    requeue.append({"record": rec, "error": reason, "first_signature": first,
                                    "failed_at": time.time(), "replays": entry.get("replays", 0) + 1,
                                    "code_version": self.code.version})
                    if signatures is None or first in signatures:
                        stats["still_failing_related"] += 1
                if requeue:
                    self.bus.publish_many(self.dlq_topic, requeue)
                self.bus.flush()
                self._replayer.commit()
                stats["replayed"] += len(records)
                stats["loaded"] += inserted
                stats["already_loaded"] += ignored
                stats["filtered"] += res.filtered
                stats["still_failing"] += len(res.failed)
        stats["seconds"] = round(time.time() - t0, 2)
        self.metrics.add(replayed=stats["replayed"], replay_loaded=stats["loaded"])
        self.log.info("dlq_replayed", **stats)
        self.on_event("replay_done", stats)
        return stats

    # -- what the detector needs --
    def recent_failures(self, since: float) -> list[tuple[float, dict, dict]]:
        return [x for x in list(self.recent_dlq) if x[0] >= since]

    def recent_records(self, n: int = 200) -> list[dict]:
        return [r for _, r in list(self.recent_raw)[-n:]]

    def peek_dlq(self, n: int = 50) -> list[dict]:
        return [{"failed_at": ts, "record": rec, "error": reason}
                for ts, rec, reason in list(self.recent_dlq)[-n:]][::-1]

    def snapshot_incident_batch(self, path: Path, n: int = 200) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.recent_records(n), default=str), encoding="utf-8")
        return path
