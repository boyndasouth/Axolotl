"""The pipeline spec: everything the healer needs to know about a pipeline it doesn't own.

The agents, the sandbox and the stream runtime read this instead of assuming the
demo's orders pipeline. It lives in `healer.yaml` at the root of the repo being
healed; with no file there, the built-in defaults describe the demo.

    from healer.spec import load_spec
    spec = load_spec(workdir)          # SHP_PIPELINE picks one when the file lists several
    spec.editable_files()              # what the repair agent may change
    spec.is_editable("pipeline/schemas.py")   # False: the contract is protected

The safety rules are checked when the spec loads, not when a patch arrives: a spec
whose `editable` patterns reach the contract, the tests or a read-only file is
rejected, so no configuration can let the agent loosen the rules it is graded on.

    python -m healer.spec check        # validate healer.yaml and show what the agent can touch
"""

from __future__ import annotations

import copy
import fnmatch
import importlib.util
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import yaml

SPEC_FILE = "healer.yaml"

# The demo: the orders pipeline that ships in this repo. Used when there is no healer.yaml.
DEFAULT_PIPELINE: dict = {
    "name": "orders",
    "transform": "pipeline/transforms/orders.py:transform_orders",
    "mappings": "pipeline/mappings.yaml",
    "contract": "pipeline/schemas.py:OrdersSchema",
    "key": "order_id",
    "editable": ["pipeline/transforms/*.py", "pipeline/mappings.yaml"],
    "read_only": ["tests/pipeline/test_orders.py"],
    "tests": "tests/pipeline",
    "code": ["pipeline", "tests/pipeline"],
    "source": {"type": "kafka", "topic": "orders", "demo_producer": True},
    "quarantine": {"topic": "orders.dlq"},
    "sink": {"type": "duckdb", "path": "stream_warehouse.duckdb", "table": "orders_clean"},
    "holdout": {"fixtures": "chaos/fixtures", "live_sample": {"rows": 500}},
    "compare": {"float_decimals": 2},
}

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class SpecError(ValueError):
    """healer.yaml is missing something, or would let the agent edit what it must not."""


def _expand_env(value):
    """${VAR} and ${VAR:-default} in strings, so secrets stay in the environment."""
    if isinstance(value, str):
        def sub(m):
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise SpecError(f"healer.yaml references ${{{name}}} but it is not set")
        return _ENV.sub(sub, value)
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _rel(path: str, what: str) -> str:
    """A repo-relative POSIX path that can't escape the repo."""
    if not isinstance(path, str) or not path.strip():
        raise SpecError(f"{what} must be a non-empty path")
    p = path.replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    if p.startswith("/") or re.match(r"^[A-Za-z]:", p) or ".." in PurePosixPath(p).parts:
        raise SpecError(f"{what} must be a path inside the repo, got {path!r}")
    return p.rstrip("/")


@dataclass(frozen=True)
class Entrypoint:
    """`path/to/file.py:attribute`, loaded by file path so hot reload sees new code."""
    path: str
    attr: str

    @classmethod
    def parse(cls, value: str, what: str) -> "Entrypoint":
        if not isinstance(value, str) or ":" not in value:
            raise SpecError(f"{what} must look like 'path/to/file.py:name', got {value!r}")
        path, attr = value.rsplit(":", 1)
        path = _rel(path, what)
        if not path.endswith(".py") or not attr.isidentifier():
            raise SpecError(f"{what} must look like 'path/to/file.py:name', got {value!r}")
        return cls(path, attr)

    def __str__(self) -> str:
        return f"{self.path}:{self.attr}"

    def load(self, root: Path, module_name: str | None = None):
        file = Path(root) / self.path
        if not file.exists():
            raise SpecError(f"{self.path} does not exist under {root}")
        name = module_name or "shp_spec_" + re.sub(r"\W", "_", self.path[:-3])
        added = str(root) not in sys.path
        if added:      # lets the file import its own package (e.g. `from pipeline.x import y`)
            sys.path.insert(0, str(root))
        try:
            spec = importlib.util.spec_from_file_location(name, file)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        finally:
            if added:
                sys.path.remove(str(root))
        if not hasattr(mod, self.attr):
            raise SpecError(f"{self.path} has no attribute {self.attr!r}")
        return getattr(mod, self.attr)


@dataclass(frozen=True)
class PipelineSpec:
    root: Path
    name: str
    transform: Entrypoint
    contract: Entrypoint
    key: str
    editable: tuple[str, ...]
    read_only: tuple[str, ...]
    mappings: str | None
    tests: str | None
    code: tuple[str, ...]
    source: dict = field(default_factory=dict)
    quarantine: dict = field(default_factory=dict)
    sink: dict = field(default_factory=dict)
    holdout: dict = field(default_factory=dict)
    compare: dict = field(default_factory=dict)
    from_file: bool = False

    # ---- what the agent may touch ----------------------------------------------------
    @property
    def protected(self) -> tuple[str, ...]:
        """Files and directories the agent can read but never change."""
        out = [self.contract.path, *self.read_only]
        if self.tests:
            out.append(self.tests)
        return tuple(dict.fromkeys(out))

    def _is_protected(self, path: str) -> bool:
        return any(path == p or path.startswith(p + "/") for p in self.protected)

    def is_editable(self, path: str) -> bool:
        try:
            norm = _rel(path, "path")
        except SpecError:
            return False
        if self._is_protected(norm):
            return False
        return any(fnmatch.fnmatch(norm, pat) for pat in self.editable)

    def editable_files(self) -> list[str]:
        """Existing files the repair agent may edit: mappings first, then code."""
        found = set()
        for pat in self.editable:
            for p in self.root.glob(pat):
                # empty files (a bare __init__.py) are only noise in the agent's context
                if p.is_file() and "__pycache__" not in p.parts and p.read_text(encoding="utf-8",
                                                                                 errors="replace").strip():
                    rel = p.relative_to(self.root).as_posix()
                    if self.is_editable(rel):
                        found.add(rel)
        return sorted(found, key=lambda r: (not r.endswith((".yaml", ".yml", ".json")), r))

    def read_only_files(self) -> list[str]:
        """Files shown to the agents as context they must satisfy: the contract, then the rest."""
        out = [self.contract.path]
        for p in self.read_only:
            full = self.root / p
            if full.is_dir():
                out += sorted(q.relative_to(self.root).as_posix() for q in full.rglob("*.py"))
            else:
                out.append(p)
        return [p for p in dict.fromkeys(out) if (self.root / p).exists()]

    # ---- streaming ---------------------------------------------------------------------
    @property
    def source_topic(self) -> str:
        return self.source.get("topic") or self.name

    @property
    def dlq_topic(self) -> str:
        return self.quarantine.get("topic") or f"{self.source_topic}.dlq"

    @property
    def demo_producer(self) -> bool:
        return bool(self.source.get("demo_producer", False))

    # ---- the contract ------------------------------------------------------------------
    def load_contract(self, root: Path | None = None):
        return self.contract.load(root or self.root)

    def columns(self, root: Path | None = None) -> list[str]:
        return list(self.load_contract(root).columns)

    def column_types(self, root: Path | None = None) -> dict[str, str]:
        """Contract column -> a portable type name: string, float, int, bool, datetime."""
        out = {}
        for name, col in self.load_contract(root).columns.items():
            t = str(col.dtype).lower()
            if "float" in t or "double" in t or "decimal" in t:
                out[name] = "float"
            elif "int" in t:
                out[name] = "int"
            elif "bool" in t:
                out[name] = "bool"
            elif "datetime" in t or "timestamp" in t:
                out[name] = "datetime"
            else:
                out[name] = "string"
        return out

    # ---- holdout -----------------------------------------------------------------------
    def fixture_dir(self) -> Path | None:
        rel = self.holdout.get("fixtures")
        return (self.root / rel) if rel else None

    @property
    def live_sample_rows(self) -> int:
        live = self.holdout.get("live_sample")
        if not live:
            return 0
        return int(live.get("rows", 500)) if isinstance(live, dict) else 500

    # ---- the sandbox -------------------------------------------------------------------
    def sandbox_manifest(self) -> dict:
        """What evaluate.py needs inside the sandbox, which has none of the host's code."""
        return {"name": self.name, "transform": str(self.transform), "contract": str(self.contract),
                "mappings": self.mappings, "tests": self.tests, "key": self.key,
                "float_decimals": int(self.compare.get("float_decimals", 6))}

    def check(self) -> list[str]:
        """Load the user's code and confirm it fits together. Returns a list of problems."""
        problems = []
        for ep, what in ((self.transform, "transform"), (self.contract, "contract")):
            if not (self.root / ep.path).exists():
                problems.append(f"{what}: {ep.path} does not exist")
        if self.mappings and not (self.root / self.mappings).exists():
            problems.append(f"mappings: {self.mappings} does not exist")
        if self.tests and not (self.root / self.tests).exists():
            problems.append(f"tests: {self.tests} does not exist")
        if not problems:
            try:
                cols = self.columns()
                if self.key not in cols:
                    problems.append(f"key {self.key!r} is not a column of the contract {cols}")
            except Exception as e:
                problems.append(f"contract failed to load: {type(e).__name__}: {e}")
            try:
                if not callable(self.transform.load(self.root)):
                    problems.append(f"transform {self.transform} is not callable")
            except Exception as e:
                problems.append(f"transform failed to load: {type(e).__name__}: {e}")
        if not self.editable_files():
            problems.append("no existing file matches `editable`: the agent would have nothing to fix")
        for p in (self.transform.path, self.mappings):
            if p and not any(p == c or p.startswith(c + "/") for c in self.code):
                problems.append(f"{p} is outside `code`, so the sandbox would not have it")
        return problems


# ---- loading ---------------------------------------------------------------------------

PIPELINE_KEYS = {"name", "transform", "mappings", "contract", "key", "editable", "read_only", "tests",
                 "code", "source", "quarantine", "sink", "holdout", "compare"}
TOP_KEYS = {"version", "pipelines"}


def _build(raw: dict, root: Path, from_file: bool) -> PipelineSpec:
    if not isinstance(raw, dict):
        raise SpecError("each pipeline must be a mapping")
    unknown = sorted(set(raw) - PIPELINE_KEYS)
    if unknown:     # a typo or bad indentation would otherwise silently drop a setting
        raise SpecError(f"pipeline {raw.get('name', '?')!r} has unknown keys {unknown}; "
                        f"allowed: {sorted(PIPELINE_KEYS)}")
    for req in ("name", "transform", "contract", "key", "editable"):
        if not raw.get(req):
            raise SpecError(f"pipeline is missing `{req}`")
    name = str(raw["name"])
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise SpecError(f"pipeline name {name!r} may only use letters, digits, '_', '-' and '.'")

    editable = tuple(_rel(p, "editable") for p in raw["editable"])
    read_only = tuple(_rel(p, "read_only") for p in raw.get("read_only") or [])
    tests = _rel(raw["tests"], "tests") if raw.get("tests") else None
    mappings = _rel(raw["mappings"], "mappings") if raw.get("mappings") else None
    transform = Entrypoint.parse(raw["transform"], "transform")
    contract = Entrypoint.parse(raw["contract"], "contract")
    code = raw.get("code")
    if code:
        code = tuple(_rel(p, "code") for p in code)
    else:   # default: the top-level directories of everything the sandbox has to run
        tops = [PurePosixPath(p).parts[0] for p in (transform.path, contract.path, mappings, tests) if p]
        code = tuple(dict.fromkeys(tops))

    spec = PipelineSpec(
        root=root, name=name, transform=transform, contract=contract, key=str(raw["key"]),
        editable=editable, read_only=read_only, mappings=mappings, tests=tests, code=code,
        source=dict(raw.get("source") or {}), quarantine=dict(raw.get("quarantine") or {}),
        sink=dict(raw.get("sink") or {"type": "duckdb"}), holdout=dict(raw.get("holdout") or {}),
        compare=dict(raw.get("compare") or {}), from_file=from_file)

    # the safety rule: no editable pattern may reach a protected file. is_editable() would
    # block such a path anyway; rejecting the spec makes the mistake visible up front.
    for pat in editable:
        for prot in spec.protected:
            probes = [prot, prot + "/x.py", prot + "/x/y.py"]
            full = root / prot
            if full.is_dir():
                probes += [q.relative_to(root).as_posix() for q in full.rglob("*") if q.is_file()]
            hit = next((p for p in probes if fnmatch.fnmatch(p, pat)), None)
            if hit:
                raise SpecError(f"editable pattern {pat!r} reaches protected {prot!r} "
                                f"(the contract, tests and read_only files are off-limits to the agent)")
    if transform.path in spec.protected:
        raise SpecError("the transform cannot be protected: it is the code the agent repairs")
    if not spec.is_editable(transform.path) and not (mappings and spec.is_editable(mappings)):
        raise SpecError("`editable` must cover the transform or the mappings file")
    sink_type = spec.sink.get("type", "duckdb")
    if sink_type not in ("duckdb", "postgres"):
        raise SpecError(f"sink type {sink_type!r} is not supported (duckdb or postgres)")
    if sink_type == "postgres" and not spec.sink.get("dsn"):
        raise SpecError("a postgres sink needs `dsn` (use ${WAREHOUSE_DSN} to keep it out of the file)")
    return spec


def load_specs(root: Path | str) -> list[PipelineSpec]:
    root = Path(root).resolve()
    path = root / SPEC_FILE
    if not path.exists():
        return [_build(copy.deepcopy(DEFAULT_PIPELINE), root, from_file=False)]
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise SpecError(f"{SPEC_FILE} is not valid YAML: {e}") from e
    if not isinstance(doc, dict):
        raise SpecError(f"{SPEC_FILE} must be a mapping with `version` and `pipelines`")
    unknown = sorted(set(doc) - TOP_KEYS)
    if unknown:
        raise SpecError(f"{SPEC_FILE} has unknown top-level keys {unknown} (check the indentation)")
    if doc.get("version", 1) != 1:
        raise SpecError(f"{SPEC_FILE} version {doc.get('version')!r} is not supported (expected 1)")
    pipelines = doc.get("pipelines")
    if not isinstance(pipelines, list) or not pipelines:
        raise SpecError(f"{SPEC_FILE} needs a non-empty `pipelines:` list")
    specs = [_build(_expand_env(p), root, from_file=True) for p in pipelines]
    names = [s.name for s in specs]
    if len(set(names)) != len(names):
        raise SpecError(f"pipeline names must be unique, got {names}")
    return specs


_cache: dict = {}


def load_spec(root: Path | str, name: str | None = None) -> PipelineSpec:
    """The pipeline this process heals: `name`, else $SHP_PIPELINE, else the first one."""
    root = Path(root).resolve()
    name = name or os.environ.get("SHP_PIPELINE") or None
    path = root / SPEC_FILE
    stamp = path.stat().st_mtime_ns if path.exists() else None
    key = (str(root), name, stamp, os.environ.get("WAREHOUSE_DSN"))
    if key not in _cache:
        specs = load_specs(root)
        if name:
            match = [s for s in specs if s.name == name]
            if not match:
                raise SpecError(f"no pipeline named {name!r} in {SPEC_FILE} (have {[s.name for s in specs]})")
            _cache[key] = match[0]
        else:
            _cache[key] = specs[0]
    return _cache[key]


def main(argv: list[str] | None = None) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="python -m healer.spec")
    p.add_argument("command", choices=["check"])
    p.add_argument("--root", default=".")
    a = p.parse_args(argv)
    try:
        specs = load_specs(a.root)
    except SpecError as e:
        print(f"healer.yaml is invalid: {e}")
        return 1
    status = 0
    for s in specs:
        src = SPEC_FILE if s.from_file else "built-in demo defaults (no healer.yaml)"
        print(f"pipeline {s.name!r} from {src}")
        print(f"  stream     {s.source_topic} -> quarantine {s.dlq_topic}"
              + ("  (demo producer on)" if s.demo_producer else ""))
        print(f"  sink       {s.sink.get('type', 'duckdb')} table {s.sink.get('table', s.name + '_clean')}, key {s.key}")
        print(f"  agent may edit:    {', '.join(s.editable_files()) or '(nothing)'}")
        print(f"  agent may not:     {', '.join(s.protected)}")
        live = f"live sample of {s.live_sample_rows} rows" if s.live_sample_rows else "no live sample"
        print(f"  holdout    {s.holdout.get('fixtures') or 'no fixtures'}; {live}")
        problems = s.check()
        for prob in problems:
            print(f"  PROBLEM    {prob}")
        if problems:
            status = 1
        else:
            print("  ok")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
