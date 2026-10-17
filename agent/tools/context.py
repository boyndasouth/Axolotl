"""Gathers the evidence an agent needs: logs, git history, sample rows, code."""

from __future__ import annotations

import json
import random
from collections import Counter
from pathlib import Path

from healer.spec import load_spec


def allowed_files(workdir: Path) -> list[str]:
    """Files the repair agent may edit, from healer.yaml (mappings first)."""
    return load_spec(workdir).editable_files()


def read_only_files(workdir: Path) -> list[str]:
    """The contract first, then the other files the agents must satisfy but never edit."""
    return load_spec(workdir).read_only_files()


def code_paths(workdir: Path) -> tuple[str, ...]:
    """Where the pipeline's code lives, for git history (the sandbox copies the same paths)."""
    spec = load_spec(workdir)
    tests = spec.tests or ""
    return tuple(p for p in spec.code if p != tests and not p.startswith(tests + "/")) or spec.code


def tail_logs(workdir: Path, n: int = 20) -> list[dict]:
    path = workdir / "logs" / "pipeline.jsonl"
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").strip().splitlines()[-n:]
    events = []
    for line in lines:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev.pop("traceback", None)   # already in the run report; keeps the prompt short
        events.append(ev)
    return events


def git_history(workdir: Path, paths: tuple[str, ...] | None = None, n: int = 8) -> list[dict]:
    try:
        from git import InvalidGitRepositoryError, NoSuchPathError, Repo
    except ImportError:
        return []
    try:
        paths = paths or code_paths(workdir)
        repo = Repo(workdir, search_parent_directories=False)
        commits = list(repo.iter_commits(paths=list(paths), max_count=n))
    except (InvalidGitRepositoryError, NoSuchPathError, ValueError):
        return []
    except Exception:
        return []
    out = []
    for c in commits:
        try:
            files = list(c.stats.files)[:10]
        except Exception:
            files = []
        out.append({"sha": c.hexsha[:10], "date": c.committed_datetime.isoformat(),
                    "author": c.author.name, "message": c.message.strip()[:200], "files": files})
    return out


def last_commit_diff(workdir: Path, paths: tuple[str, ...] | None = None, max_chars: int = 3000) -> str:
    try:
        from git import Repo
        paths = list(paths or code_paths(workdir))
        repo = Repo(workdir)
        commits = list(repo.iter_commits(paths=paths, max_count=1))
        if not commits or not commits[0].parents:
            return ""
        return repo.git.diff(commits[0].parents[0].hexsha, commits[0].hexsha, "--", *paths)[:max_chars]
    except Exception:
        return ""


def sample_records(batch_path: str | Path, n: int = 8, seed: int = 0, key: str | None = None) -> list[dict]:
    """A few rows from the batch that broke, plus rows whose shape looks unusual."""
    records = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    if len(records) <= n:
        return records
    rng = random.Random(seed)
    picked = rng.sample(records, n // 2)

    # add rows whose value types differ from the most common shape (nulls, negatives, odd types)
    def shape(r):
        return tuple(sorted((k, type(v).__name__) for k, v in r.items()))
    counts: dict = {}
    for r in records:
        counts[shape(r)] = counts.get(shape(r), 0) + 1
    common = max(counts, key=counts.get)
    odd = [r for r in records if shape(r) != common]
    odd += [r for r in records if any(isinstance(v, (int, float)) and not isinstance(v, bool) and v < 0
                                      for v in r.values())]
    if key:
        ids = [str(r.get(key)) for r in records]
        seen = Counter(ids)
        odd += [r for r in records if seen[str(r.get(key))] > 1][:2]
    for r in odd:
        if len(picked) >= n:
            break
        if r not in picked:
            picked.append(r)
    for r in records:
        if len(picked) >= n:
            break
        if r not in picked:
            picked.append(r)
    return picked


def read_files(workdir: Path, rels: list[str]) -> dict[str, str]:
    out = {}
    for rel in rels:
        p = workdir / rel
        if p.exists():
            out[rel] = p.read_text(encoding="utf-8")
    return out


def format_files(files: dict[str, str]) -> str:
    return "\n\n".join(f"--- {path} ---\n{content}" for path, content in files.items())


def render_prompt(template: str, workdir: Path) -> str:
    """Fill a prompt's {{PLACEHOLDERS}} from the pipeline spec."""
    spec = load_spec(workdir)
    files = spec.editable_files()
    code_first = [f for f in files if f.endswith(".py")] or files
    values = {
        "PIPELINE": spec.name,
        "CONTRACT": spec.contract.path,
        "EDITABLE": "\n".join(f"- {f}" for f in files) or "- (none)",
        "EXAMPLE_FILE": (code_first[0] if code_first else spec.transform.path),
    }
    for k, v in values.items():
        template = template.replace("{{" + k + "}}", v)
    return template
