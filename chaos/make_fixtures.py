"""Build the held-out and hidden fixtures for every scenario.

    python -m chaos.make_fixtures

For each scenario this writes chaos/fixtures/<scenario>/:
  holdout.json / holdout_expected.json   used by the sandbox to verify a patch
  hidden.json  / hidden_expected.json    used only by the benchmark, after the run,
                                         to catch patches that pass verification but
                                         don't generalize (the false-fix rate)

The expected output is always the ORIGINAL pipeline run on the clean, pre-drift
records: a correct repair makes drifted input produce exactly what the clean input
would have produced. The hidden set uses a different seed and larger amounts
(thousands separators, for example), so overfitted fixes get caught.

Agents never see this folder. Only the sandbox and the benchmark read it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from chaos.catalog import FIXTURE_DIR, list_scenarios, load_scenario
from chaos.drifts import apply_drift
from chaos.generator import generate_orders
from pipeline.config import load_mappings
from pipeline.schemas import OrdersSchema
from pipeline.transforms.orders import transform_orders

HOLDOUT = {"n": 150, "max_amount": 500.0}
HIDDEN = {"n": 150, "max_amount": 5000.0}


def _expected(clean: list[dict]) -> list[dict]:
    df = transform_orders(clean, load_mappings())
    OrdersSchema.validate(df)
    return df.to_dict(orient="records")


def _write(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=None), encoding="utf-8")


def build(fixture_dir: Path = FIXTURE_DIR) -> list[str]:
    if fixture_dir.exists():
        shutil.rmtree(fixture_dir)
    written = []

    # baseline: clean data the current pipeline must keep handling (regression guard)
    base = fixture_dir / "baseline"
    base.mkdir(parents=True)
    for split, seed, opts in [("holdout", 1000, HOLDOUT), ("hidden", 2000, HIDDEN)]:
        clean = generate_orders(seed=seed, n=opts["n"], max_amount=opts["max_amount"])
        _write(base / f"{split}.json", clean)
        _write(base / f"{split}_expected.json", _expected(clean))
    written.append("baseline")

    for i, sid in enumerate(list_scenarios(), start=1):
        sc = load_scenario(sid)
        if sc["expected_outcome"] == "escalate":
            continue   # nothing a patch can make correct
        d = fixture_dir / sid
        d.mkdir(parents=True)
        for split, seed, opts in [("holdout", 1000 + i, HOLDOUT), ("hidden", 2000 + i, HIDDEN)]:
            clean = generate_orders(seed=seed, n=opts["n"], max_amount=opts["max_amount"])
            _write(d / f"{split}.json", apply_drift(clean, sc["drift"], sc.get("params"), seed=seed))
            _write(d / f"{split}_expected.json", _expected(clean))
        written.append(sid)
    return written


if __name__ == "__main__":
    names = build()
    print(f"wrote fixtures for {len(names)} sets: {', '.join(names)}")
