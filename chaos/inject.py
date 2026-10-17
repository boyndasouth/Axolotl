"""Inject a failure on purpose.

    python -m chaos.inject rename_column                 # writes data/incoming/orders.json
    python -m chaos.inject rename_column --target api    # flips the fake upstream API
    python -m chaos.inject --clear --target api          # API back to normal
    python -m chaos.inject rename_column --target stream # the stream producer starts drifting
    python -m chaos.inject --clear --target stream

The incident batch uses a seed different from every fixture, so the agent never
sees the exact rows it will be verified against.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx

from chaos.catalog import list_scenarios, load_scenario
from chaos.drifts import apply_drift
from chaos.generator import generate_orders

DEFAULT_OUT = Path(__file__).resolve().parents[1] / "data" / "incoming" / "orders.json"


def make_incident_batch(scenario_id: str, out: Path = DEFAULT_OUT, seed: int = 3000,
                        n: int = 200) -> Path:
    sc = load_scenario(scenario_id)
    records = apply_drift(generate_orders(n=n, seed=seed), sc["drift"], sc.get("params"), seed=seed)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(records), encoding="utf-8")
    return out


def make_clean_batch(out: Path = DEFAULT_OUT, seed: int = 3000, n: int = 200) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(generate_orders(n=n, seed=seed)), encoding="utf-8")
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("scenario", nargs="?", choices=list_scenarios())
    p.add_argument("--target", choices=["file", "api", "stream"], default="file")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--seed", type=int, default=3000)
    p.add_argument("--clear", action="store_true")
    a = p.parse_args()

    if a.target == "stream":
        from chaos.control import FileDrift
        if a.clear or not a.scenario:
            FileDrift().clear()
            print("stream producer back to the original format")
        else:
            FileDrift().set(a.scenario)
            print(f"stream producer now drifting: {a.scenario}")
        return

    if a.target == "api":
        if a.clear:
            httpx.delete(f"{a.api}/admin/drift").raise_for_status()
            print("upstream API back to the original format")
        else:
            httpx.post(f"{a.api}/admin/drift", json={"scenario": a.scenario}).raise_for_status()
            print(f"upstream API now drifting: {a.scenario}")
        return

    if a.clear or not a.scenario:
        print(f"wrote clean batch to {make_clean_batch(seed=a.seed)}")
    else:
        print(f"wrote {a.scenario} batch to {make_incident_batch(a.scenario, seed=a.seed)}")


if __name__ == "__main__":
    main()
