"""Scenario catalog and fixture locations."""

from __future__ import annotations

from pathlib import Path

import yaml

CHAOS_DIR = Path(__file__).resolve().parent
SCENARIO_DIR = CHAOS_DIR / "scenarios"
FIXTURE_DIR = CHAOS_DIR / "fixtures"


def list_scenarios() -> list[str]:
    return sorted(p.stem for p in SCENARIO_DIR.glob("*.yaml"))


def load_scenario(scenario_id: str) -> dict:
    path = SCENARIO_DIR / f"{scenario_id}.yaml"
    if not path.exists():
        raise KeyError(f"unknown scenario {scenario_id!r}; have {list_scenarios()}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))
