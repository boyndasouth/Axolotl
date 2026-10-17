"""Paths and settings. Everything resolves relative to this checkout, so a copied
working directory (used by the benchmark) is fully self-contained."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("SHP_DATA_DIR", ROOT / "data"))
LOG_DIR = Path(os.environ.get("SHP_LOG_DIR", ROOT / "logs"))
MAPPINGS_PATH = ROOT / "pipeline" / "mappings.yaml"
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://localhost:8000")

# Monitor thresholds
MIN_ROWS = int(os.environ.get("SHP_MIN_ROWS", "10"))


def load_mappings(path: Path = MAPPINGS_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
