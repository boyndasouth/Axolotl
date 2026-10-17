"""Agent settings, all overridable through environment variables (see .env.example)."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = Path(os.environ.get("SHP_REPORTS_DIR", REPO_ROOT / "reports"))
CHECKPOINT_DB = Path(os.environ.get("SHP_CHECKPOINT_DB", REPO_ROOT / "data" / "agent_checkpoints.sqlite"))

# Any LiteLLM model string works: "groq/...", "mistral/...", "anthropic/...", "openai/...", "ollama/..."
DIAGNOSIS_MODEL = os.environ.get("DIAGNOSIS_MODEL", "groq/llama-3.3-70b-versatile")
REPAIR_MODEL = os.environ.get("REPAIR_MODEL", "groq/llama-3.3-70b-versatile")
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.1"))

CONFIDENCE_THRESHOLD = float(os.environ.get("SHP_CONFIDENCE_THRESHOLD", "0.6"))
MAX_ATTEMPTS = int(os.environ.get("SHP_MAX_ATTEMPTS", "3"))

# PRs: "github" opens real PRs (needs GITHUB_TOKEN + GITHUB_REPO), "dry-run" writes them to reports/prs/
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPO = os.environ.get("GITHUB_REPO")          # "owner/name"
PR_MODE = os.environ.get("SHP_PR_MODE") or ("github" if GITHUB_TOKEN and GITHUB_REPO else "dry-run")
