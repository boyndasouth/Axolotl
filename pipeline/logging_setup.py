"""Structured JSON logs (one event per line) so the diagnosis agent can read them."""

from __future__ import annotations

import structlog

from pipeline.config import LOG_DIR

LOG_FILE = LOG_DIR / "pipeline.jsonl"
_configured = False


def get_logger():
    global _configured
    if not _configured:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.JSONRenderer(),
            ],
            logger_factory=structlog.WriteLoggerFactory(file=open(LOG_FILE, "a", encoding="utf-8")),
            cache_logger_on_first_use=True,
        )
        _configured = True
    return structlog.get_logger("pipeline")
