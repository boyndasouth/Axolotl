"""Thin LiteLLM wrapper: one place for model choice, cost tracking and test doubles."""

from __future__ import annotations

import json
import os
import re
from typing import Callable

from agent.config import LLM_TEMPERATURE

# A fake LLM for tests and offline demos: fn(role, system, user) -> str
_fake: Callable[[str, str, str], str] | None = None


def set_fake_llm(fn: Callable[[str, str, str], str] | None) -> None:
    global _fake
    _fake = fn


def model_label(model: str) -> str:
    """What to show people: the configured model, or that a scripted stand-in is answering."""
    return "scripted stand-in (no LLM)" if _fake is not None else model


def _enable_tracing() -> None:
    if os.environ.get("LANGFUSE_PUBLIC_KEY"):
        import litellm
        litellm.success_callback = ["langfuse"]
        litellm.failure_callback = ["langfuse"]


def complete(role: str, model: str, system: str, user: str) -> tuple[str, float]:
    """Returns (text, cost_usd)."""
    if _fake is not None:
        return _fake(role, system, user), 0.0
    import litellm

    litellm.suppress_debug_info = True
    _enable_tracing()
    resp = litellm.completion(
        model=model,
        temperature=LLM_TEMPERATURE,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        metadata={"agent_role": role},
    )
    try:
        cost = float(litellm.completion_cost(completion_response=resp) or 0.0)
    except Exception:
        cost = 0.0
    return resp.choices[0].message.content or "", cost


def extract_json(text: str) -> dict:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if fenced:
        return json.loads(fenced.group(1))
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object in model output")
    return json.loads(text[start:end + 1])


def complete_json(role: str, model: str, system: str, user: str) -> tuple[dict, float, int]:
    """Returns (parsed JSON, cost, calls). Retries once if the output isn't valid JSON."""
    text, cost = complete(role, model, system, user)
    try:
        return extract_json(text), cost, 1
    except (ValueError, json.JSONDecodeError):
        text2, cost2 = complete(role, model, system,
                                user + "\n\nYour previous reply was not valid JSON. Reply with ONE JSON object only.")
        return extract_json(text2), cost + cost2, 2
