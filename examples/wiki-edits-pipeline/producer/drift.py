"""Flip the upstream's drift switch while the producer runs.

    python producer/drift.py list             what each scenario does
    python producer/drift.py rename_user      start sending the changed shape (next event on)
    python producer/drift.py off              back to the real shape
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from drifts import SCENARIOS, current, set_drift  # noqa: E402


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    cmd = argv[0]
    if cmd == "list":
        now = current()
        for name, (what, expect) in SCENARIOS.items():
            mark = "*" if name == now else " "
            print(f"{mark} {name:<17} {what}\n  {'':<17} healer should: {expect}")
        return 0
    if cmd in ("off", "clear", "none"):
        set_drift(None)
        print("drift off: upstream sends the real shape")
        return 0
    try:
        set_drift(cmd)
    except ValueError as e:
        print(e)
        return 1
    print(f"drift on: {cmd} ({SCENARIOS[cmd][0]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
