"""High-level JSONL scenario runner for the supplied drill records.

Example:
    python3 scenario_runner.py examples/repair-races-new-version.jsonl \
        --root var/replay/race --reset
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from replica_store.replay import replay_file


def process_scenario(path: str | Path, root: str | Path, reset: bool = True) -> dict:
    """Replay one scenario and return its on-call report."""
    return replay_file(path, root, reset=reset)


def main() -> int:
    parser = argparse.ArgumentParser(description="重放副本存储 JSONL 演练")
    parser.add_argument("scenario", type=Path, nargs="?")
    parser.add_argument("--examples-dir", type=Path, default=Path("examples"))
    parser.add_argument("--root", type=Path, default=Path("var/scenario"))
    parser.add_argument("--reset", action="store_true", default=True)
    parser.add_argument("--no-reset", dest="reset", action="store_false")
    args = parser.parse_args()

    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = sorted(args.examples_dir.glob("*.jsonl"))
    for scenario in scenarios:
        root = args.root / scenario.stem if len(scenarios) > 1 else args.root
        summary = process_scenario(scenario, root, args.reset)
        report = summary["report"]
        print(f"\n=== {scenario.name} ===")
        print(report["markdown"])
        print(json.dumps({
            "root": summary["root"],
            "report_json": summary["report_json"],
            "report_markdown": summary["report_markdown"],
        }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
