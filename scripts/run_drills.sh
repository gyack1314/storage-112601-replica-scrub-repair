#!/usr/bin/env bash
# Repeatable local verification and on-call record generation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m unittest discover -v
python3 scenario_reader.py examples
python3 scenario_runner.py

rm -rf var/demo
python3 drill.py --root var/demo-cli demo --report-dir var/demo

echo
echo "Drill record: $ROOT/var/demo/oncall-report.md"
