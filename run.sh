#!/usr/bin/env bash
# Start the demo; reuse existing results unless --refresh is supplied.
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -x .venv/bin/python ]]; then
  echo 'First run: bash scripts/setup.sh'
  exit 1
fi
.venv/bin/python scripts/cluster.py start
if [[ ! -f artifacts/snapshots.jsonl || "${1:-}" == '--refresh' ]]; then
  .venv/bin/python scripts/download_data.py --country IN
  .venv/bin/python scripts/run_pipeline.py --country IN
  .venv/bin/python scripts/verify_results.py
  .venv/bin/python scripts/collect_evidence.py
fi
.venv/bin/python scripts/cluster.py dashboard
