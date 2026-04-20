#!/usr/bin/env bash
# memory-purifier regression suite.
#
# Runs the six unittest files under `tests/` using the python3 `unittest`
# module. Each test provisions a disposable workspace under /tmp, installs
# the package skeleton there, and runs real `run_purifier.py` subprocesses
# against the file-backed LLM fixture (no API calls).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

PYTHONPATH="$HERE" python3 -m unittest discover -v -s "$HERE" -p 'test_*.py'
