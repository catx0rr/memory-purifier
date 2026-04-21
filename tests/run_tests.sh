#!/usr/bin/env bash
# memory-purifier v1.5.0 release-validation regression suite.
#
# Runs every `test_*.py` under `tests/` via `unittest discover`. Each test
# provisions a disposable workspace under /tmp, installs the package
# skeleton, and drives real `run_purifier.py` subprocesses against a
# file-backed LLM fixture (no API calls; no network).
#
# Two supported invocation paths from repo root:
#
#   1. Full default suite:
#        bash tests/run_tests.sh
#
#   2. Targeted module:
#        python3 -m unittest -v tests.test_runtime_v150_proof
#        python3 -m unittest -v tests.test_publish_contract
#        ...
#
# Both paths exercise the same shipped test tree. No external overlay
# required.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

PYTHONPATH="$HERE" python3 -m unittest discover -v -s "$HERE" -p 'test_*.py'
