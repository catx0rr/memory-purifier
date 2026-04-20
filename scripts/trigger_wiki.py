#!/usr/bin/env python3
"""Downstream wiki handoff signal.

The purifier does NOT compile the wiki — memory-reconciler does, on its own
cron schedule. This script's job is intentionally narrow:

- Read <runtime>/purified-manifest.json and check downstreamWikiIngestSuggested
- If the config has downstream.wiki_trigger_command set, invoke it with the
  manifest path as a positional argument (for operators who want active push)
- Write a signal file at <runtime>/purifier-downstream-signal.json so downstream
  consumers (reconciler cron, manual inspection) can tell if a fresh purified
  run is awaiting ingest

This file MUST NOT inline reconciliation / wiki-compilation logic. Adding
such logic here would collapse two layers that the spec explicitly separates.
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def _load_json_safely(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Downstream wiki handoff signal (no reconciliation logic).")
    ap.add_argument("--workspace", help="Workspace root")
    ap.add_argument("--runtime-dir", help="Runtime dir override")
    ap.add_argument("--config", help=f"Path to memory-purifier.json (default: {DEFAULT_CONFIG})")
    ap.add_argument(
        "--command",
        help="Override downstream wiki trigger command (default: config.downstream.wiki_trigger_command)",
    )
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument("--dry-run", action="store_true", help="Do not write signal file or invoke downstream command")

    args = ap.parse_args()

    config_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG
    config = _load_json_safely(config_path) if config_path.is_file() else {}

    tz_name = args.timezone or config.get("timezone") or "Asia/Manila"
    ts = timestamp_triple(tz_name)

    workspace_hint = args.workspace or os.environ.get("WORKSPACE")
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")

    manifest_path = runtime_dir / "purified-manifest.json"
    if not manifest_path.is_file():
        out = {
            "status": "skipped",
            "reason": f"manifest not found at {manifest_path}",
            "pass": "trigger_wiki",
            "dry_run": args.dry_run,
            **ts,
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    manifest = _load_json_safely(manifest_path)
    suggested = bool(manifest.get("downstreamWikiIngestSuggested"))
    run_id = manifest.get("runId")
    run_status = manifest.get("status")

    if not suggested:
        out = {
            "status": "skipped",
            "reason": "manifest.downstreamWikiIngestSuggested is false",
            "pass": "trigger_wiki",
            "run_id": run_id,
            "run_status": run_status,
            "dry_run": args.dry_run,
            **ts,
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    # Real claim count: count lines in purified-claims.jsonl (each line = one claim).
    # Previously this used sourceInventory length, which was wrong — that's a source-file count.
    claims_path = runtime_dir / "purified-claims.jsonl"
    claim_count = 0
    if claims_path.is_file():
        try:
            with claims_path.open(encoding="utf-8") as f:
                claim_count = sum(1 for line in f if line.strip())
        except Exception:
            claim_count = 0
    source_count = len(manifest.get("sourceInventory") or [])

    signal_path = runtime_dir / "purifier-downstream-signal.json"
    signal_payload = {
        "runId": run_id,
        "runStatus": run_status,
        "manifestPath": str(manifest_path),
        "profileScope": manifest.get("profileScope"),
        "mode": manifest.get("mode"),
        "claimCount": claim_count,
        "sourceCount": source_count,
        "finishedAt": manifest.get("finishedAt"),
        "signaledAt": ts["timestamp"],
        "signaledAt_utc": ts["timestamp_utc"],
        "timezone": ts["timezone"],
    }

    command = args.command
    if command is None:
        command = ((config.get("downstream") or {}).get("wiki_trigger_command")) or None

    command_result = None
    if command:
        try:
            argv = shlex.split(command) + [str(manifest_path)]
        except ValueError as e:
            command_result = {
                "invoked": False,
                "error": f"command parse failed: {e}",
            }
        else:
            if args.dry_run:
                command_result = {
                    "invoked": False,
                    "would_invoke": argv,
                    "note": "dry-run: command not executed",
                }
            else:
                try:
                    proc = subprocess.run(argv, capture_output=True, text=True, timeout=60)
                    command_result = {
                        "invoked": True,
                        "argv": argv,
                        "returncode": proc.returncode,
                        "stdout_head": (proc.stdout or "")[:500],
                        "stderr_head": (proc.stderr or "")[:500],
                    }
                except subprocess.TimeoutExpired:
                    command_result = {
                        "invoked": True,
                        "argv": argv,
                        "error": "downstream command timed out after 60s",
                    }
                except Exception as e:
                    command_result = {
                        "invoked": True,
                        "argv": argv,
                        "error": f"downstream command failed: {type(e).__name__}: {e}",
                    }

    if not args.dry_run:
        _atomic_write_json(signal_path, signal_payload)

    out = {
        "status": "ok",
        "pass": "trigger_wiki",
        "run_id": run_id,
        "run_status": run_status,
        "suggested": True,
        "signal_path": str(signal_path),
        "signal_written": not args.dry_run,
        "command_configured": bool(command),
        "command_result": command_result,
        "dry_run": args.dry_run,
        **ts,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
