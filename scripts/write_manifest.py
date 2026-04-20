#!/usr/bin/env python3
"""Finalize a purifier run — local state only.

Aggregates upstream-phase outputs (inventory, scope, pass1, pass2, assemble)
and writes:
- <runtime>/purified-manifest.json   (full run metadata — atomic rewrite)
- <runtime>/purifier-last-run-summary.json    (compact status — atomic rewrite)
- <config>/memory-purifier.json      (update lastRun + cursor — atomic rewrite)

Cursor advances only on status=="ok". Partial failures leave the cursor where
it was — the next run re-reads the same inventory delta.

Shared-memory-log telemetry (`memory-log-YYYY-MM-DD.jsonl`) and the
`last-run.md` markdown are written by `run_purifier.py` AFTER
`validate_outputs.py` runs, so those surfaces reflect final validated state.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def _load_json_maybe(path_str: str):
    if not path_str:
        return None
    p = Path(path_str).expanduser()
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _first_nonempty(*objs, key):
    for o in objs:
        if o and o.get(key) not in (None, ""):
            return o[key]
    return None


def _atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Finalize a purifier run: manifest + summary + telemetry + cursor.")
    ap.add_argument("--inventory", help="Inventory JSON path")
    ap.add_argument("--scope", help="Scope JSON path")
    ap.add_argument("--pass1", help="Pass 1 JSON path")
    ap.add_argument("--pass2", help="Pass 2 JSON path")
    ap.add_argument("--assemble", help="assemble_artifacts.py output JSON path")
    ap.add_argument("--run-id", help="Explicit run_id (default: first found among upstream outputs)")
    ap.add_argument("--mode", help="Run mode override")
    ap.add_argument("--profile", help="Profile scope override")
    ap.add_argument("--workspace", help="Workspace root override")
    ap.add_argument("--runtime-dir", help="Runtime dir override")
    ap.add_argument("--telemetry-root", help="Telemetry root override")
    ap.add_argument("--config", help="Path to memory-purifier.json config")
    ap.add_argument(
        "--status",
        choices=["ok", "skipped", "partial_failure", "error"],
        default="ok",
        help="Final run status",
    )
    ap.add_argument("--warnings", default="[]", help="JSON array of extra warnings to record")
    ap.add_argument("--partial-failures", default="[]", help="JSON array of extra partial failures to record")
    ap.add_argument("--views-rendered", default="[]", help="JSON array of rendered view paths (from render_views.py)")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument("--dry-run", action="store_true", help="Compute everything; do not write files")

    args = ap.parse_args()

    inv = _load_json_maybe(args.inventory)
    scope = _load_json_maybe(args.scope)
    pass1 = _load_json_maybe(args.pass1)
    pass2 = _load_json_maybe(args.pass2)
    assemble = _load_json_maybe(args.assemble)

    tz_name = args.timezone
    for src in (inv, scope, pass1, pass2, assemble):
        if src and src.get("timezone"):
            tz_name = tz_name or src["timezone"]
    tz_name = tz_name or "Asia/Manila"
    finished_ts = timestamp_triple(tz_name)

    run_id = args.run_id or _first_nonempty(pass2, assemble, pass1, scope, inv, key="run_id")
    if not run_id:
        out = {
            "status": "error",
            "error": "could not determine run_id from any upstream input or --run-id",
            "pass": "manifest",
            **finished_ts,
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    mode = args.mode or _first_nonempty(pass2, pass1, scope, key="mode") or "incremental"
    profile_scope = (
        args.profile
        or _first_nonempty(pass2, pass1, key="profile_scope")
        or (inv or {}).get("profile")
        or "business"
    )
    started_ts_local = _first_nonempty(inv, scope, pass1, pass2, assemble, key="timestamp")

    workspace_hint = (
        args.workspace
        or _first_nonempty(inv, pass1, pass2, assemble, key="workspace")
        or os.environ.get("WORKSPACE")
    )
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")
    telemetry_root = Path(args.telemetry_root).expanduser() if args.telemetry_root else (Path.home() / ".openclaw" / "telemetry" / "memory-purifier")
    config_path = Path(args.config).expanduser() if args.config else (Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json")

    manifest_path = runtime_dir / "purified-manifest.json"
    summary_path = runtime_dir / "purifier-last-run-summary.json"

    try:
        extra_warnings = json.loads(args.warnings)
        if not isinstance(extra_warnings, list):
            extra_warnings = []
    except json.JSONDecodeError:
        extra_warnings = []
    try:
        extra_partials = json.loads(args.partial_failures)
        if not isinstance(extra_partials, list):
            extra_partials = []
    except json.JSONDecodeError:
        extra_partials = []
    try:
        views_rendered = json.loads(args.views_rendered)
        if not isinstance(views_rendered, list):
            views_rendered = []
    except json.JSONDecodeError:
        views_rendered = []

    warnings = list(extra_warnings)
    for src in (inv, scope, pass1, pass2, assemble):
        if src and isinstance(src.get("warnings"), list):
            warnings.extend(src["warnings"])

    partial_failures = list(extra_partials)
    for src in (pass1, pass2, assemble):
        if src and src.get("status") == "partial_failure":
            partial_failures.append({
                "pass": src.get("pass") or "unknown",
                "attempts": src.get("attempts"),
                "errors": src.get("errors"),
                "failed_record_path": src.get("failed_record_path"),
            })

    source_inventory = [
        {"path": f["path"], "content_hash": f.get("content_hash")}
        for f in ((inv or {}).get("found") or [])
    ]
    processed_segments = [s.get("path") for s in ((scope or {}).get("scope") or []) if s.get("path")]

    promotion_stats = (pass1 or {}).get("verdict_stats") or {}
    claim_stats = (pass2 or {}).get("status_stats") or {}
    home_stats = (pass2 or {}).get("home_stats") or {}

    cursor_new = (scope or {}).get("cursor_new")

    downstream_suggested = (
        args.status == "ok"
        and (pass2 or {}).get("status") == "ok"
        and (assemble or {}).get("status") == "ok"
    )

    manifest = {
        "version": "1.4.0",
        "runId": run_id,
        "mode": mode,
        "status": args.status,
        "startedAt": started_ts_local,
        "finishedAt": finished_ts["timestamp"],
        "finishedAt_utc": finished_ts["timestamp_utc"],
        "timezone": finished_ts["timezone"],
        "profileScope": profile_scope,
        "sourceInventory": source_inventory,
        "processedSegments": processed_segments,
        "promotionStats": promotion_stats,
        "claimStats": claim_stats,
        "homeStats": home_stats,
        "warnings": warnings,
        "partialFailures": partial_failures,
        "lastSuccessfulCursor": cursor_new if args.status == "ok" else (
            _load_json_maybe(str(manifest_path)) or {}
        ).get("lastSuccessfulCursor"),
        "downstreamWikiIngestSuggested": bool(downstream_suggested),
    }

    duration_seconds = None
    if started_ts_local:
        try:
            started_dt = datetime.fromisoformat(started_ts_local)
            finished_dt = datetime.fromisoformat(finished_ts["timestamp"])
            duration_seconds = (finished_dt - started_dt).total_seconds()
        except ValueError:
            duration_seconds = None

    # last-run-summary mirrors the canonical final-report shape emitted by
    # run_purifier.py so operators and downstream consumers see the same fields
    # regardless of which surface they inspect.
    summary = {
        "ok": args.status == "ok",
        "status": args.status,
        "mode": mode,
        "profile": profile_scope,
        "runId": run_id,
        "startedAt": started_ts_local,
        "finishedAt": finished_ts["timestamp"],
        "durationSeconds": duration_seconds,
        "claimsNew": (assemble or {}).get("claim_count_new", 0),
        "claimsTotal": (assemble or {}).get("claim_count_total", 0),
        "contradictionCount": (pass2 or {}).get("contradiction_count", 0),
        "supersessionCount": (pass2 or {}).get("supersession_count", 0),
        "warnings": warnings,
        "partialFailures": partial_failures,
        "warningCount": len(warnings),
        "partialFailureCount": len(partial_failures),
        "viewsRendered": views_rendered,
        "downstreamWikiIngestSuggested": bool(downstream_suggested),
        "manifestPath": str(manifest_path),
    }

    # Memory-log telemetry and `last-run.md` markdown are written by run_purifier.py
    # AFTER validate_outputs runs. write_manifest only handles local state
    # (manifest, summary, config cursor).

    config_update = None
    if config_path.is_file():
        try:
            cfg = json.loads(config_path.read_text())
        except json.JSONDecodeError:
            cfg = None
        if isinstance(cfg, dict):
            last_run = cfg.get("lastRun") or {}
            if args.status == "ok":
                last_run[mode] = finished_ts["timestamp"]
                if cursor_new is not None:
                    last_run["cursor"] = cursor_new
                cfg["lastRun"] = last_run
                config_update = cfg

    if not args.dry_run:
        runtime_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(manifest_path, manifest)
        _atomic_write_json(summary_path, summary)
        if config_update is not None:
            _atomic_write_json(config_path, config_update)

    out = {
        "status": args.status,
        "run_id": run_id,
        "pass": "manifest",
        "mode": mode,
        "profile_scope": profile_scope,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
        "config_updated": config_update is not None and not args.dry_run,
        "cursor_written": cursor_new if args.status == "ok" else None,
        "downstream_wiki_ingest_suggested": bool(downstream_suggested),
        "warnings_count": len(warnings),
        "partial_failures_count": len(partial_failures),
        "duration_seconds": duration_seconds,
        "dry_run": args.dry_run,
        **finished_ts,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
