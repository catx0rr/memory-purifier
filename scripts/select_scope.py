#!/usr/bin/env python3
"""Select the processing scope for a purifier run.

Reads an inventory (from discover_sources.py) plus the manifest cursor and
decides which files feed the extractor:

- incremental mode: only files whose content_hash changed since lastSuccessfulCursor
- reconciliation mode: full inventory (widened horizon)
- first run (no prior cursor): full-sweep

Emits one JSON object to stdout.
"""

import argparse
import hashlib
import json
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


def _load_inventory(arg: str) -> dict:
    if arg == "-":
        return json.load(sys.stdin)
    return json.loads(Path(arg).expanduser().read_text())


def _load_manifest_cursor(manifest_path: Path) -> tuple:
    """Return (prior_cursor, prior_inventory_map) or (None, {}) on any failure."""
    if not manifest_path.is_file():
        return None, {}
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return None, {}
    prior_cursor = manifest.get("lastSuccessfulCursor")
    prior_inventory = manifest.get("sourceInventory") or []
    inv_map = {
        item.get("path"): item.get("content_hash")
        for item in prior_inventory
        if isinstance(item, dict) and item.get("path")
    }
    return prior_cursor, inv_map


def build_cursor(found: list) -> str:
    """Deterministic hash of the {path: content_hash} inventory.

    Stable across reruns with unchanged inputs; differs when any input
    content changes. The purifier's idempotency contract relies on this.
    """
    pairs = sorted((f["path"], f["content_hash"]) for f in found if f.get("path"))
    body = "\n".join(f"{p}:{h}" for p, h in pairs)
    return "cursor-v1:" + hashlib.sha256(body.encode()).hexdigest()[:16]


def detect_removed_sources(found: list, prior_inventory_map: dict) -> list:
    """Return source paths that were tracked by the prior run but are absent now.

    Supports incremental retirement: if a source file disappears or its content
    is no longer in the workspace, claims that depended on it become candidates
    for stale/retire_candidate marking downstream in assemble_artifacts.py.
    """
    if not prior_inventory_map:
        return []
    current_paths = {f.get("path") for f in found if f.get("path")}
    return sorted(p for p in prior_inventory_map.keys() if p and p not in current_paths)


def select(found: list, mode: str, prior_cursor, prior_inventory_map) -> tuple:
    """Return (scope_files, delta_type, new_cursor, removed_sources)."""
    new_cursor = build_cursor(found)
    removed = detect_removed_sources(found, prior_inventory_map)

    if mode == "reconciliation":
        return list(found), "full_reconciliation", new_cursor, removed

    if prior_cursor is None or not prior_inventory_map:
        return list(found), "first_run_full", new_cursor, removed

    scope = [
        f for f in found
        if prior_inventory_map.get(f["path"]) != f["content_hash"]
    ]
    return scope, "delta", new_cursor, removed


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Select processing scope given an inventory, mode, and manifest cursor.",
    )
    ap.add_argument("--inventory", required=True, help="Path to inventory JSON (from discover_sources.py) or '-' for stdin")
    ap.add_argument("--mode", required=True, choices=["incremental", "reconciliation"])
    ap.add_argument("--manifest", help="Path to purified-manifest.json (for cursor + prior inventory)")
    ap.add_argument("--timezone", default=None, help="IANA timezone name for timestamp triple (default: from inventory or Asia/Manila)")
    ap.add_argument("--dry-run", action="store_true", help="Read-only; echoed in output for chain compatibility")

    args = ap.parse_args()

    try:
        inventory = _load_inventory(args.inventory)
    except Exception as e:
        out = {
            "status": "error",
            "error": f"failed to load inventory: {type(e).__name__}: {e}",
            "dry_run": args.dry_run,
            **timestamp_triple(args.timezone or "Asia/Manila"),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    tz_name = args.timezone or inventory.get("timezone") or "Asia/Manila"

    if inventory.get("status") != "ok":
        out = {
            "status": "skipped",
            "reason": f"inventory status is {inventory.get('status')!r}",
            "inventory_status": inventory.get("status"),
            "mode": args.mode,
            "scope": [],
            "scope_count": 0,
            "cursor_before": None,
            "cursor_new": None,
            "delta_type": None,
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    found = inventory.get("found", [])
    workspace = inventory.get("workspace")
    profile = inventory.get("profile")

    prior_cursor, prior_inv_map = (None, {})
    if args.manifest:
        prior_cursor, prior_inv_map = _load_manifest_cursor(Path(args.manifest).expanduser())

    scope, delta_type, new_cursor, removed_sources = select(found, args.mode, prior_cursor, prior_inv_map)

    status = "ok" if scope else "skipped"

    out = {
        "status": status,
        "workspace": workspace,
        "profile": profile,
        "mode": args.mode,
        "scope": scope,
        "scope_count": len(scope),
        "cursor_before": prior_cursor,
        "cursor_new": new_cursor,
        "delta_type": delta_type,
        # Paths in `removed_sources` were present in the prior manifest's sourceInventory
        # but are absent from the current workspace. The orchestrator forwards these to
        # assemble_artifacts for stale/retire_candidate marking, independently of scope.
        "removed_sources": removed_sources,
        "dry_run": args.dry_run,
        **timestamp_triple(tz_name),
    }

    if status == "skipped":
        if removed_sources:
            out["reason"] = f"no new inputs; {len(removed_sources)} prior source(s) removed — stale sweep may apply"
        else:
            out["reason"] = "no changed or new inputs since last successful cursor"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
