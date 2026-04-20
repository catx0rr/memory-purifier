#!/usr/bin/env python3
"""Discover consolidated lower-substrate inputs for the memory purifier.

Applies the source-contract allow/deny lists from references/source-contract.md
and emits one JSON inventory object to stdout. Read-only; never writes files.
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


ALLOWED_SHARED = ["MEMORY.md", "RTMEMORY.md", "PROCEDURES.md"]
ALLOWED_PERSONAL = ["CHRONICLES.md", "DREAMS.md"]
EPISODES_GLOB_DIR = "episodes"

DENIED_FILES = {
    "CONSTITUTION.md",
    "KNOWLEDGE.md",
    "AGENTS.md",
    "SOUL.md",
    "HARNESS.md",
    "LTMEMORY.md",
    "PLAYBOOKS.md",
    "EPISODES.md",
    "HISTORY.md",
    "WISHES.md",
    "TRENDS.md",
}
DENIED_DIR_PREFIXES = (
    "memory/",
    "runtime/",
)

DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"
DEFAULT_REFLECTIONS_CONFIG = Path.home() / ".openclaw" / "reflections" / "reflections.json"


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def resolve_workspace(cli_arg: str, config_path: Path = None) -> Path:
    """CLI → config.paths.workspace → $WORKSPACE → default."""
    if cli_arg:
        return Path(cli_arg).expanduser().resolve()
    if config_path and config_path.is_file():
        cfg = _load_json_safely(config_path)
        cfg_ws = (cfg.get("paths") or {}).get("workspace")
        if isinstance(cfg_ws, str) and cfg_ws:
            return Path(cfg_ws).expanduser().resolve()
    env = os.environ.get("WORKSPACE")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.home() / ".openclaw" / "workspace").resolve()


def _load_json_safely(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def resolve_profile(cli_arg: str, config_path: Path) -> str:
    if cli_arg:
        return cli_arg
    env = os.environ.get("PROFILE")
    if env:
        return env
    if config_path.is_file():
        cfg = _load_json_safely(config_path)
        prof = cfg.get("profile")
        if prof in ("business", "personal"):
            return prof
    if DEFAULT_REFLECTIONS_CONFIG.is_file():
        cfg = _load_json_safely(DEFAULT_REFLECTIONS_CONFIG)
        prof = cfg.get("profile")
        # Translate reflections-hybrid profile names to purifier names.
        if prof == "personal-assistant":
            return "personal"
        if prof == "business-employee":
            return "business"
        if prof in ("business", "personal"):
            return prof
    return "personal"


def resolve_timezone(config_path: Path) -> str:
    if config_path.is_file():
        cfg = _load_json_safely(config_path)
        tz = cfg.get("timezone")
        if isinstance(tz, str) and tz:
            return tz
    return "Asia/Manila"


def file_fingerprint(path: Path) -> dict:
    stat = path.stat()
    data = path.read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    mtime_utc = (
        datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return {
        "bytes": stat.st_size,
        "mtime_utc": mtime_utc,
        "content_hash": f"sha256:{sha}",
    }


def is_denied(rel_path: str) -> bool:
    if rel_path in DENIED_FILES:
        return True
    for prefix in DENIED_DIR_PREFIXES:
        if rel_path.startswith(prefix):
            return True
    return False


def _add_if_present(workspace: Path, name: str, found: list, missing: list, severity: str) -> None:
    p = workspace / name
    if p.is_file():
        found.append({"path": name, **file_fingerprint(p)})
    else:
        missing.append({"path": name, "severity": severity})


def discover(workspace: Path, profile: str, extra_check_paths: list) -> tuple:
    found: list = []
    missing: list = []
    denied_attempts: list = []

    for name in ALLOWED_SHARED:
        _add_if_present(workspace, name, found, missing, severity="warn")

    episodes_dir = workspace / EPISODES_GLOB_DIR
    if episodes_dir.is_dir():
        for ep in sorted(episodes_dir.glob("*.md")):
            rel = str(ep.relative_to(workspace))
            found.append({"path": rel, **file_fingerprint(ep)})
    else:
        missing.append({"path": f"{EPISODES_GLOB_DIR}/", "severity": "warn"})

    if profile == "personal":
        for name in ALLOWED_PERSONAL:
            _add_if_present(workspace, name, found, missing, severity="info")

    for raw in extra_check_paths:
        candidate = Path(raw).expanduser()
        try:
            rel = str(candidate.resolve().relative_to(workspace))
        except ValueError:
            rel = str(candidate)
        if is_denied(rel):
            denied_attempts.append({"path": raw, "resolved": rel, "reason": "matches deny-list"})

    return found, missing, denied_attempts


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Discover memory-purifier inputs from workspace. "
                    "Emits one JSON object to stdout.",
    )
    ap.add_argument("--workspace", help="Workspace root (default: $WORKSPACE or ~/.openclaw/workspace)")
    ap.add_argument("--profile", choices=["business", "personal"], help="Profile scope (default: from config or 'personal')")
    ap.add_argument("--config", help=f"Path to memory-purifier.json (default: {DEFAULT_CONFIG})")
    ap.add_argument(
        "--check-path",
        action="append",
        default=[],
        help="Extra path to validate against deny-list (may be repeated)",
    )
    ap.add_argument("--dry-run", action="store_true", help="No effect on discovery (always read-only); echoed in output for chain compatibility")

    args = ap.parse_args()

    config_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG
    workspace = resolve_workspace(args.workspace, config_path)
    profile = resolve_profile(args.profile, config_path)
    tz_name = resolve_timezone(config_path)

    if not workspace.is_dir():
        out = {
            "status": "error",
            "error": f"workspace does not exist: {workspace}",
            "workspace": str(workspace),
            "profile": profile,
            "found": [],
            "missing": [],
            "denied_attempts": [],
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    found, missing, denied = discover(workspace, profile, args.check_path)

    status = "ok"
    if denied:
        status = "error"
    elif not found:
        status = "skipped"

    out = {
        "status": status,
        "workspace": str(workspace),
        "profile": profile,
        "found": found,
        "missing": missing,
        "denied_attempts": denied,
        "dry_run": args.dry_run,
        **timestamp_triple(tz_name),
    }

    if status == "error":
        out["error"] = "denied_attempts contains deny-list matches"
    elif status == "skipped":
        out["reason"] = "no eligible inputs found in workspace"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
