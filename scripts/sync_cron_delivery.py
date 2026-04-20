#!/usr/bin/env python3
"""Deterministic cron delivery sync — align openclaw cron registration
with `memoryPurifier.reporting.enabled` in <workspace>/runtime/memory-state.json.

Context
-------
At install time, `install.sh --cron-announce <true|false>` seeds both
the cron job delivery form and the `memoryPurifier.reporting.enabled`
toggle in the shared memory-state file. The two forms are:

  - announce=true  -> `--announce --channel <channel> --to <id>` (explicit
                      OpenClaw delivery mode, not merely the absence of
                      `--no-deliver`)
  - announce=false -> `--no-deliver` (silent)

Over time these can drift — the operator flips `reporting.enabled` in
memory-state.json but the existing cron job still carries the old
delivery form (or vice versa).

This helper is the single deterministic actor that resolves that drift.
The cron supervisor prompts call this helper *before* chat delivery so
the NEXT run is correctly configured (the current run's delivery mode
is whatever the cron scheduler already decided when it fired).

Behavior
--------
1. Read desired state from `<workspace>/runtime/memory-state.json`
   -> `memoryPurifier.reporting.{enabled, delivery.channel, delivery.to}`.
2. Read current state via `openclaw cron list --json` and filter for
   memory-purifier-* jobs.
3. For each job, compare its delivery form to the desired state:
   - match   -> no-op (record as in_sync)
   - mismatch -> delete + re-add with the corrected delivery form
                 (`--announce --channel --to` or `--no-deliver`),
                 preserving cron expression, tz, session, launcher
                 message, and timeout.
   - announce flip with missing channel/to -> record as
     `skipped_missing_announce_target`, do not mutate.
4. Emit a single JSON object summarizing the sync.

Exit code is 0 on structured success (even if some jobs could not be
reconciled — that state is reported in the JSON). Exit code 2 is
reserved for input/config errors. The cron supervisor prompts treat
any exit code as non-fatal and proceed using the current run's actual
delivery.

CLI
---
    python3 sync_cron_delivery.py \\
        --workspace <workspace> \\
        [--config <path>] \\
        [--skill-root <path>] \\
        [--dry-run] \\
        [--verbose]

`--dry-run` prints the plan without mutating cron.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MEMORY_PURIFIER_JOB_PREFIX = "memory-purifier-"
INCREMENTAL_PROMPT_FILENAME = "incremental-purifier-prompt.md"
RECONCILIATION_PROMPT_FILENAME = "reconciliation-purifier-prompt.md"


# ── time ──────────────────────────────────────────────────────────────

def _timestamp_triple() -> dict[str, str]:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    tz = now_local.tzinfo
    tz_name = getattr(tz, "key", None) or now_local.strftime("%Z") or now_local.strftime("UTC%z")
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


# ── state readers ─────────────────────────────────────────────────────

def read_reporting_state(workspace: Path) -> dict[str, Any] | None:
    """Read `memoryPurifier.reporting` from memory-state.json.

    Returns `{enabled: bool, delivery_channel: str | None, delivery_to: str | None}`,
    or `None` when the file/namespace is unreadable — the helper treats an
    unreadable state as "no desired state" and becomes a no-op.
    """
    state_path = workspace / "runtime" / "memory-state.json"
    if not state_path.is_file():
        return None
    try:
        data = json.loads(state_path.read_text())
    except Exception:
        return None
    mp = data.get("memoryPurifier")
    if not isinstance(mp, dict):
        return None
    reporting = mp.get("reporting")
    if not isinstance(reporting, dict):
        return None
    enabled = reporting.get("enabled")
    if not isinstance(enabled, bool):
        return None
    delivery = reporting.get("delivery") if isinstance(reporting.get("delivery"), dict) else {}
    channel = delivery.get("channel")
    to = delivery.get("to")
    return {
        "enabled": enabled,
        "delivery_channel": channel if isinstance(channel, str) and channel else None,
        "delivery_to": to if isinstance(to, str) and to else None,
    }


def read_reporting_enabled(workspace: Path) -> bool | None:
    """Back-compat shim — returns only the enabled boolean."""
    state = read_reporting_state(workspace)
    return None if state is None else state["enabled"]


def read_cron_config(config_path: Path | None) -> dict[str, Any]:
    """Read `cron.{tz, timeout_seconds, announce_channel, announce_to}`
    from memory-purifier.json as fallback values for re-registration.
    Safe defaults when absent."""
    defaults = {
        "tz": "Asia/Manila",
        "timeout_seconds": 1200,
        "announce_channel": None,
        "announce_to": None,
    }
    if config_path is None or not config_path.is_file():
        return defaults
    try:
        data = json.loads(config_path.read_text())
    except Exception:
        return defaults
    cron = data.get("cron") or {}
    if not isinstance(cron, dict):
        return defaults
    tz = cron.get("tz")
    timeout = cron.get("timeout_seconds")
    announce_channel = cron.get("announce_channel")
    announce_to = cron.get("announce_to")
    return {
        "tz": tz if isinstance(tz, str) and tz else defaults["tz"],
        "timeout_seconds": timeout if isinstance(timeout, int) and timeout > 0 else defaults["timeout_seconds"],
        "announce_channel": announce_channel if isinstance(announce_channel, str) and announce_channel else None,
        "announce_to": announce_to if isinstance(announce_to, str) and announce_to else None,
    }


# ── openclaw cron wrappers ────────────────────────────────────────────

def _openclaw_available() -> bool:
    return shutil.which("openclaw") is not None


def _list_cron_jobs() -> list[dict[str, Any]]:
    """Return memory-purifier-* jobs from `openclaw cron list --json`.

    Returns [] when openclaw is absent, the list call fails, or the
    output is unparseable — callers treat that as "nothing to sync".
    """
    if not _openclaw_available():
        return []
    try:
        proc = subprocess.run(
            ["openclaw", "cron", "list", "--json"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return []
    if proc.returncode != 0 or not proc.stdout.strip():
        return []
    try:
        jobs = json.loads(proc.stdout)
    except Exception:
        return []
    if not isinstance(jobs, list):
        return []
    return [j for j in jobs if isinstance(j, dict) and str(j.get("name", "")).startswith(MEMORY_PURIFIER_JOB_PREFIX)]


def _job_delivery_enabled(job: dict[str, Any]) -> bool | None:
    """Interpret the `deliver` / `no_deliver` / `announce` fields.

    Different openclaw versions expose the flag under different names.
    We accept any of:
      - `deliver` : bool
      - `no_deliver` : bool  (inverted)
      - `announce` : bool
      - `delivery` : "announce" | "no-deliver"
    Returns None when we cannot determine.
    """
    if "deliver" in job and isinstance(job["deliver"], bool):
        return job["deliver"]
    if "announce" in job and isinstance(job["announce"], bool):
        return job["announce"]
    if "no_deliver" in job and isinstance(job["no_deliver"], bool):
        return not job["no_deliver"]
    if "noDeliver" in job and isinstance(job["noDeliver"], bool):
        return not job["noDeliver"]
    delivery = job.get("delivery")
    if isinstance(delivery, str):
        if delivery == "announce":
            return True
        if delivery in ("no-deliver", "no_deliver", "silent"):
            return False
    return None


def _delete_job(name: str) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["openclaw", "cron", "delete", "--name", name],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"delete-exec-failed: {exc}"
    if proc.returncode != 0:
        return False, f"delete-exit-{proc.returncode}: {proc.stderr.strip()[:200]}"
    return True, ""


def _add_job(
    *,
    name: str,
    cron_expr: str,
    tz: str,
    message: str,
    timeout_seconds: int,
    announce: bool,
    announce_channel: str | None = None,
    announce_to: str | None = None,
) -> tuple[bool, str]:
    """Register one cron job. Announce mode uses explicit `--announce
    --channel <X> --to <Y>` per OpenClaw docs; no-deliver mode uses
    `--no-deliver`. Returns (ok, error-string)."""
    argv = [
        "openclaw", "cron", "add",
        "--name", name,
        "--cron", cron_expr,
        "--tz", tz,
        "--session", "isolated",
        "--timeout-seconds", str(timeout_seconds),
    ]
    if announce:
        if not announce_channel or not announce_to:
            return False, (
                "announce-missing-target: channel or to is empty. "
                "The sync helper refuses to register announce mode without both — "
                "set cron.announce_channel / cron.announce_to in memory-purifier.json, "
                "or run install.sh with --cron-announce-channel / --cron-announce-to."
            )
        argv.extend(["--announce", "--channel", announce_channel, "--to", announce_to])
    else:
        argv.append("--no-deliver")
    argv.extend(["--message", message])
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    except Exception as exc:  # noqa: BLE001
        return False, f"add-exec-failed: {exc}"
    if proc.returncode != 0:
        return False, f"add-exit-{proc.returncode}: {proc.stderr.strip()[:200]}"
    return True, ""


# ── message reconstruction ────────────────────────────────────────────

def _launcher_message_for(name: str, skill_root: Path) -> str:
    """Rebuild the short launcher message that points cron at the right prompt.

    The prompt file path is resolved to an absolute path so the cron
    session can follow it reliably.
    """
    if "reconciliation" in name:
        prompt = skill_root / "prompts" / RECONCILIATION_PROMPT_FILENAME
    else:
        prompt = skill_root / "prompts" / INCREMENTAL_PROMPT_FILENAME
    return (
        "Run memory purifier.\n"
        "\n"
        f"Read `{prompt}` and follow every step strictly."
    )


def _effective_message(existing_message: Any, name: str, skill_root: Path) -> str:
    """Prefer the existing message when it's already a launcher-shaped string.
    Otherwise regenerate from skill_root. Never re-register with the raw
    prompt-file path as the message.
    """
    if isinstance(existing_message, str) and existing_message.strip().startswith("Run memory purifier"):
        return existing_message
    return _launcher_message_for(name, skill_root)


# ── core sync ─────────────────────────────────────────────────────────

def sync(
    *,
    workspace: Path,
    config_path: Path | None,
    skill_root: Path,
    dry_run: bool,
) -> dict[str, Any]:
    plan: dict[str, Any] = {
        "ok": True,
        "status": "in_sync",
        "dry_run": dry_run,
        "workspace": str(workspace),
        "skill_root": str(skill_root),
        "desired_reporting_enabled": None,
        "desired_channel": None,
        "desired_to": None,
        "openclaw_available": _openclaw_available(),
        "jobs": [],
        "changes": 0,
        "errors": [],
        **_timestamp_triple(),
    }

    reporting_state = read_reporting_state(workspace)
    if reporting_state is None:
        plan["status"] = "skipped_no_desired_state"
        return plan
    desired = reporting_state["enabled"]
    plan["desired_reporting_enabled"] = desired

    # Resolve announce channel/to early (memory-state wins over config fallback)
    # so the result JSON surfaces these fields even when we later short-circuit
    # on `skipped_no_openclaw` or `skipped_no_jobs`. Useful for operators
    # debugging a sync from a workstation without openclaw installed.
    cron_cfg = read_cron_config(config_path)
    resolved_channel = reporting_state["delivery_channel"] or cron_cfg["announce_channel"]
    resolved_to = reporting_state["delivery_to"] or cron_cfg["announce_to"]
    plan["desired_channel"] = resolved_channel
    plan["desired_to"] = resolved_to

    if not plan["openclaw_available"]:
        plan["status"] = "skipped_no_openclaw"
        return plan

    jobs = _list_cron_jobs()

    if not jobs:
        plan["status"] = "skipped_no_jobs"
        return plan

    for job in jobs:
        name = str(job.get("name", ""))
        if not name.startswith(MEMORY_PURIFIER_JOB_PREFIX):
            continue
        current = _job_delivery_enabled(job)
        entry: dict[str, Any] = {
            "name": name,
            "current_deliver": current,
            "desired_deliver": desired,
            "action": None,
        }

        if current is None:
            entry["action"] = "skipped_indeterminate"
            plan["jobs"].append(entry)
            continue

        if current == desired:
            entry["action"] = "in_sync"
            plan["jobs"].append(entry)
            continue

        # Mismatch — rebuild the job with the corrected delivery flag.
        cron_expr = str(job.get("cron") or job.get("schedule") or "").strip()
        tz = str(job.get("tz") or job.get("timezone") or cron_cfg["tz"]).strip()
        timeout_seconds = job.get("timeout_seconds") or job.get("timeoutSeconds") or cron_cfg["timeout_seconds"]
        try:
            timeout_seconds = int(timeout_seconds)
            if timeout_seconds <= 0:
                timeout_seconds = int(cron_cfg["timeout_seconds"])
        except (TypeError, ValueError):
            timeout_seconds = int(cron_cfg["timeout_seconds"])
        message = _effective_message(job.get("message"), name, skill_root)

        if not cron_expr or not tz:
            entry["action"] = "skipped_missing_fields"
            entry["reason"] = "listing did not expose cron/tz"
            plan["jobs"].append(entry)
            continue

        # If the flip is toward announce, both channel and target must be
        # resolvable from memory-state or cron config. Refuse to mutate when
        # either is missing — surfacing a targeted skip rather than silently
        # registering an incomplete announce (which would be a repeat of the
        # bug v1.3.0 is fixing).
        if desired and (not resolved_channel or not resolved_to):
            entry["action"] = "skipped_missing_announce_target"
            entry["reason"] = (
                "delivery.channel and delivery.to must both be set in "
                "memory-state.json or cron.announce_channel / cron.announce_to "
                "in memory-purifier.json before syncing to announce mode."
            )
            plan["jobs"].append(entry)
            continue

        entry["planned_cron"] = cron_expr
        entry["planned_tz"] = tz
        entry["planned_timeout_seconds"] = timeout_seconds
        if desired:
            entry["planned_delivery"] = {"mode": "announce", "channel": resolved_channel, "to": resolved_to}
        else:
            entry["planned_delivery"] = {"mode": "no-deliver"}

        if dry_run:
            entry["action"] = "would_update"
            plan["changes"] += 1
            plan["jobs"].append(entry)
            continue

        ok_del, del_err = _delete_job(name)
        if not ok_del:
            entry["action"] = "delete_failed"
            entry["error"] = del_err
            plan["errors"].append({"job": name, "phase": "delete", "error": del_err})
            plan["ok"] = False
            plan["jobs"].append(entry)
            continue

        ok_add, add_err = _add_job(
            name=name,
            cron_expr=cron_expr,
            tz=tz,
            message=message,
            timeout_seconds=int(timeout_seconds),
            announce=desired,
            announce_channel=resolved_channel if desired else None,
            announce_to=resolved_to if desired else None,
        )
        if not ok_add:
            entry["action"] = "add_failed"
            entry["error"] = add_err
            plan["errors"].append({"job": name, "phase": "add", "error": add_err})
            plan["ok"] = False
            plan["jobs"].append(entry)
            continue

        entry["action"] = "updated"
        plan["changes"] += 1
        plan["jobs"].append(entry)

    if plan["changes"] > 0 and plan["ok"]:
        plan["status"] = "synced"
    elif plan["changes"] == 0 and plan["ok"]:
        plan["status"] = "in_sync"
    else:
        plan["status"] = "partial_failure"

    return plan


# ── CLI ───────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Align openclaw cron delivery mode with memoryPurifier.reporting.enabled. "
            "Announce mode uses the explicit OpenClaw form (--announce --channel <X> --to <Y>); "
            "no-deliver mode uses --no-deliver. Single deterministic actor for cron delivery "
            "drift — called by the cron supervisor prompts."
        ),
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path(os.environ.get("WORKSPACE", Path.home() / ".openclaw" / "workspace")),
        help="Workspace root containing runtime/memory-state.json.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "memory-purifier.json path. Default: "
            "$CONFIG_ROOT/memory-purifier/memory-purifier.json "
            "($CONFIG_ROOT default: $HOME/.openclaw). Used only as fallback "
            "for cron.tz / cron.timeout_seconds when the cron listing is "
            "missing those fields."
        ),
    )
    parser.add_argument(
        "--skill-root",
        type=Path,
        default=None,
        help=(
            "memory-purifier skill root (parent of prompts/). Default: "
            "$SKILL_ROOT env, or derived from this script's parent."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan without mutating cron registration.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="(Reserved) Reserved for future debug output. Current behavior is unaffected.",
    )

    args = parser.parse_args(argv)

    workspace = args.workspace.resolve()
    config_path = args.config
    if config_path is None:
        cfg_root = Path(os.environ.get("CONFIG_ROOT", Path.home() / ".openclaw"))
        config_path = cfg_root / "memory-purifier" / "memory-purifier.json"
    config_path = config_path.resolve() if config_path.exists() else config_path

    skill_root = args.skill_root
    if skill_root is None:
        env_root = os.environ.get("SKILL_ROOT")
        if env_root:
            skill_root = Path(env_root)
        else:
            skill_root = Path(__file__).resolve().parent.parent
    skill_root = skill_root.resolve()

    result = sync(
        workspace=workspace,
        config_path=config_path if config_path and config_path.exists() else None,
        skill_root=skill_root,
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
