#!/usr/bin/env python3
"""Memory-purifier orchestrator — entrypoint for cron and manual runs.

Chains the 11 deterministic scripts in the locked order from SKILL.md:

  discover_sources → select_scope → extract_candidates
  → score_promotion → cluster_survivors → score_purifier
  → assemble_artifacts → render_views
  → write_manifest → validate_outputs → trigger_wiki

Owns:
- run_id generation
- lock acquisition and stale-lock recovery
- staging directory for inter-step JSONs (kept on failure for post-mortem)
- mode selection (incremental | reconciliation)
- dry-run propagation
- model backend propagation to the two scoring steps
- clean summary JSON output

Each step's output is read, inspected for status, and forwarded to the next
step. Failures halt the chain cleanly and still emit a manifest so the
cursor does not advance past a broken run.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from _lib.time_utils import (  # noqa: E402
    local_date_str,
    local_report_timestamp,
    timestamp_triple,
)
from _lib.fs import atomic_write_json  # noqa: E402
from _lib.statuses import CANONICAL_TOP_LEVEL_STATUS_SET  # noqa: E402
from _lib.version import (  # noqa: E402
    PURIFIER_ARTIFACT_SCHEMA,
    PURIFIER_LOGIC_VERSION,
    PURIFIER_MANIFEST_SCHEMA,
    version_tuple,
)

DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"
DEFAULT_REFLECTIONS_CONFIG = Path.home() / ".openclaw" / "reflections" / "reflections.json"
STALE_LOCK_HOURS_DEFAULT = 2


def _tz_aware_now(tz_name: str) -> datetime:
    """Return ``datetime.now()`` anchored to the configured timezone.

    Prior to v1.5.0 several sites used ``datetime.now().astimezone()`` which
    picks up the host's system-local tz. On a host whose local tz differs
    from the configured one, cron-window checks and recall age calculations
    silently drift. This helper closes that gap.
    """
    try:
        return datetime.now(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError:
        return datetime.now(ZoneInfo("Asia/Manila"))


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
        if prof == "personal-assistant":
            return "personal"
        if prof == "business-employee":
            return "business"
        if prof in ("business", "personal"):
            return prof
    return "personal"


def resolve_timezone(cli_arg: str, config_path: Path) -> str:
    if cli_arg:
        return cli_arg
    if config_path.is_file():
        cfg = _load_json_safely(config_path)
        tz = cfg.get("timezone")
        if isinstance(tz, str) and tz:
            return tz
    return "Asia/Manila"


# v1.5.0 Contract 1 — locked final-status taxonomy.
# Single-sourced via ``_lib.statuses``; adding/removing a status MUST
# happen there (write_manifest argparse choices and validate_outputs
# status gate pull from the same tuple).
CANONICAL_STATUSES = CANONICAL_TOP_LEVEL_STATUS_SET
COMPONENT = "memory-purifier.purifier"


def _usage_unavailable() -> dict:
    return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "source": "unavailable"}


def _merge_usage(a: dict, b: dict) -> dict:
    """Aggregate two token_usage blocks. Source degrades to the weakest of the two."""
    src_rank = {"exact": 0, "approximate": 1, "unavailable": 2}
    a = a or _usage_unavailable()
    b = b or _usage_unavailable()
    a_src = a.get("source", "unavailable")
    b_src = b.get("source", "unavailable")
    merged_src = a_src if src_rank[a_src] >= src_rank[b_src] else b_src
    return {
        "prompt_tokens": int(a.get("prompt_tokens") or 0) + int(b.get("prompt_tokens") or 0),
        "completion_tokens": int(a.get("completion_tokens") or 0) + int(b.get("completion_tokens") or 0),
        "total_tokens": int(a.get("total_tokens") or 0) + int(b.get("total_tokens") or 0),
        "source": merged_src,
    }


def _build_final_report(
    status: str,
    ok: bool,
    run_id: str,
    mode: str,
    profile: str,
    manifest_path: Path,
    summary_path: Path,
    started_ts: dict,
    dry_run: bool,
    halt_reason: str = None,
    skip_reason: str = None,
    steps: dict = None,
    assemble: dict = None,
    pass2: dict = None,
    manifest: dict = None,
    validate: dict = None,
    trigger: dict = None,
    staging_dir: Path = None,
    extra: dict = None,
    token_usage: dict = None,
    global_memory_log_path: Path = None,
    latest_report_path: Path = None,
    skip_enrichment: dict = None,
    pass1: dict = None,
) -> dict:
    """Build the single authoritative final JSON report emitted to stdout.

    The cron prompts rely on this shape — do not change field names without
    updating prompts/incremental-purifier-prompt.md and
    prompts/reconciliation-purifier-prompt.md.
    """
    warnings_list = []
    partial_failures_list = []
    downstream_suggested = False

    if manifest:
        warnings_list = manifest.get("warnings") or []
        partial_failures_list = manifest.get("partialFailures") or []
        downstream_suggested = bool(manifest.get("downstreamWikiIngestSuggested"))
    else:
        if manifest_path.is_file():
            try:
                on_disk = json.loads(manifest_path.read_text())
                warnings_list = on_disk.get("warnings") or []
                partial_failures_list = on_disk.get("partialFailures") or []
                downstream_suggested = bool(on_disk.get("downstreamWikiIngestSuggested"))
            except Exception:
                pass

    out = {
        "ok": bool(ok),
        "status": status,
        "mode": mode,
        "profile": profile,
        "runId": run_id,
        "claimsNew": (assemble or {}).get("claim_count_new") or 0,
        "claimsTotal": (assemble or {}).get("claim_count_total") or 0,
        "contradictionCount": (pass2 or {}).get("contradiction_count") or 0,
        "supersessionCount": (pass2 or {}).get("supersession_count") or 0,
        "warnings": warnings_list,
        "partialFailures": partial_failures_list,
        "warningCount": len(warnings_list),
        "partialFailureCount": len(partial_failures_list),
        "downstreamWikiIngestSuggested": downstream_suggested,
        "tokenUsage": token_usage or _usage_unavailable(),
        # v1.6.0: surface backend info on the top-level report so operators
        # and cron supervisors see which backend actually scored without
        # drilling into sub-step outputs.
        "backend": (pass2 or {}).get("backend") or (pass1 or {}).get("backend"),
        "backendModel": (pass2 or {}).get("backend_model") or (pass1 or {}).get("backend_model"),
        "tokenUsageSource": (token_usage or _usage_unavailable()).get("source"),
        "manifestPath": str(manifest_path),
        "summaryPath": str(summary_path),
        "globalMemoryLogPath": str(global_memory_log_path) if global_memory_log_path else None,
        "latestReportPath": str(latest_report_path) if latest_report_path else None,
        "stagingDir": str(staging_dir) if staging_dir and staging_dir.exists() else None,
        "dryRun": dry_run,
        **started_ts,
    }
    if halt_reason:
        out["haltReason"] = halt_reason
    if skip_reason:
        out["skipReason"] = skip_reason
    if skip_enrichment:
        # Deterministic enrichment surfaces only on skip paths.
        # Override claimsTotal from enrichment (assemble result may be None on pre-pipeline skips).
        if "claimsTotal" in skip_enrichment:
            out["claimsTotal"] = skip_enrichment["claimsTotal"]
        out["nextSchedule"] = skip_enrichment.get("nextSchedule")
        out["recallSurface"] = skip_enrichment.get("recallSurface")
    if steps is not None:
        out["steps"] = steps
    if validate:
        out["validate"] = {
            "status": validate.get("status"),
            "errorCount": validate.get("error_count"),
            "warningCount": validate.get("warning_count"),
        }
    if trigger:
        out["trigger"] = {
            "status": trigger.get("status"),
            "signalWritten": trigger.get("signal_written"),
            "commandResult": trigger.get("command_result"),
        }
    if extra:
        out.update(extra)
    return out


def append_memory_log_event(
    global_log_root: Path,
    event: str,
    run_id: str,
    status: str,
    mode: str,
    profile: str,
    agent: str,
    token_usage: dict,
    details: dict,
    tz_name: str,
) -> Path:
    """Append a single JSON event line to the shared memory-log JSONL.

    The log path is `<global_log_root>/memory-log-YYYY-MM-DD.jsonl` and is
    shared across all memory plugins (reflections, purifier, etc.) so that
    `component` and `domain` are the filter keys for cross-plugin queries.
    """
    ts = timestamp_triple(tz_name)
    # v1.5.0 audit-corrective: date shard anchored to configured tz, not system-local.
    date_str = local_date_str(tz_name)
    path = global_log_root / f"memory-log-{date_str}.jsonl"
    record = {
        **ts,
        "domain": "memory",
        "component": COMPONENT,
        "event": event,
        "run_id": run_id,
        "status": status,
        "agent": agent,
        "profile": profile,
        "mode": mode,
        "token_usage": token_usage or _usage_unavailable(),
        "details": details or {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def write_latest_report(
    telemetry_root: Path,
    run_id: str,
    status: str,
    ok: bool,
    mode: str,
    profile: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    claims_new: int,
    claims_total: int,
    contradiction_count: int,
    supersession_count: int,
    views_rendered: list,
    warning_count: int,
    partial_failure_count: int,
    downstream_wiki_ingest_suggested: bool,
    token_usage: dict,
    manifest_path: Path,
    tz_name: str,
    halt_reason: str = None,
    skip_enrichment: dict = None,
) -> Path:
    """Write a deterministic operator-facing markdown report of this run.

    Overwritten every run — not an audit log, not canonical telemetry. The
    canonical telemetry is the shared memory-log JSONL.
    """
    telemetry_root.mkdir(parents=True, exist_ok=True)
    path = telemetry_root / "last-run.md"

    tu = token_usage or _usage_unavailable()
    duration_str = f"{duration_seconds:.1f}s" if isinstance(duration_seconds, (int, float)) else "—"
    status_line = f"**Status:** `{status}` ({'ok' if ok else 'not-ok'})"

    lines = [
        "# memory-purifier — last run",
        "",
        f"_Regenerated {local_report_timestamp(tz_name)} {tz_name}._",
        "",
        status_line,
        "",
        "## Run",
        "",
        f"- Run ID: `{run_id}`",
        f"- Mode: `{mode}`",
        f"- Profile: `{profile}`",
        f"- Started:  {started_at or '—'}",
        f"- Finished: {finished_at or '—'}",
        f"- Duration: {duration_str}",
    ]
    if halt_reason:
        lines.append(f"- Halt reason: {halt_reason}")
    lines.extend([
        "",
        "## Claims",
        "",
        f"- Claims new:          {claims_new}",
        f"- Claims total:        {claims_total}",
        f"- Contradiction count: {contradiction_count}",
        f"- Supersession count:  {supersession_count}",
        "",
        "## Rendered views",
        "",
    ])
    if views_rendered:
        for v in views_rendered:
            lines.append(f"- {v}")
    else:
        lines.append("_(none)_")
    lines.extend([
        "",
        "## Issues",
        "",
        f"- Warnings:         {warning_count}",
        f"- Partial failures: {partial_failure_count}",
        "",
        "## Token usage",
        "",
        f"Token Usage: prompt={tu.get('prompt_tokens', 0)}, "
        f"completion={tu.get('completion_tokens', 0)}, "
        f"total={tu.get('total_tokens', 0)} ({tu.get('source', 'unavailable')})",
        "",
        "## Downstream",
        "",
        f"- Wiki ingest suggested: {'yes' if downstream_wiki_ingest_suggested else 'no'}",
        f"- Manifest: `{manifest_path}`",
        "",
    ])

    # Skip enrichment — rendered only on skip states (local-report-only, never
    # chat). Gives the operator a deterministic snapshot of current claim
    # totals, next scheduled fire, and one oldest unresolved claim.
    if status in {"skipped", "skipped_superseded"} and skip_enrichment:
        lines.extend([
            "## Skip context",
            "",
            f"- Claims total: {skip_enrichment.get('claimsTotal', 0)}",
        ])
        ns = skip_enrichment.get("nextSchedule")
        if isinstance(ns, dict) and ns.get("when"):
            lines.append(f"- Next scheduled run: {ns['when']} ({ns.get('mode', '?')})")
        else:
            lines.append("- Next scheduled run: not resolvable")
        recall = skip_enrichment.get("recallSurface")
        if isinstance(recall, dict):
            age_part = f"age {recall.get('ageDays')}d" if recall.get("ageDays") is not None else "age unknown"
            lines.append(
                f"- Recall: [{recall.get('claimId','?')}] "
                f"{recall.get('type','?')} · {recall.get('primaryHome','?')} · "
                f"{age_part} — \"{recall.get('text','')}\""
            )
        lines.append("")

    content = "\n".join(lines)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)
    return path


def _resolve_agent_id() -> str:
    """Best-effort agent identifier for telemetry's `agent` field."""
    return (
        os.environ.get("OPENCLAW_AGENT_ID")
        or os.environ.get("AGENT_ID")
        or "unknown"
    )


def _is_reconciliation_window(config: dict, tz_name: str, now=None) -> tuple:
    """Is the current local time inside any reconciliation window from config.cadence?

    Each reconciliation cron expression is expected to look like:
      "<minute> <hour> * * <dow_set>"
    where `<hour>` is a single integer and `<dow_set>` is a comma-separated list
    of cron day-of-week values (Sun=0, Mon=1, … Sat=6). A current time is
    inside the window if today's DOW is in the set and the current hour is
    within ±1 hour of the target hour.

    Returns (in_window: bool, matching_expression: str | None).
    """
    if now is None:
        now = _tz_aware_now(tz_name)
    current_cron_dow = (now.weekday() + 1) % 7
    current_hour = now.hour

    exprs = (config.get("cadence") or {}).get("reconciliation") or []
    for expr in exprs:
        parts = expr.split()
        if len(parts) != 5:
            continue
        _, hour_field, _, _, dow_field = parts
        try:
            target_hour = int(hour_field)
        except ValueError:
            continue
        dow_set: set = set()
        for d in dow_field.split(","):
            try:
                dow_set.add(int(d))
            except ValueError:
                continue
        if current_cron_dow in dow_set and abs(current_hour - target_hour) <= 1:
            return True, expr
    return False, None


def _count_claims_file(workspace: Path) -> int:
    """Count non-empty lines in <workspace>/runtime/purified-claims.jsonl, or 0 if absent."""
    path = workspace / "runtime" / "purified-claims.jsonl"
    if not path.is_file():
        return 0
    try:
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


def _next_cron_fire(cron_expr: str, tz_name: str, now=None) -> datetime | None:
    """Minimal deterministic next-fire resolver for simple cron expressions
    of the shape `<minute> <hour> * * <dow_set>` (the only shape this
    package emits). `dow_set` is comma-separated cron day-of-week values
    (Sun=0, Mon=1, ... Sat=6). Returns a timezone-aware datetime or None
    if the expression can't be parsed.

    This is deliberately narrow — croniter is not available at runtime,
    and we only need to resolve the earliest fire across our own jobs.
    """
    if now is None:
        now = _tz_aware_now(tz_name or "Asia/Manila")
    parts = cron_expr.split()
    if len(parts) != 5:
        return None
    minute_field, hour_field, _, _, dow_field = parts
    try:
        target_min = int(minute_field)
        target_hour = int(hour_field)
    except ValueError:
        return None
    dows: set = set()
    for d in dow_field.split(","):
        try:
            dows.add(int(d))
        except ValueError:
            continue
    if not dows:
        dows = set(range(7))

    # Walk forward up to 8 days; cron patterns here repeat weekly at worst.
    for offset in range(0, 8):
        candidate = (now + timedelta(days=offset)).replace(
            hour=target_hour, minute=target_min, second=0, microsecond=0
        )
        cron_dow = (candidate.weekday() + 1) % 7
        if cron_dow not in dows:
            continue
        if candidate > now:
            return candidate
    return None


def _build_next_schedule(skill_root: Path | None = None, tz_name: str = "Asia/Manila") -> dict | None:
    """Resolve the earliest upcoming `memory-purifier-*` cron fire.

    Shells out to `openclaw cron list --json`, picks the earliest candidate
    by local time (anchored to the configured timezone), and labels its mode
    as `incremental` or `reconciliation` from the job name. Returns `None`
    when openclaw is unavailable, the listing is empty, or no job resolves
    to a future fire.
    """
    if shutil.which("openclaw") is None:
        return None
    try:
        proc = subprocess.run(
            ["openclaw", "cron", "list", "--json"],
            capture_output=True, text=True, check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        jobs = json.loads(proc.stdout)
    except Exception:
        return None
    if not isinstance(jobs, list):
        return None

    now = _tz_aware_now(tz_name)
    best: tuple[datetime, str, str] | None = None  # (when, name, mode)
    for job in jobs:
        if not isinstance(job, dict):
            continue
        name = str(job.get("name", ""))
        if not name.startswith("memory-purifier-"):
            continue
        cron_expr = str(job.get("cron") or job.get("schedule") or "").strip()
        if not cron_expr:
            continue
        when = _next_cron_fire(cron_expr, str(job.get("tz") or ""), now=now)
        if when is None:
            continue
        mode = "reconciliation" if "reconciliation" in name else "incremental"
        if best is None or when < best[0]:
            best = (when, name, mode)

    if best is None:
        return None
    when, name, mode = best
    return {
        "when": when.isoformat(),
        "name": name,
        "mode": mode,
    }


# v1.4.0: status weights for the weighted recall score. Contested claims
# are most actionable (they signal a real conflict the operator should
# resolve); unresolved next; retire_candidate last (the claim's sources
# are gone, so it's often informational).
_RECALL_STATUS_WEIGHT = {
    "contested": 3.0,
    "unresolved": 2.0,
    "retire_candidate": 1.0,
}


def _build_recall_surface(workspace: Path, tz_name: str = "Asia/Manila") -> dict | None:
    """Deterministic single-claim recall for the skip report.

    Pool: claims in `<workspace>/runtime/purified-claims.jsonl` with
    `status ∈ {contested, unresolved, retire_candidate}`.

    v1.4.0 ranking (weighted score — actionability beats pure age):
        score = (age_days × 0.1)
              + status_weight[status]          # contested=3, unresolved=2, retire_candidate=1
              + (min(len(provenance), 5) × 0.3)
              + (min(recurrence or 0, 10) × 0.2)

    Rationale: a freshly-contested claim with multi-source support is
    more actionable than a long-orphaned retire_candidate. Ties broken
    by oldest `updatedAt`, then `id` lexicographic. Returns a bounded
    dict (with `recallScore` exposed for debugging), or `None` when the
    pool is empty.
    """
    path = workspace / "runtime" / "purified-claims.jsonl"
    if not path.is_file():
        return None
    pool = []
    eligible = set(_RECALL_STATUS_WEIGHT.keys())
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    claim = json.loads(line)
                except Exception:
                    continue
                if claim.get("status") in eligible:
                    pool.append(claim)
    except Exception:
        return None
    if not pool:
        return None

    now = _tz_aware_now(tz_name)

    def _age_days(claim):
        updated_at = str(claim.get("updatedAt") or "")
        if not updated_at:
            return 0
        try:
            ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            return max(0, int((now.astimezone(ts.tzinfo) - ts).total_seconds() // 86400))
        except Exception:
            return 0

    def _score(claim):
        # Age contribution is capped at 14 days so a genuinely ancient
        # orphan can't drown out a recent contested claim via linear age
        # dominance. Status weight then becomes the primary ranker.
        age = min(_age_days(claim), 14)
        status_w = _RECALL_STATUS_WEIGHT.get(claim.get("status"), 0.0)
        prov_count = len(claim.get("provenance") or [])
        recurrence = int(claim.get("recurrence") or 0)
        return (
            age * 0.1
            + status_w
            + min(prov_count, 5) * 0.3
            + min(recurrence, 10) * 0.2
        )

    # Sort: primary by score desc; tiebreak by oldest updatedAt asc, then id asc.
    def _sort_key(c):
        return (-_score(c), str(c.get("updatedAt") or ""), str(c.get("id") or ""))

    winner = sorted(pool, key=_sort_key)[0]
    age_days = _age_days(winner)
    text = str(winner.get("text") or "")
    if len(text) > 240:
        text = text[:237] + "..."
    return {
        "claimId": winner.get("id"),
        "type": winner.get("type"),
        "status": winner.get("status"),
        "text": text,
        "primaryHome": winner.get("primaryHome"),
        "updatedAt": str(winner.get("updatedAt") or "") or None,
        "ageDays": age_days,
        "recallScore": round(_score(winner), 3),
    }


def _build_skip_enrichment(workspace: Path, tz_name: str = "Asia/Manila") -> dict:
    """Collect deterministic skip-path enrichment in one pass.

    Called only on skip states so the extra file I/O doesn't hit the
    normal run path. Returns `{claimsTotal, nextSchedule, recallSurface}`.
    """
    return {
        "claimsTotal": _count_claims_file(workspace),
        "nextSchedule": _build_next_schedule(tz_name=tz_name),
        "recallSurface": _build_recall_surface(workspace, tz_name=tz_name),
    }


def acquire_lock(locks_dir: Path, run_id: str, stale_hours: int, tz_name: str = "Asia/Manila") -> tuple:
    """Try to acquire the single-run lock.

    Returns (acquired: bool, lock_path: Path, existing_info: dict|None).

    A stale lock older than stale_hours is overwritten — this prevents a
    crashed run from blocking subsequent crons indefinitely. Running
    processes within the window cause us to skip cleanly (no crash, just
    exit with status=skipped).
    """
    locks_dir.mkdir(parents=True, exist_ok=True)
    # Prefix ensures the lock doesn't collide with other memory plugins writing
    # into the shared runtime/locks/ directory.
    lock_path = locks_dir / "purifier-run.lock"
    existing_info = None

    if lock_path.is_file():
        try:
            existing_info = json.loads(lock_path.read_text())
        except Exception:
            existing_info = {"corrupt": True}
        try:
            mtime = datetime.fromtimestamp(lock_path.stat().st_mtime, tz=timezone.utc)
            age_hours = (datetime.now(tz=timezone.utc) - mtime).total_seconds() / 3600.0
        except Exception:
            age_hours = 0.0
        existing_info["age_hours"] = round(age_hours, 3)
        if age_hours < stale_hours:
            return False, lock_path, existing_info

    ts = timestamp_triple(tz_name)
    payload = {
        "run_id": run_id,
        "pid": os.getpid(),
        "acquired_at": ts["timestamp"],
        "acquired_at_utc": ts["timestamp_utc"],
    }
    lock_path.write_text(json.dumps(payload, indent=2))
    return True, lock_path, existing_info


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


# v1.5.0 D1 (Contract 6) — which statuses reflect benign, no-failure outcomes?
# Cleanup runs in full only for these. Failure statuses preserve staging and
# pass-failure records for post-mortem.
_BENIGN_STATUSES = frozenset({"ok", "skipped", "skipped_superseded"})


def _apply_cleanup_policy(
    *,
    final_status: str,
    run_id: str,
    staging_dir: Path,
    lock_path: Path,
    locks_dir: Path,
    keep_staging: bool,
    dry_run: bool,
) -> None:
    """Apply the full Contract 6 cleanup matrix.

    Scope:
    - `run-<run_id>.lock`: released on EVERY exit path (including failures).
    - Staging dirs + temp files inside: cleaned on benign statuses, preserved
      on failure unless ``keep_staging`` or ``dry_run``.
    - Pass-failure records (``purifier-failed-*-{run_id}.json``): removed on
      ``ok``, preserved on every other status for forensics.
    - ``upgrade_required`` is handled before staging is even created — the
      caller should never reach this helper under that status.
    """
    # Always release the run-active lock — never leaks.
    release_lock(lock_path)

    # Staging + temps cleanup on benign statuses only.
    if final_status in _BENIGN_STATUSES and not keep_staging and not dry_run:
        shutil.rmtree(staging_dir, ignore_errors=True)

    # Clean pass-failure records on a fully clean run. Keep them on any
    # non-ok outcome so operators can inspect what the failed pass returned.
    if final_status == "ok" and not dry_run:
        try:
            for p in locks_dir.glob(f"purifier-failed-*-{run_id}.json"):
                try:
                    p.unlink()
                except OSError:
                    pass
        except OSError:
            pass


# ── v1.5.0 C1 config defaulted-field detection (Contract 3) ───────────

# Optional config fields whose absence should surface as an explicit
# ``config_defaulted`` warning rather than silently picking up defaults.
# Each entry is ``(path, default, reason)`` where ``path`` is a tuple of
# successive dict keys. Additive — Phase 5+ will append widening/batching
# fields as they land.
_OPTIONAL_CONFIG_FIELDS = (
    (("cron", "timeout_seconds"), 1200, "cron timeout falls back to 1200s"),
)


def _detect_config_defaulted(config: dict) -> list:
    """Return one ``config_defaulted`` warning per missing optional field.

    Walks the dotted path in ``_OPTIONAL_CONFIG_FIELDS``; a missing leaf,
    intermediate ``None``, or wrong type produces a warning. Used by the
    orchestrator; output threads into ``manifest.warnings[]`` via the
    ``--warnings`` CLI arg on ``write_manifest.py``.
    """
    warnings: list = []
    if not isinstance(config, dict):
        return warnings
    for path, default, reason in _OPTIONAL_CONFIG_FIELDS:
        node = config
        missing = False
        for key in path:
            if not isinstance(node, dict) or key not in node or node.get(key) is None:
                missing = True
                break
            node = node[key]
        if missing:
            warnings.append({
                "code": "config_defaulted",
                "field": ".".join(path),
                "default": default,
                "reason": reason,
            })
    # v1.6.0 backend-migration nudge: if the operator still has the
    # pre-v1.6.0 default (`claude-code`) on disk, surface a non-blocking
    # deprecation notice. The preflight fails loudly if the binary is
    # missing; this warning is for operators whose claude CLI still works
    # so they know the package default moved to `openclaw`.
    prompts_cfg = config.get("prompts")
    if isinstance(prompts_cfg, dict):
        backend = prompts_cfg.get("backend")
        if backend == "claude-code":
            warnings.append({
                "code": "backend_deprecated_default",
                "from": "claude-code",
                "to": "openclaw",
                "reason": (
                    "v1.6.0 changed the default backend to `openclaw`. "
                    "The legacy `claude-code` backend still works when the "
                    "`claude` CLI is installed, but the OpenClaw path is "
                    "the intended default for this deployment. Update "
                    "`prompts.backend` in memory-purifier.json at your leisure."
                ),
            })
    return warnings


# ── v1.5.0 C1 upgrade state machine (Contract 3) ───────────────────────

def _read_stored_versions(runtime_dir: Path) -> dict:
    """Read the four load-bearing version fields from the persisted manifest.

    Returns a dict with keys ``logicVersion``, ``manifestSchemaVersion``,
    ``artifactSchemaVersion``, ``lastSuccessfulLogicVersion``. Missing fields
    map to ``None`` — the state machine treats ``None`` as "no prior state"
    (fresh install).
    """
    manifest_path = runtime_dir / "purified-manifest.json"
    if not manifest_path.is_file():
        return {}
    try:
        data = json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        "logicVersion": data.get("logicVersion"),
        "manifestSchemaVersion": data.get("manifestSchemaVersion"),
        "artifactSchemaVersion": data.get("artifactSchemaVersion"),
        "lastSuccessfulLogicVersion": data.get("lastSuccessfulLogicVersion"),
    }


def _detect_upgrade(stored: dict) -> tuple[bool, str | None]:
    """Return ``(required, reason)`` per Contract 3.

    Never fires on first-run (stored dict empty), on exact match, or on
    downgrade (running < stored — operator's concern). Only triggers when
    at least one stored identifier is strictly less than its running
    counterpart.
    """
    stored_logic = stored.get("logicVersion")
    stored_ms = stored.get("manifestSchemaVersion")
    stored_as = stored.get("artifactSchemaVersion")
    if stored_logic is None and stored_ms is None and stored_as is None:
        return False, None
    if stored_logic and version_tuple(stored_logic) < version_tuple(PURIFIER_LOGIC_VERSION):
        return True, "logic_version_mismatch"
    if stored_ms and version_tuple(stored_ms) < version_tuple(PURIFIER_MANIFEST_SCHEMA):
        return True, "manifest_schema_mismatch"
    if stored_as and version_tuple(stored_as) < version_tuple(PURIFIER_ARTIFACT_SCHEMA):
        return True, "artifact_schema_mismatch"
    return False, None


def _is_downgrade(stored: dict) -> bool:
    """Operator is running OLDER code than last successful run. Warn, proceed."""
    stored_logic = stored.get("logicVersion")
    if not stored_logic:
        return False
    return version_tuple(stored_logic) > version_tuple(PURIFIER_LOGIC_VERSION)


def _upgrade_lock_path(locks_dir: Path, from_version: str, to_version: str) -> Path:
    return locks_dir / f"purifier-upgrade-pending-{from_version}-{to_version}.json"


def _write_upgrade_pending_lock(
    locks_dir: Path,
    from_version: str,
    to_version: str,
    reason: str,
    tz_name: str,
) -> Path:
    """Write the refuse-and-lock marker operators read to unblock the run."""
    locks_dir.mkdir(parents=True, exist_ok=True)
    ts = timestamp_triple(tz_name)
    lock_path = _upgrade_lock_path(locks_dir, from_version, to_version)
    payload = {
        "from": from_version,
        "to": to_version,
        "reason": reason,
        "detected_at": ts["timestamp"],
        "detected_at_utc": ts["timestamp_utc"],
        "instructions": (
            "Run once manually: python3 scripts/run_purifier.py --acknowledge-upgrade. "
            "That will force a reconciliation pass under the new logic and clear this lock."
        ),
    }
    lock_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    return lock_path


def _clear_upgrade_pending_lock(locks_dir: Path) -> None:
    """Remove any outstanding upgrade-pending lock files.

    The filename pattern is ``purifier-upgrade-pending-<from>-<to>.json`` so
    this uses a glob to catch stale markers from earlier attempts as well.
    """
    try:
        for p in locks_dir.glob("purifier-upgrade-pending-*.json"):
            try:
                p.unlink()
            except OSError:
                pass
    except OSError:
        pass


def _print_upgrade_instructions(
    stored_logic: str, running_logic: str, reason: str, lock_path: Path
) -> None:
    """Operator-facing stderr block — never consumed by JSON parsers."""
    banner = (
        f"[purifier] UPGRADE REQUIRED: stored {reason} "
        f"({stored_logic} < {running_logic}); refusing to run.\n"
        f"[purifier] Artifact state under v{stored_logic} cannot safely mix "
        f"with v{running_logic} logic.\n"
        "[purifier] To proceed, run once manually:\n"
        "[purifier]   python3 scripts/run_purifier.py --acknowledge-upgrade\n"
        f"[purifier] That will force a reconciliation run under v{running_logic} "
        "semantics and clear the lock.\n"
        f"[purifier] Upgrade-pending lock: {lock_path}\n"
    )
    sys.stderr.write(banner)
    sys.stderr.flush()


def _build_upgrade_required_report(
    *,
    run_id: str,
    mode: str,
    profile: str,
    stored: dict,
    reason: str,
    lock_path: Path,
    started_ts: dict,
    tz_name: str,
) -> dict:
    """Emit the structured blocked-run report. No staging was ever created."""
    stored_logic = stored.get("logicVersion")
    return {
        "ok": False,
        "status": "upgrade_required",
        "mode": mode,
        "profile": profile,
        "runId": run_id,
        "upgradeRequired": True,
        "upgradeReason": reason,
        "upgradeBlockedAt": started_ts["timestamp"],
        "upgradeBlockedAt_utc": started_ts["timestamp_utc"],
        "requiresForcedReconciliation": True,
        "storedLogicVersion": stored_logic,
        "storedManifestSchemaVersion": stored.get("manifestSchemaVersion"),
        "storedArtifactSchemaVersion": stored.get("artifactSchemaVersion"),
        "runningLogicVersion": PURIFIER_LOGIC_VERSION,
        "runningManifestSchemaVersion": PURIFIER_MANIFEST_SCHEMA,
        "runningArtifactSchemaVersion": PURIFIER_ARTIFACT_SCHEMA,
        "upgradePendingLockPath": str(lock_path),
        "instructions": (
            "python3 scripts/run_purifier.py --acknowledge-upgrade"
        ),
        **timestamp_triple(tz_name),
    }


# ── v1.5.0 A2 transactional commit (Contract 2) ───────────────────────

# Ordered list of artifacts whose promotion order is load-bearing per
# Contract 2: JSONL first (machine artifacts consumed by downstream wiki),
# markdown views second (derived, consumed by human + other skills),
# manifest LAST as the single-file commit marker. If promote dies
# mid-sequence, readers at worst see stale views alongside fresh JSONL —
# never the reverse.
_JSONL_ARTIFACTS = (
    "purified-claims.jsonl",
    "purified-contradictions.jsonl",
    "purified-entities.json",
    "purified-routes.json",
)
_MARKDOWN_VIEWS = (
    "LTMEMORY.md",
    "PLAYBOOKS.md",
    "EPISODES.md",
    "HISTORY.md",  # personal-only, may not exist in staging
    "WISHES.md",   # personal-only, may not exist in staging
)
_MANIFEST_FILENAME = "purified-manifest.json"


def _promote_staged_outputs(
    staged_publish_dir: Path,
    runtime_dir: Path,
    workspace_dir: Path,
) -> tuple[list, list]:
    """Promote staged JSONL → runtime and views → workspace. Manifest stays put.

    Returns ``(promoted_artifacts, promoted_views)`` — paths (as strings) of
    every file that landed. The manifest is promoted SEPARATELY by the
    orchestrator AFTER this returns, because the commit-time patch
    (``_patch_staged_manifest_for_commit``) has to see the list of
    promoted files to populate ``publishedArtifactSet``/``publishedViewSet``.

    Uses ``os.replace`` per file. Atomic per-file, not across files —
    crash-recovery mid-sequence is out of scope per Contract 2 §deferred.
    The load-bearing guarantee is "no promotion begins until validation passes".
    """
    promoted_artifacts: list = []
    promoted_views: list = []

    # JSONL artifacts first
    for name in _JSONL_ARTIFACTS:
        src = staged_publish_dir / name
        if not src.is_file():
            continue
        dst = runtime_dir / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dst)
        promoted_artifacts.append(str(dst))

    # Markdown views second
    for name in _MARKDOWN_VIEWS:
        src = staged_publish_dir / name
        if not src.is_file():
            continue
        dst = workspace_dir / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dst)
        promoted_views.append(str(dst))

    return promoted_artifacts, promoted_views


def _patch_staged_manifest_for_commit(
    staged_manifest_path: Path,
    run_id: str,
    promoted_artifacts: list,
    promoted_views: list,
    downstream_signal_will_fire: bool,
    tz_name: str,
) -> None:
    """Flip the publish-contract fields to committed state BEFORE promoting the manifest.

    This keeps the manifest's on-disk state honest: once promoted it will
    claim ``publishCommitted=True`` truthfully, because the JSONL + views
    are already on their final paths. Writes are atomic via a tmp-sibling.
    """
    if not staged_manifest_path.is_file():
        return
    try:
        payload = json.loads(staged_manifest_path.read_text())
    except (OSError, ValueError):
        return
    ts = timestamp_triple(tz_name)
    payload["publishCommitted"] = True
    payload["publishedAt"] = ts["timestamp"]
    payload["publishedArtifactSet"] = promoted_artifacts
    payload["publishedViewSet"] = promoted_views
    payload["downstreamWikiSignalEmitted"] = bool(downstream_signal_will_fire)
    payload["commitRunId"] = run_id
    atomic_write_json(staged_manifest_path, payload)


def _advance_config_cursor(
    config_path: Path, mode: str, cursor_new, finished_ts_local: str
) -> bool:
    """Merge-update ``memory-purifier.json.lastRun`` after successful promote.

    Only runs when the full publish sequence succeeded — the cursor never
    advances for partial_failure or failed runs. Returns True when an
    update was written.
    """
    if not config_path.is_file():
        return False
    try:
        cfg = json.loads(config_path.read_text())
    except (OSError, ValueError):
        return False
    if not isinstance(cfg, dict):
        return False
    last_run = cfg.get("lastRun") or {}
    if not isinstance(last_run, dict):
        last_run = {}
    last_run[mode] = finished_ts_local
    if cursor_new is not None:
        last_run["cursor"] = cursor_new
    cfg["lastRun"] = last_run
    atomic_write_json(config_path, cfg)
    return True


def _run_script(script_name: str, argv: list, name: str) -> dict:
    """Invoke a sub-script with argv; parse one JSON object from stdout.

    On stdout parse failure (non-JSON, empty, garbage), return a synthetic
    error dict so the orchestrator can halt cleanly without re-raising.
    """
    # v1.5.0 Contract 1: internal subprocess-wrapper status strings align
    # with the locked top-level taxonomy. Script-invocation failures map
    # to `failed` (orchestrator surfaces the `reason` field upstream).
    full = [sys.executable, str(SCRIPT_DIR / script_name)] + argv
    proc = subprocess.run(full, capture_output=True, text=True)
    if proc.returncode != 0:
        return {
            "status": "failed",
            "reason": f"{name} exit code {proc.returncode}",
            "stderr_head": (proc.stderr or "")[:1000],
            "stdout_head": (proc.stdout or "")[:1000],
        }
    if not (proc.stdout or "").strip():
        return {
            "status": "failed",
            "reason": f"{name} produced empty stdout",
            "stderr_head": (proc.stderr or "")[:1000],
        }
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        return {
            "status": "failed",
            "reason": f"{name} stdout not JSON: {e.msg}",
            "stdout_head": proc.stdout[:1000],
            "stderr_head": (proc.stderr or "")[:500],
        }


def _write_staging(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="Orchestrate a memory-purifier run.")
    ap.add_argument("--mode", required=True, choices=["incremental", "reconciliation"])
    ap.add_argument("--workspace", help="Workspace root (default: $WORKSPACE or ~/.openclaw/workspace)")
    ap.add_argument("--profile", choices=["business", "personal"], help="Profile override")
    ap.add_argument("--config", help=f"Path to memory-purifier.json (default: {DEFAULT_CONFIG})")
    ap.add_argument("--runtime-dir", help="Runtime dir override")
    ap.add_argument("--telemetry-root", help="Package telemetry root (default: ~/.openclaw/telemetry/memory-purifier). Holds last-run.md directly (flat).")
    ap.add_argument("--global-log-root", help="Shared memory-log root (default: ~/.openclaw/telemetry). Memory-log JSONL appended here.")
    ap.add_argument(
        "--backend",
        help="Model backend for scoring passes: openclaw (default, v1.6.0+) | "
             "claude-code | anthropic-sdk | file. When omitted, resolves via "
             "$MEMORY_PURIFIER_BACKEND → prompts.backend in memory-purifier.json → "
             "_lib.backend.DEFAULT_BACKEND (`openclaw`).",
    )
    ap.add_argument("--fixture-dir", help="Fixture directory (backend=file)")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument("--stale-lock-hours", type=int, default=STALE_LOCK_HOURS_DEFAULT, help="Overwrite a lock older than this (default: 2h)")
    ap.add_argument(
        "--keep-staging",
        action="store_true",
        help="Preserve the staging dir even on ok (debug forensics). "
             "PURIFIER_DEBUG_RETAIN=1 env var has the same effect.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Chain runs but no files persist (artifacts, cursor, config)")
    ap.add_argument("--run-id", help="Explicit run_id override (default: generated UUID). Useful for deterministic fixture-based testing.")
    ap.add_argument("--force", action="store_true", help="Override the reconciliation-window guard. Normally an incremental run inside a reconciliation slot skips with status=skipped_superseded.")
    ap.add_argument(
        "--acknowledge-upgrade",
        action="store_true",
        help="Acknowledge a detected version upgrade (v1.5.0 C1). Forces mode=reconciliation "
             "for this run, rebuilds state under the running logic, and clears the upgrade-pending lock.",
    )

    args = ap.parse_args()

    run_id = args.run_id or str(uuid.uuid4())
    config_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG
    profile = resolve_profile(args.profile, config_path)
    tz_name = resolve_timezone(args.timezone, config_path)
    started_ts = timestamp_triple(tz_name)

    # Workspace resolution ladder: CLI → config.paths.workspace → $WORKSPACE → default.
    config_snapshot = _load_json_safely(config_path) if config_path.is_file() else {}
    cfg_workspace = (config_snapshot.get("paths") or {}).get("workspace")
    workspace_hint = (
        args.workspace
        or (cfg_workspace if isinstance(cfg_workspace, str) and cfg_workspace else None)
        or os.environ.get("WORKSPACE")
    )
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    # Flat runtime layout: purifier files live directly under <workspace>/runtime/.
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")
    telemetry_root = Path(args.telemetry_root).expanduser() if args.telemetry_root else (Path.home() / ".openclaw" / "telemetry" / "memory-purifier")
    # Shared memory-log root is the parent of the package telemetry root by convention.
    global_log_root = Path(args.global_log_root).expanduser() if args.global_log_root else telemetry_root.parent

    locks_dir = runtime_dir / "locks"
    # Staging is namespaced to this package since runtime_dir is now shared.
    staging_dir = runtime_dir / ".staging-purifier" / run_id
    # v1.5.0 A2 (Contract 2): artifacts + views + the manifest stage under
    # publish/ before the orchestrator atomically promotes them. Validation
    # runs against this dir; a failed run leaves the prior-committed state
    # (if any) on disk untouched.
    staging_publish_dir = staging_dir / "publish"

    # v1.5.0 C1 (Contract 3) — upgrade state machine.
    # Detects mismatch between stored runtime state and the running code's
    # logic/schema versions. On mismatch: refuse-and-lock unless
    # --acknowledge-upgrade is set. Runs BEFORE lock acquisition and staging
    # creation so a blocked run produces no state residue.
    stored_versions = _read_stored_versions(runtime_dir)
    upgrade_required, upgrade_reason = _detect_upgrade(stored_versions)
    upgrade_acknowledged = False
    _downgrade_warning = None
    if _is_downgrade(stored_versions):
        _downgrade_warning = {
            "code": "downgrade_detected",
            "stored": stored_versions.get("logicVersion"),
            "running": PURIFIER_LOGIC_VERSION,
        }
    if upgrade_required:
        if args.acknowledge_upgrade:
            # Force reconciliation + bypass the window check. The acknowledged
            # upgrade becomes a manifest warning on successful finalize.
            args.mode = "reconciliation"
            args.force = True
            upgrade_acknowledged = True
            _clear_upgrade_pending_lock(locks_dir)
        else:
            upgrade_lock_path = _write_upgrade_pending_lock(
                locks_dir,
                from_version=stored_versions.get("logicVersion") or "unknown",
                to_version=PURIFIER_LOGIC_VERSION,
                reason=upgrade_reason or "version_mismatch",
                tz_name=tz_name,
            )
            _print_upgrade_instructions(
                stored_logic=stored_versions.get("logicVersion") or "unknown",
                running_logic=PURIFIER_LOGIC_VERSION,
                reason=upgrade_reason or "version_mismatch",
                lock_path=upgrade_lock_path,
            )
            blocked = _build_upgrade_required_report(
                run_id=run_id,
                mode=args.mode,
                profile=profile,
                stored=stored_versions,
                reason=upgrade_reason or "version_mismatch",
                lock_path=upgrade_lock_path,
                started_ts=started_ts,
                tz_name=tz_name,
            )
            print(json.dumps(blocked, indent=2, ensure_ascii=False))
            return 2

    # Runtime reconciliation-over-incremental supersession:
    # even if cron has drifted or been misregistered, never let an incremental
    # run inside a reconciliation window.
    if args.mode == "incremental" and not args.force:
        in_window, expr = _is_reconciliation_window(config_snapshot, tz_name)
        if in_window:
            manifest_path = runtime_dir / "purified-manifest.json"
            summary_path = runtime_dir / "purifier-last-run-summary.json"
            skip_enrichment = _build_skip_enrichment(workspace, tz_name)
            out = _build_final_report(
                status="skipped_superseded",
                ok=True,
                run_id=run_id,
                mode=args.mode,
                profile=profile,
                skip_reason=f"superseded_by_reconciliation_window (matching cadence expression: {expr})",
                manifest_path=manifest_path,
                summary_path=summary_path,
                started_ts=started_ts,
                dry_run=args.dry_run,
                skip_enrichment=skip_enrichment,
            )
            print(json.dumps(out, indent=2, ensure_ascii=False))
            return 0

    acquired, lock_path, existing = acquire_lock(locks_dir, run_id, args.stale_lock_hours, tz_name)
    if not acquired:
        manifest_path = runtime_dir / "purified-manifest.json"
        summary_path = runtime_dir / "purifier-last-run-summary.json"
        skip_enrichment = _build_skip_enrichment(workspace, tz_name)
        out = _build_final_report(
            status="skipped",
            ok=True,
            run_id=run_id,
            mode=args.mode,
            profile=profile,
            skip_reason="another run appears active",
            manifest_path=manifest_path,
            summary_path=summary_path,
            started_ts=started_ts,
            dry_run=args.dry_run,
            extra={"existing_lock": existing, "stale_lock_hours": args.stale_lock_hours},
            skip_enrichment=skip_enrichment,
        )
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    # Lock held — append run_started to the shared memory log so the timeline
    # has both edges (started → completed/failed/skipped) regardless of what
    # happens next in the pipeline. Skipped-before-lock paths (supersession
    # guard, lock-held) deliberately do NOT emit run_started.
    if not args.dry_run:
        try:
            append_memory_log_event(
                global_log_root=global_log_root,
                event="run_started",
                run_id=run_id,
                status="started",
                mode=args.mode,
                profile=profile,
                agent=_resolve_agent_id(),
                # At start, actual counts are unknown (not zero). Use nulls so
                # downstream queries can distinguish "pre-run" from "real run
                # that consumed zero tokens".
                token_usage={
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "source": "unavailable",
                },
                details={
                    "config_path": str(config_path),
                    "workspace": str(workspace),
                    "backend": args.backend,
                },
                tz_name=tz_name,
            )
        except Exception:
            pass

    def finalize(overall_status: str, halt_reason: str = None) -> int:
        """Write manifest, validate, promote staged outputs, release lock.

        v1.5.0 A2 (Contract 2) transactional flow:
          1. write_manifest runs with --output-dir staging_publish when the
             run produced new output and overall_status == "ok" — otherwise
             manifest lands directly in runtime for diagnostic continuity.
          2. validate_outputs runs against the staged set.
          3. On validate-ok: atomically promote JSONL → views → manifest
             (manifest LAST = commit marker), advance config cursor, fire
             trigger_wiki.
          4. On validate-fail or non-ok status: copy staged manifest into
             runtime for diagnostic, skip promote, skip trigger_wiki.
        """
        # Does this path have staged artifacts that could be promoted?
        staged_has_artifacts = (staging_publish_dir / "purified-claims.jsonl").is_file()
        attempting_publish = (overall_status == "ok") and staged_has_artifacts

        wm_argv = [
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--telemetry-root", str(telemetry_root),
            "--config", str(config_path),
            "--mode", args.mode,
            "--profile", profile,
            "--run-id", run_id,
            "--timezone", tz_name,
            "--status", overall_status,
        ]
        if attempting_publish:
            wm_argv.extend(["--output-dir", str(staging_publish_dir)])
        for name, path in (
            ("--inventory", staging_dir / "inventory.json"),
            ("--scope", staging_dir / "scope.json"),
            ("--pass1", staging_dir / "pass1.json"),
            ("--pass2", staging_dir / "pass2.json"),
            ("--assemble", staging_dir / "assemble.json"),
        ):
            if path.is_file():
                wm_argv.extend([name, str(path)])
        if render_result and isinstance(render_result, dict):
            views = [v["path"] for v in (render_result.get("views_rendered") or []) if v.get("written")]
            if views:
                wm_argv.extend(["--views-rendered", json.dumps(views)])
        # v1.5.0 C1 — propagate orchestrator-level warnings into manifest.
        extra_warnings = []
        if upgrade_acknowledged:
            extra_warnings.append({
                "code": "upgrade_acknowledged",
                "from": stored_versions.get("logicVersion"),
                "to": PURIFIER_LOGIC_VERSION,
                "reason": upgrade_reason or "version_mismatch",
            })
        if _downgrade_warning:
            extra_warnings.append(_downgrade_warning)
        # config_defaulted: missing optional config fields surface rather than silently default.
        extra_warnings.extend(_detect_config_defaulted(config_snapshot))
        if extra_warnings:
            wm_argv.extend(["--warnings", json.dumps(extra_warnings)])
        if args.dry_run:
            wm_argv.append("--dry-run")

        wm = _run_script("write_manifest.py", wm_argv, "write_manifest")
        _write_staging(staging_dir / "manifest.json", wm)

        # Validate against staged outputs when attempting publish; otherwise
        # validate the runtime state (legacy path for skip / error flows).
        val_argv = [
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--profile", profile,
            "--config", str(config_path),
            "--timezone", tz_name,
        ]
        if attempting_publish:
            val_argv.extend(["--target-dir", str(staging_publish_dir)])
        val = _run_script("validate_outputs.py", val_argv, "validate_outputs")
        _write_staging(staging_dir / "validate.json", val)

        validate_failed = (val or {}).get("status") == "errors"
        runtime_manifest_path = runtime_dir / "purified-manifest.json"
        staged_manifest_path = staging_publish_dir / "purified-manifest.json"

        # Decide final_status, possibly promote, always copy staged manifest
        # to runtime for diagnostic continuity (so skip-state consumers still
        # see the most recent manifest on disk).
        promoted_artifacts: list = []
        promoted_views: list = []
        publish_committed = False

        if attempting_publish and not validate_failed and not args.dry_run:
            # Determine whether wiki signal is going to fire so the manifest
            # can record it truthfully before the commit.
            downstream_will_fire = False
            try:
                staged_payload = json.loads(staged_manifest_path.read_text())
                downstream_will_fire = bool(staged_payload.get("downstreamWikiIngestSuggested"))
            except (OSError, ValueError):
                downstream_will_fire = False
            promoted_artifacts, promoted_views = _promote_staged_outputs(
                staging_publish_dir, runtime_dir, workspace,
            )
            # Patch staged manifest BEFORE the final os.replace so the on-disk
            # committed manifest carries publishCommitted=true truthfully.
            _patch_staged_manifest_for_commit(
                staged_manifest_path,
                run_id=run_id,
                promoted_artifacts=promoted_artifacts,
                promoted_views=promoted_views,
                downstream_signal_will_fire=downstream_will_fire,
                tz_name=tz_name,
            )
            # Manifest LAST — the commit marker.
            if staged_manifest_path.is_file():
                os.replace(staged_manifest_path, runtime_manifest_path)
            publish_committed = True
            # Advance the config cursor only after successful promote.
            cursor_new = (scope or {}).get("cursor_new") if scope else None
            _advance_config_cursor(config_path, args.mode, cursor_new, timestamp_triple(tz_name)["timestamp"])
            final_status = "ok"
        elif attempting_publish and validate_failed:
            # Staged set failed validation — roll back to prior state.
            # Patch the staged manifest to reflect the true status/publish
            # state before copying it to runtime as the diagnostic record.
            if not args.dry_run and staged_manifest_path.is_file():
                try:
                    staged_payload = json.loads(staged_manifest_path.read_text())
                    staged_payload["status"] = "partial_failure"
                    staged_payload["publishCommitted"] = False
                    staged_payload["downstreamWikiIngestSuggested"] = False
                    staged_payload["downstreamWikiSignalEmitted"] = False
                    partials = list(staged_payload.get("partialFailures") or [])
                    partials.append({
                        "code": "transactional_commit_failed",
                        "pass": "validate",
                        "error_count": (val or {}).get("error_count"),
                        "reason": "validate_outputs reported errors on staged set — publish suppressed",
                    })
                    staged_payload["partialFailures"] = partials
                    atomic_write_json(staged_manifest_path, staged_payload)
                    shutil.copy(staged_manifest_path, runtime_manifest_path)
                except (OSError, ValueError):
                    pass
            final_status = "partial_failure"
        else:
            # Non-publish paths (skipped, partial_failure upstream, error).
            # If a staged manifest exists, copy to runtime for diagnostic.
            if not args.dry_run and staged_manifest_path.is_file():
                try:
                    shutil.copy(staged_manifest_path, runtime_manifest_path)
                except OSError:
                    pass
            final_status = overall_status
            if final_status == "ok" and validate_failed:
                # Edge case: non-staged publish path reported validate errors.
                # v1.5.0: fold into partial_failure per Contract 1 locked
                # taxonomy; the specific cause lives in partialFailures[].code.
                final_status = "partial_failure"

        # Aggregate scoring-pass-only token usage across Pass 1 + Pass 2.
        run_usage = _usage_unavailable()
        if pass1 and isinstance(pass1, dict):
            run_usage = _merge_usage(run_usage, pass1.get("token_usage") or _usage_unavailable())
        if pass2 and isinstance(pass2, dict):
            run_usage = _merge_usage(run_usage, pass2.get("token_usage") or _usage_unavailable())

        # Patch the manifest on-disk with final token_usage + latest paths so
        # last-run-summary.json reflects the validated final state.
        summary_path = runtime_dir / "purifier-last-run-summary.json"
        global_log_path = global_log_root / f"memory-log-{local_date_str(tz_name)}.jsonl"
        latest_report_path = telemetry_root / "last-run.md"

        manifest_after_gate = {}
        if runtime_manifest_path.is_file():
            try:
                manifest_after_gate = json.loads(runtime_manifest_path.read_text())
            except Exception:
                manifest_after_gate = {}

        # Compute views rendered (for both the summary mirror and the markdown report)
        views_rendered = []
        if render_result and isinstance(render_result, dict):
            views_rendered = [v.get("path") for v in (render_result.get("views_rendered") or []) if v.get("written")]

        # Duration for last-run.md (best-effort)
        duration_seconds = None
        finish_ts = timestamp_triple(tz_name)
        try:
            started_dt = datetime.fromisoformat(started_ts["timestamp"])
            finished_dt = datetime.fromisoformat(finish_ts["timestamp"])
            duration_seconds = (finished_dt - started_dt).total_seconds()
        except Exception:
            duration_seconds = None

        claims_new = (assemble or {}).get("claim_count_new", 0) or 0
        claims_total = (assemble or {}).get("claim_count_total", 0) or 0
        contradiction_count = (pass2 or {}).get("contradiction_count", 0) or 0
        supersession_count = (pass2 or {}).get("supersession_count", 0) or 0
        warnings_on_disk = manifest_after_gate.get("warnings") or []
        partials_on_disk = manifest_after_gate.get("partialFailures") or []
        # v1.5.0 A2: the downstream signal fires only if publish actually
        # committed. A staged+validation-failed run has ingestSuggested=true
        # in its diagnostic manifest copy but never promoted, so the real
        # state is "suggested but not emitted" — treat as false.
        downstream_final = publish_committed and bool(
            manifest_after_gate.get("downstreamWikiIngestSuggested")
        )

        # v1.4.0: precompute skip enrichment once so summary, last-run.md,
        # and the final JSON all share the same state. Cheap (bounded file
        # scan); only runs on skip paths.
        finalize_skip_enrichment = None
        if final_status in {"skipped", "skipped_superseded"}:
            finalize_skip_enrichment = _build_skip_enrichment(workspace, tz_name)

        # Patch last-run-summary.json so it mirrors the final JSON shape emitted below.
        # (write_manifest.py wrote an initial version; we now append tokenUsage + paths.)
        if summary_path.is_file() and not args.dry_run:
            try:
                summary_current = json.loads(summary_path.read_text())
                summary_current["ok"] = final_status in {"ok", "skipped", "skipped_superseded"}
                summary_current["status"] = final_status
                summary_current["tokenUsage"] = run_usage
                summary_current["globalMemoryLogPath"] = str(global_log_path)
                summary_current["latestReportPath"] = str(latest_report_path)
                summary_current["downstreamWikiIngestSuggested"] = downstream_final
                # v1.4.0: mirror skip enrichment fields into the summary
                # so operators reading purifier-last-run-summary.json see
                # the same fields the cron-prompt final JSON carries.
                if finalize_skip_enrichment is not None:
                    summary_current["claimsTotal"] = finalize_skip_enrichment.get("claimsTotal", summary_current.get("claimsTotal", 0))
                    summary_current["nextSchedule"] = finalize_skip_enrichment.get("nextSchedule")
                    summary_current["recallSurface"] = finalize_skip_enrichment.get("recallSurface")
                atomic_write_json(summary_path, summary_current)
            except Exception:
                pass

        # Write <telemetry-root>/last-run.md from final deterministic state.
        if not args.dry_run:
            try:
                write_latest_report(
                    telemetry_root=telemetry_root,
                    run_id=run_id,
                    status=final_status,
                    ok=(final_status in {"ok", "skipped", "skipped_superseded"}),
                    mode=args.mode,
                    profile=profile,
                    started_at=started_ts["timestamp"],
                    finished_at=finish_ts["timestamp"],
                    duration_seconds=duration_seconds,
                    claims_new=claims_new,
                    claims_total=claims_total,
                    contradiction_count=contradiction_count,
                    supersession_count=supersession_count,
                    views_rendered=views_rendered,
                    warning_count=len(warnings_on_disk),
                    partial_failure_count=len(partials_on_disk),
                    downstream_wiki_ingest_suggested=downstream_final,
                    token_usage=run_usage,
                    manifest_path=runtime_manifest_path,
                    tz_name=tz_name,
                    halt_reason=halt_reason,
                    skip_enrichment=finalize_skip_enrichment,
                )
            except Exception:
                pass

        # Append the run_completed / run_failed / run_skipped event to the shared memory log.
        if not args.dry_run:
            try:
                telemetry_event = "run_completed"
                if final_status in {"skipped", "skipped_superseded"}:
                    telemetry_event = "run_skipped"
                elif final_status in {"partial_failure", "failed"}:
                    telemetry_event = "run_failed"
                append_memory_log_event(
                    global_log_root=global_log_root,
                    event=telemetry_event,
                    run_id=run_id,
                    status=final_status,
                    mode=args.mode,
                    profile=profile,
                    agent=_resolve_agent_id(),
                    token_usage=run_usage,
                    details={
                        "claims_new": claims_new,
                        "claims_total": claims_total,
                        "contradiction_count": contradiction_count,
                        "supersession_count": supersession_count,
                        "rendered_views": views_rendered,
                        "warnings_count": len(warnings_on_disk),
                        "partial_failures_count": len(partials_on_disk),
                        "downstream_wiki_ingest_suggested": downstream_final,
                    },
                    tz_name=tz_name,
                )
            except Exception:
                pass

        # v1.5.0 A2 (Contract 2): trigger_wiki fires only after successful
        # commit — gated by ``publish_committed`` (the orchestrator's truth)
        # AND the manifest's ``publishCommitted`` (what trigger_wiki reads).
        # If the staged set failed validation, ``publish_committed`` is
        # false and this branch is skipped entirely.
        trig: dict = {"status": "skipped_publish_not_committed"}
        if publish_committed:
            trig_argv = [
                "--workspace", str(workspace),
                "--runtime-dir", str(runtime_dir),
                "--config", str(config_path),
                "--timezone", tz_name,
            ]
            if args.dry_run:
                trig_argv.append("--dry-run")
            trig = _run_script("trigger_wiki.py", trig_argv, "trigger_wiki")
        _write_staging(staging_dir / "trigger.json", trig)

        # v1.5.0 D1 (Contract 6) — apply full cleanup matrix.
        # `run-<run_id>.lock` is released on EVERY exit path. Staging dirs
        # and temp files are cleaned on benign statuses (ok, skipped,
        # skipped_superseded) and preserved on failure paths
        # (partial_failure, failed) for post-mortem. `upgrade_required`
        # is handled elsewhere — it never reaches this code path because
        # staging is not created on a blocked run.
        _apply_cleanup_policy(
            final_status=final_status,
            run_id=run_id,
            staging_dir=staging_dir,
            lock_path=lock_path,
            locks_dir=locks_dir,
            keep_staging=bool(args.keep_staging) or os.environ.get("PURIFIER_DEBUG_RETAIN") == "1",
            dry_run=args.dry_run,
        )

        # Benign skips are still "ok" from the cron prompt's perspective —
        # the run didn't fail, there was just nothing to do. Skip enrichment
        # was precomputed earlier in this scope and reused here so the
        # summary file, last-run.md, and this final JSON share identical fields.
        benign_statuses = {"ok", "skipped", "skipped_superseded"}
        out = _build_final_report(
            status=final_status,
            ok=(final_status in benign_statuses),
            run_id=run_id,
            mode=args.mode,
            profile=profile,
            halt_reason=halt_reason,
            manifest_path=runtime_manifest_path,
            summary_path=summary_path,
            started_ts=started_ts,
            dry_run=args.dry_run,
            steps=step_summary,
            assemble=assemble,
            pass1=pass1,
            pass2=pass2,
            manifest=manifest_after_gate,
            validate=val,
            trigger=trig,
            staging_dir=staging_dir,
            token_usage=run_usage,
            global_memory_log_path=global_log_path,
            latest_report_path=latest_report_path,
            skip_enrichment=finalize_skip_enrichment,
        )
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    step_summary: dict = {}
    render_result = None
    pass1: dict = None
    pass2: dict = None
    assemble: dict = None

    try:
        # Step 1: discover_sources
        inv_argv = [
            "--workspace", str(workspace),
            "--profile", profile,
            "--config", str(config_path),
            "--timezone", tz_name,
        ]
        if args.dry_run:
            inv_argv.append("--dry-run")
        inventory = _run_script("discover_sources.py", inv_argv, "discover_sources")
        _write_staging(staging_dir / "inventory.json", inventory)
        step_summary["discover"] = {"status": inventory.get("status"), "found": len(inventory.get("found") or [])}
        if inventory.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"discover: {inventory.get('reason') or inventory.get('error')}")
        if inventory.get("status") == "skipped":
            return finalize("skipped", halt_reason=inventory.get("reason", "discover skipped"))

        # Step 2: select_scope
        scope_argv = [
            "--inventory", str(staging_dir / "inventory.json"),
            "--mode", args.mode,
            "--manifest", str(runtime_dir / "purified-manifest.json"),
            "--timezone", tz_name,
        ]
        scope = _run_script("select_scope.py", scope_argv, "select_scope")
        _write_staging(staging_dir / "scope.json", scope)
        step_summary["scope"] = {
            "status": scope.get("status"),
            "scope_count": scope.get("scope_count"),
            "delta_type": scope.get("delta_type"),
            "removed_sources": len(scope.get("removed_sources") or []),
        }
        if scope.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"select_scope: {scope.get('error')}")
        if scope.get("status") == "skipped":
            removed_sources = scope.get("removed_sources") or []
            if removed_sources and not args.dry_run:
                # Stale-only sweep: no new inputs but sources disappeared — run
                # assemble_artifacts without a pass2 payload to mark orphaned
                # claims as retire_candidate. Skip Pass 1/Pass 2 but DO run
                # render_views so the markdown views on disk stop referencing
                # the retired claims (artifact state and rendered views must
                # stay coherent in the same run).
                # v1.5.0 D2: stale-sweep inherits the orchestrator's run_id so
                # claim retirement lineage points at the parent run rather than
                # a synthetic ``sweep-<uuid>``.
                asm_argv = [
                    "--workspace", str(workspace),
                    "--runtime-dir", str(runtime_dir),
                    "--timezone", tz_name,
                    "--run-id", run_id,
                    "--removed-sources", json.dumps(removed_sources),
                    # v1.5.0 A2: stage stale-sweep outputs under publish/ too.
                    "--output-dir", str(staging_publish_dir),
                ]
                assemble = _run_script("assemble_artifacts.py", asm_argv, "assemble_artifacts")
                _write_staging(staging_dir / "assemble.json", assemble)
                step_summary["assemble"] = {
                    "status": assemble.get("status"),
                    "stale_sweep": True,
                    "claim_count_retired_this_run": assemble.get("claim_count_retired_this_run"),
                }
                if assemble.get("status") in {"error", "failed"}:
                    return finalize("failed", halt_reason=f"stale-sweep assemble: {assemble.get('reason') or assemble.get('error')}")

                # Re-render markdown views from the staged artifact state so
                # the rendered views match what will be promoted (A2).
                rv_argv = [
                    "--workspace", str(workspace),
                    "--runtime-dir", str(runtime_dir),
                    "--profile", profile,
                    "--config", str(config_path),
                    "--timezone", tz_name,
                    "--claims", str(staging_publish_dir / "purified-claims.jsonl"),
                    "--output-dir", str(staging_publish_dir),
                ]
                render_result = _run_script("render_views.py", rv_argv, "render_views")
                _write_staging(staging_dir / "render.json", render_result)
                step_summary["render"] = {
                    "status": render_result.get("status"),
                    "stale_sweep": True,
                    "views_rendered": [v["path"] for v in (render_result.get("views_rendered") or [])],
                    "views_skipped": len(render_result.get("views_skipped") or []),
                }

                return finalize(
                    "ok",
                    halt_reason=f"stale sweep: {assemble.get('claim_count_retired_this_run', 0)} claim(s) marked retire_candidate",
                )
            return finalize("skipped", halt_reason=scope.get("reason", "scope skipped"))

        # Step 3: extract_candidates
        ext_argv = [
            "--scope", str(staging_dir / "scope.json"),
            "--workspace", str(workspace),
            "--run-id", run_id,
            "--profile", profile,
            "--mode", args.mode,
            "--timezone", tz_name,
        ]
        if args.dry_run:
            ext_argv.append("--dry-run")
        candidates = _run_script("extract_candidates.py", ext_argv, "extract_candidates")
        _write_staging(staging_dir / "candidates.json", candidates)
        step_summary["extract"] = {"status": candidates.get("status"), "candidate_count": candidates.get("candidate_count")}
        if candidates.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"extract: {candidates.get('reason') or candidates.get('error')}")
        if candidates.get("status") == "skipped":
            return finalize("skipped", halt_reason=candidates.get("reason", "extract skipped"))

        # Step 4: score_promotion (Pass 1)
        # v1.5.0 A3: pass config batching limits through; 0 = back-compat monolithic.
        limits_cfg = (config_snapshot.get("limits") or {}) if isinstance(config_snapshot, dict) else {}
        p1_batch_size = int(limits_cfg.get("max_candidates_per_batch") or 0)
        p2_batch_size = int(limits_cfg.get("max_clusters_per_batch") or 0)
        oversized_strategy = str(limits_cfg.get("oversized_run_strategy") or "bounded_batches")
        sp_argv = [
            "--candidates", str(staging_dir / "candidates.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
            "--batch-size", str(p1_batch_size),
            "--oversized-strategy", oversized_strategy,
        ]
        if args.backend:
            sp_argv.extend(["--backend", args.backend])
        if args.fixture_dir:
            sp_argv.extend(["--fixture-dir", args.fixture_dir])
        if args.dry_run:
            sp_argv.append("--dry-run")
        pass1 = _run_script("score_promotion.py", sp_argv, "score_promotion")
        _write_staging(staging_dir / "pass1.json", pass1)
        step_summary["pass1"] = {"status": pass1.get("status"), "survivor_count": pass1.get("survivor_count"), "verdict_stats": pass1.get("verdict_stats")}
        if pass1.get("status") == "partial_failure":
            return finalize("partial_failure", halt_reason=f"pass1 partial_failure: {pass1.get('errors', [])[:3]}")
        if pass1.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"pass1: {pass1.get('reason') or pass1.get('error')}")
        if pass1.get("status") == "skipped":
            return finalize("skipped", halt_reason=pass1.get("reason", "pass1 skipped"))

        # Step 5: cluster_survivors
        cl_argv = [
            "--pass1", str(staging_dir / "pass1.json"),
            "--timezone", tz_name,
        ]
        clusters = _run_script("cluster_survivors.py", cl_argv, "cluster_survivors")
        _write_staging(staging_dir / "clusters.json", clusters)
        step_summary["cluster"] = {"status": clusters.get("status"), "cluster_count": clusters.get("cluster_count")}
        if clusters.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"cluster: {clusters.get('reason') or clusters.get('error')}")
        if clusters.get("status") == "skipped":
            return finalize("skipped", halt_reason=clusters.get("reason", "cluster skipped"))

        # Step 6: score_purifier (Pass 2)
        p2_argv = [
            "--clusters", str(staging_dir / "clusters.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
            "--batch-size", str(p2_batch_size),
            "--oversized-strategy", oversized_strategy,
        ]
        if args.backend:
            p2_argv.extend(["--backend", args.backend])
        if args.fixture_dir:
            p2_argv.extend(["--fixture-dir", args.fixture_dir])
        if args.dry_run:
            p2_argv.append("--dry-run")
        pass2 = _run_script("score_purifier.py", p2_argv, "score_purifier")
        _write_staging(staging_dir / "pass2.json", pass2)
        step_summary["pass2"] = {"status": pass2.get("status"), "claim_count": pass2.get("claim_count"), "home_stats": pass2.get("home_stats")}
        if pass2.get("status") == "partial_failure":
            return finalize("partial_failure", halt_reason=f"pass2 partial_failure: {pass2.get('errors', [])[:3]}")
        if pass2.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"pass2: {pass2.get('reason') or pass2.get('error')}")
        if pass2.get("status") == "skipped":
            return finalize("skipped", halt_reason=pass2.get("reason", "pass2 skipped"))

        # Step 7: assemble_artifacts (forwards removed_sources for stale sweep)
        # v1.5.0 A2: stage output under publish/; promotion happens in finalize.
        removed_sources = (scope or {}).get("removed_sources") or []
        asm_argv = [
            "--pass2", str(staging_dir / "pass2.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
            "--removed-sources", json.dumps(removed_sources),
            "--output-dir", str(staging_publish_dir),
        ]
        if args.dry_run:
            asm_argv.append("--dry-run")
        assemble = _run_script("assemble_artifacts.py", asm_argv, "assemble_artifacts")
        _write_staging(staging_dir / "assemble.json", assemble)
        step_summary["assemble"] = {"status": assemble.get("status"), "claim_count_total": assemble.get("claim_count_total"), "claim_count_new": assemble.get("claim_count_new")}
        if assemble.get("status") in {"error", "failed"}:
            return finalize("failed", halt_reason=f"assemble: {assemble.get('reason') or assemble.get('error')}")

        # Step 8: render_views — reads the staged claims JSONL, writes views
        # alongside into staging/publish/. Promotion happens in finalize.
        rv_argv = [
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--profile", profile,
            "--config", str(config_path),
            "--timezone", tz_name,
            "--claims", str(staging_publish_dir / "purified-claims.jsonl"),
            "--output-dir", str(staging_publish_dir),
        ]
        if args.dry_run:
            rv_argv.append("--dry-run")
        render_result = _run_script("render_views.py", rv_argv, "render_views")
        _write_staging(staging_dir / "render.json", render_result)
        step_summary["render"] = {
            "status": render_result.get("status"),
            "views_rendered": [v["path"] for v in (render_result.get("views_rendered") or [])],
            "views_skipped": len(render_result.get("views_skipped") or []),
        }
        overall = "ok" if render_result.get("status") in ("ok", "skipped") else "partial_failure"
        return finalize(overall)

    except Exception as e:
        step_summary["orchestrator_exception"] = f"{type(e).__name__}: {e}"
        return finalize("failed", halt_reason=f"orchestrator exception: {type(e).__name__}: {e}")


if __name__ == "__main__":
    sys.exit(main())
