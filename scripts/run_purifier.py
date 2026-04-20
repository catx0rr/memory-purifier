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


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"
DEFAULT_REFLECTIONS_CONFIG = Path.home() / ".openclaw" / "reflections" / "reflections.json"
STALE_LOCK_HOURS_DEFAULT = 2


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


CANONICAL_STATUSES = {
    "ok",                    # pipeline completed, validate passed
    "skipped",               # benign no-op (nothing to process, lock held)
    "skipped_superseded",    # incremental skipped inside a reconciliation window
    "validation_failed",     # pipeline completed but validate reported errors
    "partial_failure",       # Pass 1 or Pass 2 produced invalid output; cursor not advanced
    "error",                 # fundamental step failed
}
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
    date_str = datetime.now().strftime("%Y-%m-%d")
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
        f"_Regenerated {datetime.now().strftime('%Y-%m-%d %H:%M')} {tz_name}._",
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


def _is_reconciliation_window(config: dict, now=None) -> tuple:
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
        now = datetime.now().astimezone()
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
        now = datetime.now().astimezone()
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


def _build_next_schedule(skill_root: Path | None = None) -> dict | None:
    """Resolve the earliest upcoming `memory-purifier-*` cron fire.

    Shells out to `openclaw cron list --json`, picks the earliest candidate
    by local time, and labels its mode as `incremental` or `reconciliation`
    from the job name. Returns `None` when openclaw is unavailable, the
    listing is empty, or no job resolves to a future fire.
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

    now = datetime.now().astimezone()
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


def _build_recall_surface(workspace: Path) -> dict | None:
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

    now = datetime.now().astimezone()

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


def _build_skip_enrichment(workspace: Path) -> dict:
    """Collect deterministic skip-path enrichment in one pass.

    Called only on skip states so the extra file I/O doesn't hit the
    normal run path. Returns `{claimsTotal, nextSchedule, recallSurface}`.
    """
    return {
        "claimsTotal": _count_claims_file(workspace),
        "nextSchedule": _build_next_schedule(),
        "recallSurface": _build_recall_surface(workspace),
    }


def acquire_lock(locks_dir: Path, run_id: str, stale_hours: int) -> tuple:
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

    payload = {
        "run_id": run_id,
        "pid": os.getpid(),
        "acquired_at": datetime.now().astimezone().isoformat(),
        "acquired_at_utc": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    lock_path.write_text(json.dumps(payload, indent=2))
    return True, lock_path, existing_info


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


def _run_script(script_name: str, argv: list, name: str) -> dict:
    """Invoke a sub-script with argv; parse one JSON object from stdout.

    On stdout parse failure (non-JSON, empty, garbage), return a synthetic
    error dict so the orchestrator can halt cleanly without re-raising.
    """
    full = [sys.executable, str(SCRIPT_DIR / script_name)] + argv
    proc = subprocess.run(full, capture_output=True, text=True)
    if proc.returncode != 0:
        return {
            "status": "error",
            "error": f"{name} exit code {proc.returncode}",
            "stderr_head": (proc.stderr or "")[:1000],
            "stdout_head": (proc.stdout or "")[:1000],
        }
    if not (proc.stdout or "").strip():
        return {
            "status": "error",
            "error": f"{name} produced empty stdout",
            "stderr_head": (proc.stderr or "")[:1000],
        }
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        return {
            "status": "error",
            "error": f"{name} stdout not JSON: {e.msg}",
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
    ap.add_argument("--backend", help="Model backend for scoring passes: claude-code | anthropic-sdk | file")
    ap.add_argument("--fixture-dir", help="Fixture directory (backend=file)")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument("--stale-lock-hours", type=int, default=STALE_LOCK_HOURS_DEFAULT, help="Overwrite a lock older than this (default: 2h)")
    ap.add_argument("--keep-staging", action="store_true", help="Preserve the staging directory even on success")
    ap.add_argument("--dry-run", action="store_true", help="Chain runs but no files persist (artifacts, cursor, config)")
    ap.add_argument("--run-id", help="Explicit run_id override (default: generated UUID). Useful for deterministic fixture-based testing.")
    ap.add_argument("--force", action="store_true", help="Override the reconciliation-window guard. Normally an incremental run inside a reconciliation slot skips with status=skipped_superseded.")

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

    # Runtime reconciliation-over-incremental supersession:
    # even if cron has drifted or been misregistered, never let an incremental
    # run inside a reconciliation window.
    if args.mode == "incremental" and not args.force:
        in_window, expr = _is_reconciliation_window(config_snapshot)
        if in_window:
            manifest_path = runtime_dir / "purified-manifest.json"
            summary_path = runtime_dir / "purifier-last-run-summary.json"
            skip_enrichment = _build_skip_enrichment(workspace)
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

    acquired, lock_path, existing = acquire_lock(locks_dir, run_id, args.stale_lock_hours)
    if not acquired:
        manifest_path = runtime_dir / "purified-manifest.json"
        summary_path = runtime_dir / "purifier-last-run-summary.json"
        skip_enrichment = _build_skip_enrichment(workspace)
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
        """Write the manifest, run validation and downstream signal, release lock."""
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
        if args.dry_run:
            wm_argv.append("--dry-run")

        wm = _run_script("write_manifest.py", wm_argv, "write_manifest")
        _write_staging(staging_dir / "manifest.json", wm)

        val_argv = [
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir)
            , "--profile", profile,
            "--config", str(config_path),
            "--timezone", tz_name,
        ]
        val = _run_script("validate_outputs.py", val_argv, "validate_outputs")
        _write_staging(staging_dir / "validate.json", val)

        # If validation errored, invalidate the downstream signal before trigger_wiki runs.
        # The manifest was written optimistically (downstream suggested based on pass-ok state);
        # validation is the final gate — errors here must suppress downstream ingest.
        manifest_path = runtime_dir / "purified-manifest.json"
        if (val or {}).get("status") == "errors" and manifest_path.is_file() and not args.dry_run:
            try:
                manifest_current = json.loads(manifest_path.read_text())
                if manifest_current.get("downstreamWikiIngestSuggested"):
                    manifest_current["downstreamWikiIngestSuggested"] = False
                    manifest_current.setdefault("warnings", []).append({
                        "pass": "validate",
                        "reason": "validate_outputs reported errors — downstream signal suppressed",
                        "error_count": (val or {}).get("error_count"),
                    })
                    tmp = manifest_path.with_name(manifest_path.name + f".tmp.{os.getpid()}")
                    tmp.write_text(json.dumps(manifest_current, indent=2, ensure_ascii=False) + "\n")
                    os.replace(tmp, manifest_path)
            except Exception:
                pass

        # Promote validate-errors into a distinct final status so the cron
        # prompt can report "validation_failed" cleanly.
        final_status = overall_status
        if final_status == "ok" and (val or {}).get("status") == "errors":
            final_status = "validation_failed"

        # Aggregate scoring-pass-only token usage across Pass 1 + Pass 2.
        run_usage = _usage_unavailable()
        if pass1 and isinstance(pass1, dict):
            run_usage = _merge_usage(run_usage, pass1.get("token_usage") or _usage_unavailable())
        if pass2 and isinstance(pass2, dict):
            run_usage = _merge_usage(run_usage, pass2.get("token_usage") or _usage_unavailable())

        # Patch the manifest on-disk with final token_usage + latest paths so
        # last-run-summary.json reflects the validated final state.
        summary_path = runtime_dir / "purifier-last-run-summary.json"
        global_log_path = global_log_root / f"memory-log-{datetime.now().strftime('%Y-%m-%d')}.jsonl"
        latest_report_path = telemetry_root / "last-run.md"

        manifest_after_gate = {}
        if manifest_path.is_file():
            try:
                manifest_after_gate = json.loads(manifest_path.read_text())
            except Exception:
                manifest_after_gate = {}

        # Compute views rendered (for both the summary mirror and the markdown report)
        views_rendered = []
        if render_result and isinstance(render_result, dict):
            views_rendered = [v.get("path") for v in (render_result.get("views_rendered") or []) if v.get("written")]

        # Duration for last-run.md (best-effort)
        duration_seconds = None
        try:
            started_dt = datetime.fromisoformat(started_ts["timestamp"])
            finished_dt = datetime.now().astimezone()
            duration_seconds = (finished_dt - started_dt).total_seconds()
        except Exception:
            duration_seconds = None

        claims_new = (assemble or {}).get("claim_count_new", 0) or 0
        claims_total = (assemble or {}).get("claim_count_total", 0) or 0
        contradiction_count = (pass2 or {}).get("contradiction_count", 0) or 0
        supersession_count = (pass2 or {}).get("supersession_count", 0) or 0
        warnings_on_disk = manifest_after_gate.get("warnings") or []
        partials_on_disk = manifest_after_gate.get("partialFailures") or []
        downstream_final = bool(manifest_after_gate.get("downstreamWikiIngestSuggested"))

        # v1.4.0: precompute skip enrichment once so summary, last-run.md,
        # and the final JSON all share the same state. Cheap (bounded file
        # scan); only runs on skip paths.
        finalize_skip_enrichment = None
        if final_status in {"skipped", "skipped_superseded"}:
            finalize_skip_enrichment = _build_skip_enrichment(workspace)

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
                tmp = summary_path.with_name(summary_path.name + f".tmp.{os.getpid()}")
                tmp.write_text(json.dumps(summary_current, indent=2, ensure_ascii=False) + "\n")
                os.replace(tmp, summary_path)
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
                    finished_at=datetime.now().astimezone().isoformat(),
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
                    manifest_path=manifest_path,
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
                elif final_status in {"partial_failure", "validation_failed", "error"}:
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

        # Downstream signal runs last, only after validation + reporting are done.
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

        release_lock(lock_path)

        cleanup_staging = final_status == "ok" and not args.keep_staging and not args.dry_run
        if cleanup_staging:
            shutil.rmtree(staging_dir, ignore_errors=True)

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
            manifest_path=manifest_path,
            summary_path=summary_path,
            started_ts=started_ts,
            dry_run=args.dry_run,
            steps=step_summary,
            assemble=assemble,
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
        ]
        if args.dry_run:
            inv_argv.append("--dry-run")
        inventory = _run_script("discover_sources.py", inv_argv, "discover_sources")
        _write_staging(staging_dir / "inventory.json", inventory)
        step_summary["discover"] = {"status": inventory.get("status"), "found": len(inventory.get("found") or [])}
        if inventory.get("status") == "error":
            return finalize("error", halt_reason=f"discover: {inventory.get('error')}")
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
        if scope.get("status") == "error":
            return finalize("error", halt_reason=f"select_scope: {scope.get('error')}")
        if scope.get("status") == "skipped":
            removed_sources = scope.get("removed_sources") or []
            if removed_sources and not args.dry_run:
                # Stale-only sweep: no new inputs but sources disappeared — run
                # assemble_artifacts without a pass2 payload to mark orphaned
                # claims as retire_candidate. Skip Pass 1/Pass 2 but DO run
                # render_views so the markdown views on disk stop referencing
                # the retired claims (artifact state and rendered views must
                # stay coherent in the same run).
                asm_argv = [
                    "--workspace", str(workspace),
                    "--runtime-dir", str(runtime_dir),
                    "--timezone", tz_name,
                    "--removed-sources", json.dumps(removed_sources),
                ]
                assemble = _run_script("assemble_artifacts.py", asm_argv, "assemble_artifacts")
                _write_staging(staging_dir / "assemble.json", assemble)
                step_summary["assemble"] = {
                    "status": assemble.get("status"),
                    "stale_sweep": True,
                    "claim_count_retired_this_run": assemble.get("claim_count_retired_this_run"),
                }
                if assemble.get("status") == "error":
                    return finalize("error", halt_reason=f"stale-sweep assemble: {assemble.get('error')}")

                # Re-render markdown views from the updated artifact state.
                rv_argv = [
                    "--workspace", str(workspace),
                    "--runtime-dir", str(runtime_dir),
                    "--profile", profile,
                    "--config", str(config_path),
                    "--timezone", tz_name,
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
        if candidates.get("status") == "error":
            return finalize("error", halt_reason=f"extract: {candidates.get('error')}")
        if candidates.get("status") == "skipped":
            return finalize("skipped", halt_reason=candidates.get("reason", "extract skipped"))

        # Step 4: score_promotion (Pass 1)
        sp_argv = [
            "--candidates", str(staging_dir / "candidates.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
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
        if pass1.get("status") == "error":
            return finalize("error", halt_reason=f"pass1: {pass1.get('error')}")
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
        if clusters.get("status") == "error":
            return finalize("error", halt_reason=f"cluster: {clusters.get('error')}")
        if clusters.get("status") == "skipped":
            return finalize("skipped", halt_reason=clusters.get("reason", "cluster skipped"))

        # Step 6: score_purifier (Pass 2)
        p2_argv = [
            "--clusters", str(staging_dir / "clusters.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
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
        if pass2.get("status") == "error":
            return finalize("error", halt_reason=f"pass2: {pass2.get('error')}")
        if pass2.get("status") == "skipped":
            return finalize("skipped", halt_reason=pass2.get("reason", "pass2 skipped"))

        # Step 7: assemble_artifacts (forwards removed_sources for stale sweep)
        removed_sources = (scope or {}).get("removed_sources") or []
        asm_argv = [
            "--pass2", str(staging_dir / "pass2.json"),
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--timezone", tz_name,
            "--removed-sources", json.dumps(removed_sources),
        ]
        if args.dry_run:
            asm_argv.append("--dry-run")
        assemble = _run_script("assemble_artifacts.py", asm_argv, "assemble_artifacts")
        _write_staging(staging_dir / "assemble.json", assemble)
        step_summary["assemble"] = {"status": assemble.get("status"), "claim_count_total": assemble.get("claim_count_total"), "claim_count_new": assemble.get("claim_count_new")}
        if assemble.get("status") == "error":
            return finalize("error", halt_reason=f"assemble: {assemble.get('error')}")

        # Step 8: render_views
        rv_argv = [
            "--workspace", str(workspace),
            "--runtime-dir", str(runtime_dir),
            "--profile", profile,
            "--config", str(config_path),
            "--timezone", tz_name,
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
        return finalize("error", halt_reason=f"orchestrator exception: {type(e).__name__}: {e}")


if __name__ == "__main__":
    sys.exit(main())
