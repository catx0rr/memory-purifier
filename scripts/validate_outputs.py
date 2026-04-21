#!/usr/bin/env python3
"""Post-run contract validation for memory-purifier artifacts.

Verifies the five machine artifacts and up to five markdown views against
the schema / routing / profile-scope rules from references/routing-rules.md
and references/render-rules.md. Emits a structured report to stdout.

Violations are categorized:
- errors:   contract breaks that should block downstream ingest (e.g. invalid
            primaryHome, personal-only home on business profile)
- warnings: soft issues worth surfacing but not blocking (e.g. missing
            optional markdown view, oversized EPISODES digest)

Status:
- ok         — zero errors, zero warnings
- warnings   — zero errors, ≥1 warning
- errors     — ≥1 error (downstream ingest should be blocked)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _lib.time_utils import timestamp_triple  # noqa: E402
from _lib.statuses import CANONICAL_TOP_LEVEL_STATUS_SET  # noqa: E402


VALID_TYPES = {
    "fact", "lesson", "decision", "commitment", "constraint", "preference",
    "identity", "relationship", "method", "procedure", "episode",
    "aspiration", "milestone", "open_question",
}

# v1.5.0 B3 (Contract 4/spec) — type→home affinity table.
# Each type maps to (strong_home, acceptable_homes_set, impossible_homes_set).
# - strong: the semantically-expected home — no warning, no error.
# - acceptable: plausible edge case — warning only; claim still published.
# - impossible: semantic contradiction — hard error, claim rejected.
# Homes NOT in any of the three categories fall into an implicit "suspicious"
# bucket — treated as acceptable (warning) by default.
_TYPE_HOME_AFFINITY = {
    # Facts / preferences / constraints etc. live in LTMEMORY by design.
    "fact":        ("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "preference":  ("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "constraint":  ("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "commitment":  ("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
    "identity":    ("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "WISHES.md"}),
    "relationship":("LTMEMORY.md", {"LTMEMORY.md"},                                    {"PLAYBOOKS.md", "EPISODES.md", "WISHES.md"}),
    "lesson":      ("LTMEMORY.md", {"LTMEMORY.md", "PLAYBOOKS.md"},                    {"EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "decision":    ("LTMEMORY.md", {"LTMEMORY.md", "PLAYBOOKS.md"},                    {"EPISODES.md", "WISHES.md"}),
    "open_question":("LTMEMORY.md", {"LTMEMORY.md"},                                   {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
    # Methods / procedures are PLAYBOOKS-native; LTMEMORY is a
    # noteworthy edge case when the method is more factoid than recipe.
    "method":      ("PLAYBOOKS.md", {"PLAYBOOKS.md", "LTMEMORY.md"},                   {"EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "procedure":   ("PLAYBOOKS.md", {"PLAYBOOKS.md"},                                  {"LTMEMORY.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    # Episodes are narrative bundles — EPISODES.md only.
    "episode":     ("EPISODES.md",  {"EPISODES.md"},                                   {"LTMEMORY.md", "PLAYBOOKS.md", "WISHES.md"}),
    # Milestones are personal-only timeline events; HISTORY.md is the strong home.
    "milestone":   ("HISTORY.md",   {"HISTORY.md", "EPISODES.md"},                     {"LTMEMORY.md", "PLAYBOOKS.md", "WISHES.md"}),
    # Aspirations are personal-only wishes; anywhere else is impossible.
    "aspiration":  ("WISHES.md",    {"WISHES.md"},                                     {"LTMEMORY.md", "PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
}


def classify_type_home_affinity(ctype: str, home: str) -> tuple:
    """Classify a (type, home) pair per the affinity table.

    Returns ``(state, suggested_home, affinity_score)`` where:
      - ``state`` ∈ {``"strong"``, ``"acceptable"``, ``"suspicious"``, ``"impossible"``}
      - ``suggested_home`` is the affinity-strong home for this type, or None.
      - ``affinity_score`` is a deterministic 0.1/0.5/1.0 scalar useful as a
        weak ranking signal but not a threshold — suitable for per-claim
        diagnostic surfaces.
    """
    entry = _TYPE_HOME_AFFINITY.get(ctype)
    if entry is None:
        # Unknown type — caller handles via VALID_TYPES check, but be safe.
        return ("suspicious", None, 0.1)
    strong_home, acceptable, impossible = entry
    if home in impossible:
        return ("impossible", strong_home, 0.0)
    if home == strong_home:
        return ("strong", None, 1.0)
    if home in acceptable:
        return ("acceptable", strong_home, 0.5)
    return ("suspicious", strong_home, 0.1)
VALID_STATUSES = {"resolved", "contested", "unresolved", "superseded", "stale", "retire_candidate", "probable_duplicate"}
INACTIVE_STATUSES = {"superseded", "stale", "retire_candidate"}
VALID_HOMES = {"LTMEMORY.md", "PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}
PERSONAL_ONLY_HOMES = {"HISTORY.md", "WISHES.md"}

MANIFEST_REQUIRED_KEYS = [
    "version", "runId", "mode", "status", "profileScope",
    "startedAt", "finishedAt", "timezone",
    "sourceInventory", "processedSegments",
    "promotionStats", "claimStats",
    "warnings", "partialFailures",
    "lastSuccessfulCursor", "downstreamWikiIngestSuggested",
]

EPISODES_DIGEST_CAP = 500

DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"


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
    return "personal"


def _load_jsonl(path: Path) -> tuple:
    """Return (records, parse_errors)."""
    records: list = []
    errors: list = []
    if not path.is_file():
        return records, errors
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                errors.append(f"{path.name}:{lineno} parse error: {e.msg}")
    return records, errors


def check_files_exist(runtime_dir: Path, workspace: Path, profile: str) -> tuple:
    errors: list = []
    warnings: list = []
    required = [
        runtime_dir / "purified-claims.jsonl",
        runtime_dir / "purified-contradictions.jsonl",
        runtime_dir / "purified-entities.json",
        runtime_dir / "purified-routes.json",
        runtime_dir / "purified-manifest.json",
    ]
    for p in required:
        if not p.is_file():
            errors.append(f"missing required artifact: {p}")

    required_views = [workspace / "LTMEMORY.md", workspace / "PLAYBOOKS.md", workspace / "EPISODES.md"]
    for v in required_views:
        if not v.is_file():
            warnings.append(f"missing markdown view: {v}")

    if profile == "personal":
        for v in (workspace / "HISTORY.md", workspace / "WISHES.md"):
            if not v.is_file():
                warnings.append(f"missing personal markdown view: {v}")

    return errors, warnings


def check_manifest(path: Path) -> tuple:
    errors: list = []
    warnings: list = []
    if not path.is_file():
        return errors, warnings
    try:
        manifest = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        errors.append(f"manifest parse error: {e.msg}")
        return errors, warnings
    for k in MANIFEST_REQUIRED_KEYS:
        if k not in manifest:
            errors.append(f"manifest missing required key: {k}")
    # v1.5.0 Contract 1 — single-sourced from `_lib.statuses`. Validator,
    # write_manifest argparse choices, and orchestrator CANONICAL_STATUSES
    # all resolve to the same frozenset; divergence is a contract break.
    status = manifest.get("status")
    if status is not None and status not in CANONICAL_TOP_LEVEL_STATUS_SET:
        allowed = "|".join(sorted(CANONICAL_TOP_LEVEL_STATUS_SET))
        warnings.append(
            f"manifest.status={status!r} unrecognized (expected {allowed})"
        )
    prof = manifest.get("profileScope")
    if prof not in (None, "business", "personal", "shared"):
        warnings.append(f"manifest.profileScope={prof!r} unrecognized")
    return errors, warnings


def check_claims(claims: list, profile: str) -> tuple:
    errors: list = []
    warnings: list = []
    if not claims:
        return errors, warnings
    seen_ids: set = set()
    claims_by_id: dict = {c.get("id"): c for c in claims if c.get("id")}

    for i, c in enumerate(claims):
        prefix = f"claim[{i}]"
        cid = c.get("id")
        if not cid:
            errors.append(f"{prefix} missing id")
            continue
        if cid in seen_ids:
            errors.append(f"{prefix} duplicate id: {cid}")
        seen_ids.add(cid)

        ctype = c.get("type")
        if ctype not in VALID_TYPES:
            errors.append(f"{prefix} id={cid} type={ctype!r} not in valid set")

        cstatus = c.get("status")
        if cstatus not in VALID_STATUSES:
            errors.append(f"{prefix} id={cid} status={cstatus!r} not in valid set")

        home = c.get("primaryHome")
        if home not in VALID_HOMES:
            errors.append(f"{prefix} id={cid} primaryHome={home!r} not in valid set")
        elif home in PERSONAL_ONLY_HOMES and profile != "personal":
            errors.append(f"{prefix} id={cid} primaryHome={home!r} only allowed on personal profile (got {profile!r})")
        else:
            # v1.5.0 B3: type-home affinity check. impossible → hard error;
            # acceptable/suspicious → warning with suggested home.
            if ctype in VALID_TYPES:
                route_state, suggested_home, _score = classify_type_home_affinity(ctype, home)
                if route_state == "impossible":
                    errors.append(
                        f"{prefix} id={cid} type={ctype!r}+primaryHome={home!r}: "
                        f"type-home pairing forbidden; expected {suggested_home}"
                    )
                elif route_state in ("acceptable", "suspicious"):
                    warnings.append(
                        f"{prefix} id={cid} type={ctype!r}+primaryHome={home!r}: "
                        f"route affinity={route_state!r}, suggested home {suggested_home!r}"
                    )

        prof_scope = c.get("profileScope")
        if prof_scope not in (None, "business", "personal", "shared"):
            warnings.append(f"{prefix} id={cid} profileScope={prof_scope!r} unrecognized")

        text = c.get("text")
        if not isinstance(text, str) or not text.strip():
            errors.append(f"{prefix} id={cid} text missing or empty")

        prov = c.get("provenance") or []
        if not prov:
            errors.append(f"{prefix} id={cid} provenance empty")

        stags = c.get("secondaryTags") or []
        if isinstance(stags, list) and home in stags:
            warnings.append(f"{prefix} id={cid} secondaryTags contains own primaryHome ({home})")

        if ctype == "episode":
            if isinstance(text, str) and len(text) > EPISODES_DIGEST_CAP:
                warnings.append(
                    f"{prefix} id={cid} EPISODES digest exceeds {EPISODES_DIGEST_CAP} chars ({len(text)})"
                )
            if prov:
                src = (prov[0] or {}).get("source") or ""
                if not src.startswith("episodes/"):
                    warnings.append(
                        f"{prefix} id={cid} EPISODES provenance[0].source={src!r} does not start with 'episodes/'"
                    )

        for sup_id in (c.get("supersedes") or []):
            target = claims_by_id.get(sup_id)
            if target is None:
                warnings.append(f"{prefix} id={cid} supersedes {sup_id!r} but referenced claim not found")
                continue
            tsb = target.get("supersededBy") or []
            if cid not in tsb:
                errors.append(
                    f"{prefix} id={cid} supersedes {sup_id} but {sup_id}.supersededBy lacks {cid} (chain inconsistent)"
                )
            if target.get("status") != "superseded":
                errors.append(
                    f"{prefix} id={cid} supersedes {sup_id} but {sup_id}.status={target.get('status')!r} (expected 'superseded')"
                )

        for by_id in (c.get("supersededBy") or []):
            if by_id == cid:
                errors.append(f"{prefix} id={cid} supersededBy contains self")
            if by_id not in claims_by_id:
                warnings.append(f"{prefix} id={cid} supersededBy {by_id!r} but referenced claim not found")

    return errors, warnings


def check_routes(routes_path: Path, claims: list) -> tuple:
    errors: list = []
    warnings: list = []
    if not routes_path.is_file():
        return errors, warnings
    try:
        routes = json.loads(routes_path.read_text())
    except json.JSONDecodeError as e:
        errors.append(f"routes parse error: {e.msg}")
        return errors, warnings
    for home in VALID_HOMES:
        if home not in routes:
            warnings.append(f"routes file missing key for {home}")
    if not isinstance(routes, dict):
        errors.append("routes file is not a JSON object")
        return errors, warnings

    claims_by_id = {c.get("id"): c for c in claims if c.get("id")}
    for home, ids in routes.items():
        if not isinstance(ids, list):
            errors.append(f"routes[{home!r}] is not a list")
            continue
        for cid in ids:
            claim = claims_by_id.get(cid)
            if claim is None:
                errors.append(f"routes[{home!r}] references unknown claim_id: {cid}")
                continue
            if claim.get("primaryHome") != home:
                errors.append(
                    f"routes[{home!r}] includes {cid} whose primaryHome={claim.get('primaryHome')!r}"
                )
            if claim.get("status") in INACTIVE_STATUSES:
                errors.append(f"routes[{home!r}] includes inactive claim ({claim.get('status')}): {cid}")
    return errors, warnings


def check_markdown_view_presence(workspace: Path, claims: list, profile: str) -> tuple:
    errors: list = []
    warnings: list = []
    actives_by_home: dict = {}
    for c in claims:
        if c.get("status") in INACTIVE_STATUSES:
            continue
        home = c.get("primaryHome")
        if home in VALID_HOMES:
            actives_by_home.setdefault(home, []).append(c)

    eligible_views = ["LTMEMORY.md", "PLAYBOOKS.md", "EPISODES.md"]
    if profile == "personal":
        eligible_views += ["HISTORY.md", "WISHES.md"]

    for view in eligible_views:
        path = workspace / view
        if not path.is_file():
            continue
        has_any_claim = len(actives_by_home.get(view, [])) > 0
        content = path.read_text()
        if has_any_claim and "###" not in content:
            warnings.append(
                f"{view} has active claims ({len(actives_by_home[view])}) but rendered output lacks claim headings"
            )

    if profile == "business":
        for view in ("HISTORY.md", "WISHES.md"):
            path = workspace / view
            if path.is_file():
                warnings.append(f"business profile but personal-only view exists: {view}")

    return errors, warnings


def main() -> int:
    ap = argparse.ArgumentParser(description="Post-run contract validation for memory-purifier artifacts.")
    ap.add_argument("--workspace", help="Workspace root (default: env / config / ~/.openclaw/workspace)")
    ap.add_argument("--runtime-dir", help="Runtime dir override")
    ap.add_argument("--profile", choices=["business", "personal"], help="Profile for view eligibility checks")
    ap.add_argument("--config", help=f"Path to memory-purifier.json (default: {DEFAULT_CONFIG})")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument(
        "--target-dir",
        help="Override for the validation target directory (v1.5.0 A2). Default = --runtime-dir "
             "for artifacts and --workspace for views (back-compat). When set to a staging path, "
             "both artifacts AND views are read from that single staging dir — used by the "
             "transactional commit pipeline to validate staged output BEFORE promotion.",
    )

    args = ap.parse_args()

    config_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG
    profile = resolve_profile(args.profile, config_path)
    tz_name = args.timezone or "Asia/Manila"
    ts = timestamp_triple(tz_name)

    cfg_snapshot = _load_json_safely(config_path) if config_path.is_file() else {}
    cfg_workspace = (cfg_snapshot.get("paths") or {}).get("workspace")
    workspace_hint = (
        args.workspace
        or (cfg_workspace if isinstance(cfg_workspace, str) and cfg_workspace else None)
        or os.environ.get("WORKSPACE")
    )
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")

    # v1.5.0 A2: when the orchestrator stages outputs, artifacts + views
    # both live under one staging dir; point everything there.
    if args.target_dir:
        target = Path(args.target_dir).expanduser()
        artifact_dir = target
        view_dir = target
    else:
        artifact_dir = runtime_dir
        view_dir = workspace

    errors: list = []
    warnings: list = []

    fe, fw = check_files_exist(artifact_dir, view_dir, profile)
    errors.extend(fe); warnings.extend(fw)

    claims_path = artifact_dir / "purified-claims.jsonl"
    claims, parse_errors = _load_jsonl(claims_path)
    for pe in parse_errors:
        errors.append(pe)

    me, mw = check_manifest(artifact_dir / "purified-manifest.json")
    errors.extend(me); warnings.extend(mw)

    ce, cw = check_claims(claims, profile)
    errors.extend(ce); warnings.extend(cw)

    re_, rw = check_routes(artifact_dir / "purified-routes.json", claims)
    errors.extend(re_); warnings.extend(rw)

    ve, vw = check_markdown_view_presence(view_dir, claims, profile)
    errors.extend(ve); warnings.extend(vw)

    if errors:
        status = "errors"
    elif warnings:
        status = "warnings"
    else:
        status = "ok"

    out = {
        "status": status,
        "pass": "validate",
        "profile_scope": profile,
        "workspace": str(workspace),
        "runtime_dir": str(runtime_dir),
        "claim_count": len(claims),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        **ts,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
