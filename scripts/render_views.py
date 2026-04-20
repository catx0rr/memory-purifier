#!/usr/bin/env python3
"""Render human-facing markdown views from artifact state.

Reads <runtime>/purified-claims.jsonl and rewrites the five human-facing views
at <workspace>/ (LTMEMORY.md, PLAYBOOKS.md, EPISODES.md, and — on personal
profile — HISTORY.md and WISHES.md). Layout is defined by
references/render-rules.md.

Determinism contract: given the same artifact state, rendered output is
byte-identical across reruns except for a single regeneration-timestamp line.
Superseded claims are excluded from all views.

All writes are atomic (temp file + os.replace). A failed render leaves the
prior view intact.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_CONFIG = Path.home() / ".openclaw" / "memory-purifier" / "memory-purifier.json"

EPISODES_DIGEST_CAP = 500

LTMEMORY_SECTIONS = [
    ("Facts", "fact", ("subject_asc", "updated_desc")),
    ("Preferences", "preference", ("subject_asc",)),
    ("Constraints", "constraint", ("subject_asc",)),
    ("Commitments", "commitment", ("updated_desc",)),
    ("Lessons", "lesson", ("updated_desc",)),
    ("Decisions", "decision", ("updated_desc",)),
    ("Identity", "identity", ("subject_asc",)),
    ("Relationships", "relationship", ("subject_asc",)),
    ("Open Questions", "open_question", ("updated_desc",)),
]


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
    return "personal"


def resolve_timezone(config_path: Path) -> str:
    if config_path.is_file():
        cfg = _load_json_safely(config_path)
        tz = cfg.get("timezone")
        if isinstance(tz, str) and tz:
            return tz
    return "Asia/Manila"


def load_jsonl(path: Path) -> list:
    if not path.is_file():
        return []
    out: list = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _active_claims(claims: list) -> list:
    inactive = {"superseded", "stale", "retire_candidate"}
    return [c for c in claims if c.get("status") not in inactive]


def _updated_at(c: dict) -> str:
    return (c.get("updatedAt") or c.get("updatedAt_utc") or "").strip()


def _captured_at(c: dict) -> str:
    prov = c.get("provenance") or []
    if prov and isinstance(prov[0], dict):
        return (prov[0].get("capturedAt") or "").strip()
    return ""


def _short_date(iso_str: str) -> str:
    if not iso_str:
        return ""
    return iso_str[:10]


def _sort_key(kind: str, c: dict):
    subj = (c.get("subject") or "").lower()
    if kind == "subject_asc":
        return (subj, _updated_at(c))
    if kind == "updated_desc":
        return ("", _negative_str(_updated_at(c)))
    if kind == "captured_desc":
        return _negative_str(_captured_at(c))
    if kind == "captured_asc":
        return _captured_at(c)
    if kind == "recurrence_desc":
        rec = c.get("recurrence")
        rec_neg = -(rec if isinstance(rec, (int, float)) else 0)
        return (rec_neg, _negative_str(_updated_at(c)))
    return (subj,)


def _negative_str(s: str) -> str:
    """For descending string sort as a single key in ascending sort tuples."""
    return "".join(chr(0x10FFFF - ord(ch)) if ord(ch) < 0x10FFFF else ch for ch in s)


def _apply_sorts(claims: list, sorts: tuple) -> list:
    for kind in reversed(sorts):
        claims = sorted(claims, key=lambda c: _sort_key(kind, c))
    return claims


def _claim_heading(c: dict) -> str:
    subj = (c.get("subject") or "").strip()
    pred = (c.get("predicate") or "").strip()
    obj = (c.get("object") or "").strip()
    if subj and pred:
        if obj:
            return f"### {subj} — {pred} {obj}"
        return f"### {subj} — {pred}"
    text = (c.get("text") or "").strip()
    if not text:
        return f"### {c.get('id', 'claim')}"
    fallback = text.splitlines()[0].strip()
    if len(fallback) > 80:
        fallback = fallback[:77] + "…"
    return f"### {fallback}"


def _claim_metadata_line(c: dict, verbose_status: bool = False) -> str:
    prov = c.get("provenance") or []
    parts = []
    if prov:
        first = prov[0] or {}
        src = first.get("source") or "?"
        span = first.get("lineSpan") or [1, 1]
        if len(prov) == 1:
            parts.append(f"_Source: {src} L{span[0]}-{span[1]}_")
        else:
            sources = sorted({(p or {}).get("source") or "?" for p in prov})
            parts.append(f"_Sources: {', '.join(sources)}_")
    status = c.get("status")
    if verbose_status or (status and status != "resolved"):
        parts.append(f"_Status: {status}_")
    conf = c.get("confidencePosture")
    if conf:
        parts.append(f"_Confidence: {conf}_")
    updated = _short_date(_updated_at(c))
    if updated:
        parts.append(f"_Updated: {updated}_")
    return "  ".join(parts)


def _contradictions_block(c: dict) -> list:
    lines: list = []
    contras = c.get("contradictions") or []
    if not contras:
        return lines
    lines.append("")
    lines.append("> Contested by:")
    for cc in contras:
        if not isinstance(cc, dict):
            continue
        ref = cc.get("competingClaimId") or cc.get("competingText") or "(unknown)"
        rel = cc.get("relation") or "contested"
        lines.append(f"> - [{rel}] {ref}")
    return lines


def _render_claim(c: dict) -> list:
    lines: list = [_claim_heading(c)]
    text = (c.get("text") or "").strip()
    if text:
        lines.append(text)
    meta = _claim_metadata_line(c)
    if meta:
        lines.append("")
        lines.append(meta)
    lines.extend(_contradictions_block(c))
    return lines


def _header(title: str, ts_local: str, subtitle: str = None, do_not_edit: bool = False) -> list:
    lines = [f"# {title}", ""]
    lines.append(f"_Regenerated {_short_date(ts_local) or ts_local} {ts_local.split('T')[1][:5] if 'T' in ts_local else ''} Asia/Manila from `runtime/purified-claims.jsonl`._".rstrip())
    if do_not_edit:
        lines.append("_Do not edit this file by hand — changes will be overwritten on the next purifier run._")
    if subtitle:
        lines.append(subtitle)
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def render_ltmemory(claims: list, ts_local: str) -> str:
    active = [c for c in _active_claims(claims) if c.get("primaryHome") == "LTMEMORY.md"]
    lines = _header("LTMEMORY — Purified Durable Memory", ts_local, do_not_edit=True)
    any_section = False
    for section_title, type_filter, sorts in LTMEMORY_SECTIONS:
        members = [c for c in active if c.get("type") == type_filter]
        if not members:
            continue
        any_section = True
        members = _apply_sorts(members, sorts)
        lines.append(f"## {section_title}")
        lines.append("")
        for c in members:
            lines.extend(_render_claim(c))
            lines.append("")
    if not any_section:
        return "\n".join(lines).rstrip() + "\n"
    return "\n".join(lines).rstrip() + "\n"


def render_playbooks(claims: list, ts_local: str) -> str:
    active = [
        c for c in _active_claims(claims)
        if c.get("primaryHome") == "PLAYBOOKS.md" and c.get("type") in ("method", "procedure")
    ]
    lines = _header("PLAYBOOKS — Purified Reusable Methods", ts_local)

    grouped: dict = {}
    for c in active:
        tags = [t for t in (c.get("secondaryTags") or []) if t and not t.endswith(".md")]
        if tags:
            key = tags[0]
        else:
            key = "Uncategorized"
        grouped.setdefault(key, []).append(c)

    ordered_keys = sorted((k for k in grouped if k != "Uncategorized"), key=lambda k: k.lower())
    if "Uncategorized" in grouped:
        ordered_keys.append("Uncategorized")

    for key in ordered_keys:
        members = _apply_sorts(grouped[key], ("subject_asc",))
        lines.append(f"## {key}")
        lines.append("")
        for c in members:
            lines.extend(_render_claim(c))
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _episode_heading(c: dict) -> str:
    captured = _short_date(_captured_at(c))
    subj = (c.get("subject") or "").strip() or "episode"
    if captured:
        return f"### {captured} — {subj}"
    return f"### {subj}"


def _episode_digest(text: str) -> str:
    text = (text or "").strip()
    if len(text) <= EPISODES_DIGEST_CAP:
        return text
    return text[: EPISODES_DIGEST_CAP - 1].rstrip() + "…"


def _episode_source_link(prov_entry: dict) -> str:
    src = (prov_entry or {}).get("source") or ""
    span = (prov_entry or {}).get("lineSpan") or [1, 1]
    if src.startswith("episodes/"):
        return f"[{src}]({src}) L{span[0]}-{span[1]}"
    return f"{src} L{span[0]}-{span[1]}"


def _render_episode_block(c: dict) -> list:
    lines = [_episode_heading(c)]
    digest = _episode_digest(c.get("text") or "")
    if digest:
        lines.append(digest)
    prov = c.get("provenance") or []
    first_prov = prov[0] if prov else {}
    src_link = _episode_source_link(first_prov)
    parts = [f"_Source: {src_link}_"]
    tags = [t for t in (c.get("secondaryTags") or []) if t and not t.endswith(".md")]
    if tags:
        parts.append(f"_Tags: {', '.join(tags)}_")
    lines.append("")
    lines.append("  ".join(parts))
    lines.extend(_contradictions_block(c))
    return lines


def render_episodes(claims: list, ts_local: str) -> str:
    active = [
        c for c in _active_claims(claims)
        if c.get("primaryHome") == "EPISODES.md" and c.get("type") == "episode"
    ]
    subtitle = "_Full narratives live in `episodes/<slug>.md`. This file is an index._"
    lines = _header("EPISODES — Purified Event Digest", ts_local, subtitle=subtitle)
    active = _apply_sorts(active, ("captured_desc",))
    for c in active:
        lines.extend(_render_episode_block(c))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_history_block(c: dict) -> list:
    captured = _short_date(_captured_at(c))
    subj = (c.get("subject") or "").strip() or (c.get("text") or "")[:60]
    heading = f"### {captured} — {subj}" if captured else f"### {subj}"
    lines = [heading]
    text = (c.get("text") or "").strip()
    if text:
        lines.append(text)
    prov = c.get("provenance") or []
    sources = sorted({(p or {}).get("source") or "?" for p in prov})
    parts = []
    if sources:
        parts.append(f"_Source: {', '.join(sources)}_")
    tags = [t for t in (c.get("secondaryTags") or []) if t and not t.endswith(".md")]
    if tags:
        parts.append(f"_Tags: {', '.join(tags)}_")
    if parts:
        lines.append("")
        lines.append("  ".join(parts))
    lines.extend(_contradictions_block(c))
    return lines


def render_history(claims: list, ts_local: str) -> str:
    active = [
        c for c in _active_claims(claims)
        if c.get("primaryHome") == "HISTORY.md" and c.get("type") == "milestone"
    ]
    lines = _header("HISTORY — Personal Milestones & Turning Points", ts_local)
    active = _apply_sorts(active, ("captured_asc",))
    for c in active:
        lines.extend(_render_history_block(c))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_wish_block(c: dict) -> list:
    subj = (c.get("subject") or "").strip() or (c.get("text") or "")[:60]
    lines = [f"### {subj}"]
    text = (c.get("text") or "").strip()
    if text:
        lines.append(text)
    rec = c.get("recurrence")
    last_reinforced = _short_date(_updated_at(c))
    parts = []
    if isinstance(rec, (int, float)):
        parts.append(f"_Recurrence: {int(rec)} occurrences_")
    if last_reinforced:
        parts.append(f"_Last reinforced: {last_reinforced}_")
    if parts:
        lines.append("")
        lines.append("  ".join(parts))
    lines.extend(_contradictions_block(c))
    return lines


def render_wishes(claims: list, ts_local: str) -> str:
    active_all = [
        c for c in _active_claims(claims)
        if c.get("primaryHome") == "WISHES.md" and c.get("type") == "aspiration"
    ]
    resolved_aspirations = [c for c in active_all if c.get("status") == "resolved"]
    unresolved = [c for c in active_all if c.get("status") == "unresolved"]
    subtitle = (
        "_Aspirations included here are evidenced across multiple entries or reinforced by chronicles. "
        "One-off dream residue is excluded._"
    )
    lines = _header("WISHES — Stable Aspirations", ts_local, subtitle=subtitle)
    if resolved_aspirations:
        lines.append("## Active Aspirations")
        lines.append("")
        for c in _apply_sorts(resolved_aspirations, ("recurrence_desc", "updated_desc")):
            lines.extend(_render_wish_block(c))
            lines.append("")
    if unresolved:
        lines.append("## Emerging Patterns")
        lines.append("")
        for c in _apply_sorts(unresolved, ("updated_desc",)):
            lines.extend(_render_wish_block(c))
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def atomic_write_text(path: Path, content: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)
    return len(content.encode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Render human-facing markdown views from purified artifacts.")
    ap.add_argument("--claims", help="Path to purified-claims.jsonl (default: <runtime>/purified-claims.jsonl)")
    ap.add_argument("--workspace", help="Workspace root for view output (default: config/env)")
    ap.add_argument("--runtime-dir", help="Runtime dir override (for claims lookup)")
    ap.add_argument("--profile", choices=["business", "personal"], help="Profile — governs personal view eligibility")
    ap.add_argument("--config", help=f"Path to memory-purifier.json (default: {DEFAULT_CONFIG})")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument("--dry-run", action="store_true", help="Render but do not write files; echo paths in output")
    ap.add_argument("--force-personal", action="store_true", help="Force personal views even on business profile (for debugging)")

    args = ap.parse_args()

    config_path = Path(args.config).expanduser() if args.config else DEFAULT_CONFIG
    profile = resolve_profile(args.profile, config_path)
    tz_name = args.timezone or resolve_timezone(config_path)
    ts = timestamp_triple(tz_name)

    workspace_hint = args.workspace or os.environ.get("WORKSPACE")
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")

    claims_path = Path(args.claims).expanduser() if args.claims else (runtime_dir / "purified-claims.jsonl")
    claims = load_jsonl(claims_path)

    views_plan: list = [
        ("LTMEMORY.md", render_ltmemory, True),
        ("PLAYBOOKS.md", render_playbooks, True),
        ("EPISODES.md", render_episodes, True),
        ("HISTORY.md", render_history, profile == "personal" or args.force_personal),
        ("WISHES.md", render_wishes, profile == "personal" or args.force_personal),
    ]

    views_rendered: list = []
    views_skipped: list = []
    for filename, renderer, eligible in views_plan:
        target = workspace / filename
        if not eligible:
            views_skipped.append({"path": str(target), "reason": f"profile={profile}: personal-only view not eligible"})
            continue
        try:
            content = renderer(claims, ts["timestamp"])
        except Exception as e:
            views_skipped.append({"path": str(target), "reason": f"render error: {type(e).__name__}: {e}"})
            continue
        if args.dry_run:
            views_rendered.append({
                "path": str(target),
                "bytes": len(content.encode("utf-8")),
                "lines": content.count("\n"),
                "written": False,
            })
        else:
            written_bytes = atomic_write_text(target, content)
            views_rendered.append({
                "path": str(target),
                "bytes": written_bytes,
                "lines": content.count("\n"),
                "written": True,
            })

    out = {
        "status": "ok" if views_rendered else "skipped",
        "pass": "render",
        "profile_scope": profile,
        "workspace": str(workspace),
        "claims_path": str(claims_path),
        "claim_count_total": len(claims),
        "claim_count_active": len(_active_claims(claims)),
        "views_rendered": views_rendered,
        "views_skipped": views_skipped,
        "dry_run": args.dry_run,
        **ts,
    }
    if not views_rendered:
        out["reason"] = "no views rendered"
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
