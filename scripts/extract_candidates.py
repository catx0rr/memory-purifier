#!/usr/bin/env python3
"""Extract candidate memory units from in-scope source files.

Splits each source into paragraph- or section-level units, attaches provenance,
and produces the input shape defined by prompts/promotion-pass.md §2. Emits
one JSON object to stdout.
"""

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


TYPE_HINTS_BY_SOURCE = {
    "MEMORY.md": "fact",
    "RTMEMORY.md": "lesson",
    "PROCEDURES.md": "method",
    "CHRONICLES.md": "milestone",
    "DREAMS.md": "aspiration",
}
EPISODE_HINT = "episode"

MIN_UNIT_CHARS = 10
HORIZONTAL_RULES = {"---", "___", "***"}


def _is_heading_only(body: str) -> bool:
    """Body consists entirely of markdown heading lines (no prose payload).

    These are structural scaffolding — they carry no semantic claim. Filter
    them before Pass 1 rather than burning verdict slots on them.
    """
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    if not lines:
        return True
    return all(ln.startswith("#") for ln in lines)


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def _file_captured_at(path: Path) -> str:
    dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _normalize_for_hash(text: str) -> str:
    t = re.sub(r"\s+", " ", text.strip())
    return t.lower()


def _candidate_id(source_path: str, line_start: int, text: str) -> str:
    key = f"{source_path}:{line_start}:{_normalize_for_hash(text)}"
    return "cand-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def split_paragraphs(text: str) -> list:
    """Paragraph units separated by blank lines. Returns list of (start, end, body)."""
    lines = text.splitlines()
    units: list = []
    buf: list = []
    buf_start = None

    def flush(end_line: int) -> None:
        nonlocal buf, buf_start
        if buf and buf_start is not None:
            body = "\n".join(buf).strip()
            if len(body) >= MIN_UNIT_CHARS and not _is_heading_only(body):
                units.append((buf_start, end_line, body))
        buf = []
        buf_start = None

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped in HORIZONTAL_RULES:
            flush(end_line=i - 1)
            continue
        if buf_start is None:
            buf_start = i
        buf.append(line)

    flush(end_line=len(lines))
    return units


def split_by_h2_sections(text: str) -> list:
    """Split on markdown '## ' headings. Each section (heading + body) is one unit.

    Falls back to paragraph split when no '## ' sections are present.
    """
    lines = text.splitlines()
    units: list = []
    section_start = None
    section_buf: list = []

    def flush(end_line: int) -> None:
        nonlocal section_buf, section_start
        if section_buf and section_start is not None:
            body = "\n".join(section_buf).strip()
            if len(body) >= MIN_UNIT_CHARS and not _is_heading_only(body):
                units.append((section_start, end_line, body))
        section_buf = []
        section_start = None

    for i, line in enumerate(lines, start=1):
        if line.startswith("## "):
            flush(end_line=i - 1)
            section_start = i
            section_buf = [line]
        elif section_start is not None:
            section_buf.append(line)

    flush(end_line=len(lines))

    if not units:
        return split_paragraphs(text)
    return units


def extract_file(file_path: Path, rel_path: str, captured_at: str) -> list:
    text = file_path.read_text(encoding="utf-8", errors="replace")

    if rel_path.startswith("episodes/"):
        lines = text.splitlines()
        body = text.strip()
        if len(body) < MIN_UNIT_CHARS:
            return []
        return [{
            "candidate_id": _candidate_id(rel_path, 1, body),
            "text": body,
            "type_hint": EPISODE_HINT,
            "source_refs": [{
                "source": rel_path,
                "line_span": [1, max(1, len(lines))],
                "captured_at": captured_at,
            }],
        }]

    if rel_path == "PROCEDURES.md":
        units = split_by_h2_sections(text)
    else:
        units = split_paragraphs(text)

    type_hint = TYPE_HINTS_BY_SOURCE.get(rel_path, "unknown")

    candidates = []
    for start, end, body in units:
        candidates.append({
            "candidate_id": _candidate_id(rel_path, start, body),
            "text": body,
            "type_hint": type_hint,
            "source_refs": [{
                "source": rel_path,
                "line_span": [start, end],
                "captured_at": captured_at,
            }],
        })
    return candidates


def _load_scope(arg: str) -> dict:
    if arg == "-":
        return json.load(sys.stdin)
    return json.loads(Path(arg).expanduser().read_text())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract candidate memory units from in-scope source files. "
                    "Output is the Pass 1 input shape.",
    )
    ap.add_argument("--scope", required=True, help="Path to scope JSON (from select_scope.py) or '-' for stdin")
    ap.add_argument("--workspace", help="Workspace root (default: inventory/scope workspace field)")
    ap.add_argument("--run-id", help="Explicit run ID (default: generated UUID)")
    ap.add_argument("--profile", choices=["business", "personal"], help="Profile override (default: from scope)")
    ap.add_argument("--mode", choices=["incremental", "reconciliation"], help="Mode override (default: from scope)")
    ap.add_argument("--timezone", default=None, help="Timezone name (default: from scope)")
    ap.add_argument("--dry-run", action="store_true", help="Read-only; echoed in output for chain compatibility")

    args = ap.parse_args()

    try:
        scope_obj = _load_scope(args.scope)
    except Exception as e:
        out = {
            "status": "error",
            "error": f"failed to load scope: {type(e).__name__}: {e}",
            "dry_run": args.dry_run,
            **timestamp_triple(args.timezone or "Asia/Manila"),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    tz_name = args.timezone or scope_obj.get("timezone") or "Asia/Manila"
    profile = args.profile or scope_obj.get("profile") or "business"
    mode = args.mode or scope_obj.get("mode") or "incremental"
    workspace = args.workspace or scope_obj.get("workspace")

    if not workspace:
        out = {
            "status": "error",
            "error": "workspace not provided and missing from scope",
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    workspace_path = Path(workspace).expanduser().resolve()

    if scope_obj.get("status") != "ok":
        out = {
            "status": "skipped",
            "reason": f"scope status is {scope_obj.get('status')!r}",
            "run_id": args.run_id or str(uuid.uuid4()),
            "mode": mode,
            "profile_scope": profile,
            "candidate_count": 0,
            "candidates": [],
            "warnings": [],
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    scope_files = scope_obj.get("scope", [])
    all_candidates: list = []
    warnings: list = []

    for entry in scope_files:
        rel_path = entry.get("path")
        if not rel_path:
            warnings.append({"issue": "scope entry missing 'path' field"})
            continue
        abs_path = workspace_path / rel_path
        if not abs_path.is_file():
            warnings.append({"path": rel_path, "issue": "file missing at extraction time"})
            continue
        try:
            captured_at = _file_captured_at(abs_path)
            cands = extract_file(abs_path, rel_path, captured_at)
            all_candidates.extend(cands)
        except Exception as e:
            warnings.append({"path": rel_path, "issue": f"extraction failed: {type(e).__name__}: {e}"})

    run_id = args.run_id or str(uuid.uuid4())

    out = {
        "status": "ok" if all_candidates else "skipped",
        "run_id": run_id,
        "mode": mode,
        "profile_scope": profile,
        "workspace": str(workspace_path),
        "candidate_count": len(all_candidates),
        "candidates": all_candidates,
        "warnings": warnings,
        "dry_run": args.dry_run,
        **timestamp_triple(tz_name),
    }

    if not all_candidates:
        out["reason"] = "no candidates extracted from scope"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
