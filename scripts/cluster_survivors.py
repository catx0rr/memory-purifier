#!/usr/bin/env python3
"""Deterministically cluster Pass 1 survivors into groups for Pass 2.

Takes the `survivors[]` array produced by score_promotion.py (candidates with
verdicts in {promote, compress, merge}) and groups semantically-overlapping
ones into clusters via union-find on explicit merge hints. Each cluster gets a
stable cluster_id plus cluster_hints that bias — but don't override — Pass 2's
canonicalization judgment.

Emits one JSON object to stdout; input shape for prompts/purifier-pass.md §2.
"""

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


TYPE_TO_PRIMARY_HOME = {
    "fact": "LTMEMORY.md",
    "lesson": "LTMEMORY.md",
    "preference": "LTMEMORY.md",
    "constraint": "LTMEMORY.md",
    "commitment": "LTMEMORY.md",
    "identity": "LTMEMORY.md",
    "relationship": "LTMEMORY.md",
    "decision": "LTMEMORY.md",
    "method": "PLAYBOOKS.md",
    "procedure": "PLAYBOOKS.md",
    "episode": "EPISODES.md",
    "milestone": "HISTORY.md",
    "aspiration": "WISHES.md",
    "open_question": "LTMEMORY.md",
}

ENTITY_PATTERN = re.compile(r"\b[A-Z][\w./-]{2,}\b")
STOPWORDS = {
    "The", "When", "This", "That", "These", "Those", "What", "How", "Why",
    "Where", "Who", "Operator", "User",
}


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


class UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _extract_entities(text: str) -> list:
    return [m for m in ENTITY_PATTERN.findall(text) if m not in STOPWORDS]


def _shared_entities(candidates: list) -> list:
    if not candidates:
        return []
    per_cand = [set(_extract_entities(c.get("text", ""))) for c in candidates]
    if len(per_cand) == 1:
        return sorted(per_cand[0])
    shared = set.intersection(*per_cand)
    return sorted(shared)


def _majority_type_hint(candidates: list) -> str:
    hints = [c.get("type_hint") for c in candidates if c.get("type_hint")]
    if not hints:
        return "unknown"
    counter = Counter(hints)
    return counter.most_common(1)[0][0]


def _cluster_id(candidate_ids: list) -> str:
    key = "|".join(sorted(candidate_ids))
    return "clust-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def _build_cluster_hint_block(candidates: list) -> dict:
    proposed_type = _majority_type_hint(candidates)
    proposed_home = TYPE_TO_PRIMARY_HOME.get(proposed_type)

    subjects = Counter()
    for c in candidates:
        for src in (c.get("source_refs") or []):
            sub = src.get("source")
            if sub:
                subjects[sub] += 1
    shared_subject = subjects.most_common(1)[0][0] if subjects else None

    return {
        "shared_entities": _shared_entities(candidates),
        "shared_subject": shared_subject,
        "proposed_type": proposed_type,
        "proposed_primary_home": proposed_home,
        "contradiction_candidates": [],
    }


def _to_cluster_candidate_shape(survivor: dict) -> dict:
    """Shape per prompts/purifier-pass.md §2 clusters[].candidates[] entry."""
    return {
        "candidate_id": survivor["candidate_id"],
        "text": survivor.get("text"),
        "type_hint": survivor.get("type_hint"),
        "source_refs": survivor.get("source_refs") or [],
        "pass_1_verdict": survivor.get("verdict"),
        "pass_1_rationale": survivor.get("rationale"),
        "compress_target": survivor.get("compress_target"),
    }


def build_clusters(survivors: list) -> list:
    if not survivors:
        return []

    uf = UnionFind([s["candidate_id"] for s in survivors])
    by_id = {s["candidate_id"]: s for s in survivors}

    for s in survivors:
        my_id = s["candidate_id"]
        for other_id in (s.get("merge_candidate_ids") or []):
            if other_id in by_id:
                uf.union(my_id, other_id)

    groups: dict = {}
    for cid in list(by_id.keys()):
        root = uf.find(cid)
        groups.setdefault(root, []).append(by_id[cid])

    clusters: list = []
    for members in groups.values():
        members_sorted = sorted(members, key=lambda m: m["candidate_id"])
        ids = [m["candidate_id"] for m in members_sorted]
        cluster = {
            "cluster_id": _cluster_id(ids),
            "candidates": [_to_cluster_candidate_shape(m) for m in members_sorted],
            "cluster_hints": _build_cluster_hint_block(members_sorted),
        }
        clusters.append(cluster)

    clusters.sort(key=lambda c: c["cluster_id"])
    return clusters


def _load_pass1(arg: str) -> dict:
    if arg == "-":
        return json.load(sys.stdin)
    return json.loads(Path(arg).expanduser().read_text())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cluster Pass 1 survivors into groups for Pass 2.",
    )
    ap.add_argument("--pass1", required=True, help="Pass 1 result JSON (from score_promotion.py) or '-' for stdin")
    ap.add_argument("--timezone", help="IANA timezone name (default: from pass1 or Asia/Manila)")
    ap.add_argument("--dry-run", action="store_true", help="Read-only; echoed in output for chain compatibility")

    args = ap.parse_args()

    try:
        pass1 = _load_pass1(args.pass1)
    except Exception as e:
        out = {
            "status": "error",
            "error": f"failed to load pass1 result: {type(e).__name__}: {e}",
            "dry_run": args.dry_run,
            **timestamp_triple(args.timezone or "Asia/Manila"),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    tz_name = args.timezone or pass1.get("timezone") or "Asia/Manila"

    if pass1.get("status") != "ok":
        out = {
            "status": "skipped",
            "reason": f"pass1 status is {pass1.get('status')!r}",
            "pass1_status": pass1.get("status"),
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    run_id = pass1.get("run_id")
    survivors = pass1.get("survivors") or []
    mode = pass1.get("mode") if isinstance(pass1.get("mode"), str) else None
    profile_scope = pass1.get("profile_scope") if isinstance(pass1.get("profile_scope"), str) else None

    clusters = build_clusters(survivors)

    status = "ok" if clusters else "skipped"

    out = {
        "status": status,
        "run_id": run_id,
        "mode": mode,
        "profile_scope": profile_scope,
        "survivor_count": len(survivors),
        "cluster_count": len(clusters),
        "clusters": clusters,
        "dry_run": args.dry_run,
        **timestamp_triple(tz_name),
    }

    if status == "skipped":
        out["reason"] = "no survivors to cluster"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
