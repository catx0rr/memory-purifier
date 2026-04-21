#!/usr/bin/env python3
"""Pass 2 — purifier scoring (canonicalization).

Loads prompts/purifier-pass.md as the system prompt, sends clusters
(from cluster_survivors.py) to the configured model backend, validates the
returned canonical_claims against the Pass 2 output schema, and emits the
claims as JSON for downstream assemble_artifacts.py to persist.

Persistence (writing purified-claims.jsonl and friends) is NOT this script's
job — that is Phase 5. This script only produces the validated canonical
claim payload.

Backends:
- claude-code   (default) — shells out to `claude -p`
- anthropic-sdk           — uses the anthropic Python SDK (requires ANTHROPIC_API_KEY)
- file                    — reads a canned response; used for smoke tests
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _lib.time_utils import timestamp_triple  # noqa: E402


DEFAULT_BACKEND = "claude-code"

VALID_TYPES = {
    "fact", "lesson", "decision", "commitment", "constraint", "preference",
    "identity", "relationship", "method", "procedure", "episode",
    "aspiration", "milestone", "open_question",
}
VALID_STATUSES = {"resolved", "contested", "unresolved", "superseded", "stale", "retire_candidate", "probable_duplicate"}
VALID_HOMES = {"LTMEMORY.md", "PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}
PERSONAL_ONLY_HOMES = {"HISTORY.md", "WISHES.md"}
VALID_FRESHNESS = {"fresh", "recent", "aging", "stale"}
VALID_CONFIDENCE = {"high", "medium", "low", "tentative"}
VALID_PROVENANCE_TYPES = {"direct", "inferred", "merged"}
VALID_CONTRADICTION_RELATIONS = {"contested", "stale", "superseded"}

SCORE_KEYS = [
    "semantic_cluster_confidence",
    "canonical_clarity",
    "provenance_strength",
    "contradiction_pressure",
    "freshness",
    "confidence",
    "route_fitness",
    "supersession_confidence",
]

PRIOR_CLAIMS_CAP = 50
PRIOR_PER_CLUSTER = 5
PRIOR_MIN_SCORE = 0.5
CONTRADICTION_CANDIDATES_PER_CLUSTER = 3

# v1.5.0 B1 (Contract 5) adaptive widening defaults.
#
# ``SAME_SUBJECT_BONUS_WINDOW_DAYS``: a prior whose normalized subject matches
# the current cluster AND whose ``updated_at`` falls within this window gets a
# ``SAME_SUBJECT_BONUS_SCORE`` bonus. Keeps long-horizon same-subject priors
# from being drowned by fresher but less-relevant matches.
#
# ``CONTRADICTION_PRESSURE_BONUS``: extra top-K slots granted to clusters
# showing borderline priors (scored just below min_score). Lets Pass 2
# adjudicate contradictions it might otherwise miss. Bounded by
# ``WIDENING_MAX_TOTAL_BONUS`` across the whole run so contradiction-pressure
# widening can't balloon the context.
#
# ``RECONCILIATION_*`` caps: reconciliation mode runs with wider horizon by
# design (it's an explicit reprocess). Incremental uses the tighter defaults.
SAME_SUBJECT_BONUS_WINDOW_DAYS = 30
SAME_SUBJECT_BONUS_SCORE = 1.0
CONTRADICTION_PRESSURE_BONUS = 3
WIDENING_MAX_TOTAL_BONUS = 30
RECONCILIATION_GLOBAL_CAP = 120
RECONCILIATION_PER_CLUSTER = 10

# v1.4.0: English stopwords filtered from Jaccard inputs. Small conservative
# set — avoids stemming, keeps deterministic. Dropping these prevents a
# claim scoring positively just because it shares common function words
# with the current cluster.
_STOPWORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "to", "of", "and", "or", "in", "on", "at", "for", "with", "by",
    "this", "that", "these", "those", "it", "as", "from",
})


def _snake_from_camel(claim_camel: dict) -> dict:
    """Translate a persisted camelCase claim record back to the snake_case
    shape Pass 2 expects in `prior_claims_context[]`.

    Only the fields the prompt reads are emitted; internal bookkeeping fields
    are dropped.
    """
    prov = []
    for p in (claim_camel.get("provenance") or []):
        prov.append({
            "source": p.get("source"),
            "line_span": p.get("lineSpan"),
            "captured_at": p.get("capturedAt"),
        })
    return {
        "claim_id": claim_camel.get("id"),
        "text": claim_camel.get("text"),
        "type": claim_camel.get("type"),
        "status": claim_camel.get("status"),
        "subject": claim_camel.get("subject"),
        "predicate": claim_camel.get("predicate"),
        "primary_home": claim_camel.get("primaryHome"),
        "provenance": prov,
        "updated_at": claim_camel.get("updatedAt"),
    }


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9._-]{1,}")


def _tokens(text: str) -> set:
    """Tokenize for Jaccard comparisons. v1.4.0: strips English stopwords
    so common function words don't inflate similarity."""
    if not text:
        return set()
    return {t for t in _TOKEN_RE.findall(text.lower()) if t not in _STOPWORDS}


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return (inter / union) if union else 0.0


def _cluster_query(cluster: dict) -> dict:
    """Derive a retrieval query from a cluster — subject, entities, text tokens, proposed type/home."""
    hints = cluster.get("cluster_hints") or {}
    texts = [(c.get("text") or "") for c in (cluster.get("candidates") or [])]
    shared_subject = hints.get("shared_subject") or ""
    shared_entities = [e for e in (hints.get("shared_entities") or []) if isinstance(e, str)]
    return {
        "subject": shared_subject.strip().lower(),
        "subject_tokens": _tokens(shared_subject),
        "entity_tokens": {e.lower() for e in shared_entities if len(e) >= 3},
        "text_tokens": _tokens(" ".join(texts)),
        "proposed_type": hints.get("proposed_type"),
        "proposed_home": hints.get("proposed_primary_home"),
    }


def _rank_prior_claim(query: dict, claim: dict) -> float:
    """Relevance score of `claim` for `query`. Higher = more relevant.

    v1.4.0 additions:
    - Predicate signal: +1.0 when the prior claim's predicate tokens fully
      appear in the cluster's text tokens (treated as 'exact predicate
      match' against cluster context), else +0.5 × Jaccard. Gives priors
      with the same predicate a material lift even when subject wording
      drifted.
    - Tokenizer upstream filters stopwords so common function words don't
      inflate subject/text/predicate Jaccard.
    """
    claim_subject = (claim.get("subject") or "").strip().lower()
    claim_predicate = (claim.get("predicate") or "").strip().lower()
    claim_text = claim.get("text") or ""
    claim_home = claim.get("primary_home")
    claim_type = claim.get("type")
    claim_tokens = _tokens(claim_text)
    claim_predicate_tokens = _tokens(claim_predicate)

    score = 0.0
    # Subject: exact match is a strong signal; token Jaccard is the weaker fallback.
    if query["subject"] and claim_subject == query["subject"]:
        score += 3.0
    score += 2.0 * _jaccard(query["subject_tokens"], _tokens(claim_subject))
    # Shared entity tokens that appear in the claim text.
    if query["entity_tokens"]:
        hits = sum(1 for e in query["entity_tokens"] if e in claim_tokens)
        score += 1.5 * min(1.0, hits / max(1, len(query["entity_tokens"])))
    # Same primary_home is a routing-affinity signal.
    if query["proposed_home"] and query["proposed_home"] == claim_home:
        score += 1.0
    # Same type is a weaker affinity signal.
    if query["proposed_type"] and query["proposed_type"] == claim_type:
        score += 0.5
    # Text-level Jaccard of word tokens.
    score += 1.0 * _jaccard(query["text_tokens"], claim_tokens)
    # Predicate signal against cluster text tokens (v1.4.0).
    if claim_predicate_tokens:
        if claim_predicate_tokens.issubset(query["text_tokens"]):
            score += 1.0
        else:
            score += 0.5 * _jaccard(claim_predicate_tokens, query["text_tokens"])
    return score


def _days_since(iso_ts: str) -> float:
    """Crude days-ago helper for the same-subject window check.

    Returns ``float('inf')`` on unparseable / empty timestamps so the
    window never admits undated priors. Uses UTC to avoid host-tz skew.
    """
    if not iso_ts:
        return float("inf")
    try:
        dt = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return float("inf")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 86400.0)


def retrieve_prior_claims(
    path: Path,
    clusters: list,
    cap: int = PRIOR_CLAIMS_CAP,
    per_cluster: int = PRIOR_PER_CLUSTER,
    min_score: float = PRIOR_MIN_SCORE,
    *,
    mode: str = "incremental",
    same_subject_window_days: int = SAME_SUBJECT_BONUS_WINDOW_DAYS,
    contradiction_pressure_bonus: int = CONTRADICTION_PRESSURE_BONUS,
    widening_max_total_bonus: int = WIDENING_MAX_TOTAL_BONUS,
    reconciliation_global_cap: int = RECONCILIATION_GLOBAL_CAP,
    reconciliation_per_cluster: int = RECONCILIATION_PER_CLUSTER,
) -> list:
    """Load prior purified claims, rank per-cluster with adaptive widening, cap.

    v1.4.0: per-cluster top-K union (hard floor per cluster, global cap trim).

    v1.5.0 B1 (Contract 5) additions — all still bounded, all deterministic:
    - **Same-subject bonus window.** Priors whose normalized subject matches
      the current cluster AND whose ``updated_at`` is within
      ``same_subject_window_days`` receive ``SAME_SUBJECT_BONUS_SCORE``,
      surfacing long-horizon same-subject priors that fresher generic
      matches would otherwise outrank.
    - **Contradiction-pressure widening.** Clusters with borderline priors
      (score in [min_score-0.2, min_score)) get ``contradiction_pressure_bonus``
      extra top-K slots. Global budget ``widening_max_total_bonus`` caps the
      aggregate bonus so one noisy cluster can't balloon the context.
    - **Reconciliation mode wider caps.** ``mode == "reconciliation"`` uses
      ``reconciliation_global_cap`` and ``reconciliation_per_cluster`` in
      place of the incremental caps — reconciliation is an explicit
      reprocess, so the horizon widens.
    """
    if not path.is_file():
        return []
    records: list = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not records:
        return []

    snake = [_snake_from_camel(r) for r in records]
    if not clusters:
        snake.sort(key=lambda c: c.get("updated_at") or "", reverse=True)
        return snake[:cap]

    # v1.5.0 B1: reconciliation mode uses wider caps.
    if mode == "reconciliation":
        cap = max(cap, reconciliation_global_cap)
        per_cluster = max(per_cluster, reconciliation_per_cluster)

    queries = [_cluster_query(c) for c in clusters]

    union_by_id: dict = {}
    union_max_score: dict = {}
    total_bonus_spent = 0
    for cluster, query in zip(clusters, queries):
        ranked = []
        borderline_count = 0
        for claim in snake:
            base_score = _rank_prior_claim(query, claim)

            # Same-subject bonus window — claim's subject matches query's
            # subject AND the claim was updated recently enough.
            claim_subject = str(claim.get("subject") or "").strip().lower()
            query_subject = str(query.get("subject") or "").strip().lower()
            if (
                claim_subject
                and query_subject
                and claim_subject == query_subject
                and _days_since(claim.get("updated_at")) <= same_subject_window_days
            ):
                base_score += SAME_SUBJECT_BONUS_SCORE

            # Track borderline priors so we can widen this cluster's budget.
            if (min_score - 0.2) <= base_score < min_score:
                borderline_count += 1

            if base_score >= min_score:
                ranked.append((base_score, claim.get("updated_at") or "", claim))
        ranked.sort(key=lambda x: (-x[0], _recency_neg(x[1])))

        # Contradiction-pressure widening: if this cluster had borderline
        # priors, grant a bounded top-K bonus, respecting the global budget.
        cluster_per_cluster = per_cluster
        if borderline_count > 0 and total_bonus_spent < widening_max_total_bonus:
            room = widening_max_total_bonus - total_bonus_spent
            bonus = min(contradiction_pressure_bonus, room)
            cluster_per_cluster += bonus
            total_bonus_spent += bonus

        per_cluster_top = ranked[:cluster_per_cluster]

        # Populate the cluster's contradiction_candidates with the top-N ids.
        hints = cluster.setdefault("cluster_hints", {})
        cand_ids = [
            c.get("claim_id")
            for _, _, c in per_cluster_top[:CONTRADICTION_CANDIDATES_PER_CLUSTER]
            if c.get("claim_id")
        ]
        # Preserve any existing ids (deterministic dedupe while keeping order).
        existing = hints.get("contradiction_candidates") or []
        merged: list = []
        seen: set = set()
        for cid in list(existing) + cand_ids:
            if cid and cid not in seen:
                merged.append(cid)
                seen.add(cid)
        hints["contradiction_candidates"] = merged

        # Union into global pool, remembering max score for cap-trimming.
        for score, _, claim in per_cluster_top:
            cid = claim.get("claim_id")
            if not cid:
                continue
            if cid not in union_by_id:
                union_by_id[cid] = claim
                union_max_score[cid] = score
            elif score > union_max_score[cid]:
                union_max_score[cid] = score

    if not union_by_id:
        return []

    # Cap: if union exceeds global cap, trim by lowest max_score (worst first).
    all_items = [(union_max_score[cid], union_by_id[cid]) for cid in union_by_id]
    all_items.sort(key=lambda x: (-x[0], _recency_neg(x[1].get("updated_at") or "")))
    return [claim for _, claim in all_items[:cap]]


def _recency_neg(updated_at: str) -> str:
    """Desc-sort key for iso timestamps inside a tuple-sort that's ascending."""
    return "".join(chr(0x10FFFF - ord(ch)) if ord(ch) < 0x10FFFF else ch for ch in (updated_at or ""))


def _is_numeric(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validate_claims(
    claims_obj,
    input_clusters: list,
    run_id: str,
    profile_scope: str,
    prior_claim_ids: set,
) -> tuple:
    errors: list = []

    if not isinstance(claims_obj, dict):
        return False, ["output is not a JSON object"]

    if claims_obj.get("run_id") != run_id:
        errors.append(f"run_id mismatch: expected {run_id!r}, got {claims_obj.get('run_id')!r}")

    claims = claims_obj.get("canonical_claims")
    if not isinstance(claims, list):
        return False, errors + ["canonical_claims field missing or not a list"]

    input_cluster_ids = {c["cluster_id"] for c in input_clusters}
    seen_cluster_ids: set = set()

    cluster_sources: dict = {}
    for c in input_clusters:
        sources: set = set()
        for cand in (c.get("candidates") or []):
            for ref in (cand.get("source_refs") or []):
                src = ref.get("source")
                if src:
                    sources.add(src)
        cluster_sources[c["cluster_id"]] = sources

    for i, claim in enumerate(claims):
        prefix = f"canonical_claims[{i}]"
        if not isinstance(claim, dict):
            errors.append(f"{prefix} is not an object")
            continue

        cluster_id = claim.get("source_cluster_id")
        if cluster_id not in input_cluster_ids:
            errors.append(f"{prefix} source_cluster_id={cluster_id!r} not in input clusters")
        if cluster_id in seen_cluster_ids:
            errors.append(f"{prefix} duplicate source_cluster_id: {cluster_id}")
        seen_cluster_ids.add(cluster_id)

        claim_id = claim.get("claim_id")
        if claim_id != "<new>" and claim_id not in prior_claim_ids:
            errors.append(
                f"{prefix} claim_id={claim_id!r} is not '<new>' and not in prior_claims_context"
            )

        scores = claim.get("scores", {})
        if not isinstance(scores, dict):
            errors.append(f"{prefix} scores is not an object")
            scores = {}
        for k in SCORE_KEYS:
            v = scores.get(k)
            if not _is_numeric(v):
                errors.append(f"{prefix} scores.{k} missing or not numeric")
                continue
            if not (0.0 <= float(v) <= 1.0):
                errors.append(f"{prefix} scores.{k}={v} out of [0,1]")

        canonical = claim.get("canonical", {})
        if not isinstance(canonical, dict):
            errors.append(f"{prefix} canonical is not an object")
            continue

        ctype = canonical.get("type")
        if ctype not in VALID_TYPES:
            errors.append(f"{prefix} canonical.type={ctype!r} not in valid set")

        cstatus = canonical.get("status")
        if cstatus not in VALID_STATUSES:
            errors.append(f"{prefix} canonical.status={cstatus!r} not in valid set")

        chome = canonical.get("primary_home")
        if chome not in VALID_HOMES:
            errors.append(f"{prefix} canonical.primary_home={chome!r} not in valid set")
        if chome in PERSONAL_ONLY_HOMES and profile_scope != "personal":
            errors.append(
                f"{prefix} primary_home={chome!r} only allowed on personal profile (got {profile_scope!r})"
            )

        ctext = canonical.get("text")
        if not isinstance(ctext, str) or not ctext.strip():
            errors.append(f"{prefix} canonical.text missing or empty")

        stags = canonical.get("secondary_tags")
        if stags is None:
            canonical["secondary_tags"] = []
        elif not isinstance(stags, list):
            errors.append(f"{prefix} canonical.secondary_tags must be a list")

        prov = claim.get("provenance", [])
        if not isinstance(prov, list) or not prov:
            errors.append(f"{prefix} provenance missing or empty")
        else:
            allowed_sources = cluster_sources.get(cluster_id, set())
            for j, p in enumerate(prov):
                if not isinstance(p, dict):
                    errors.append(f"{prefix}.provenance[{j}] is not an object")
                    continue
                src = p.get("source")
                if src and src not in allowed_sources:
                    errors.append(
                        f"{prefix}.provenance[{j}] source={src!r} not traceable to cluster candidates"
                    )
                ptype = p.get("type")
                if ptype is not None and ptype not in VALID_PROVENANCE_TYPES:
                    errors.append(
                        f"{prefix}.provenance[{j}] type={ptype!r} not in {sorted(VALID_PROVENANCE_TYPES)}"
                    )

        contras = claim.get("contradictions", [])
        if contras and isinstance(contras, list):
            for j, c in enumerate(contras):
                if not isinstance(c, dict):
                    errors.append(f"{prefix}.contradictions[{j}] is not an object")
                    continue
                if not c.get("competing_claim_id") and not c.get("competing_text"):
                    errors.append(
                        f"{prefix}.contradictions[{j}] missing both competing_claim_id and competing_text"
                    )
                relation = c.get("relation")
                if relation is not None and relation not in VALID_CONTRADICTION_RELATIONS:
                    errors.append(
                        f"{prefix}.contradictions[{j}] relation={relation!r} not in {sorted(VALID_CONTRADICTION_RELATIONS)}"
                    )

        for field in ("supersedes", "superseded_by"):
            val = claim.get(field)
            if val is not None and not isinstance(val, list):
                errors.append(f"{prefix} {field} must be a list")

        for field, allowed in (
            ("freshness_posture", VALID_FRESHNESS),
            ("confidence_posture", VALID_CONFIDENCE),
        ):
            val = claim.get(field)
            if val is not None and val not in allowed:
                errors.append(f"{prefix} {field}={val!r} not in {sorted(allowed)}")

    missing_clusters = input_cluster_ids - seen_cluster_ids
    if missing_clusters:
        sample = sorted(missing_clusters)[:5]
        errors.append(f"{len(missing_clusters)} input cluster_id(s) missing from output; sample: {sample}")

    return len(errors) == 0, errors


def _fixture_lookup(fixture_dir: Path, fixture_file: Path, input_payload: dict) -> Path:
    if fixture_file and fixture_file.is_file():
        return fixture_file
    key = hashlib.sha256(
        json.dumps(input_payload, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:16]
    specific = fixture_dir / f"purifier-{key}.json"
    if specific.is_file():
        return specific
    default = fixture_dir / "purifier-default.json"
    if default.is_file():
        return default
    raise FileNotFoundError(
        f"no fixture at {specific} or {default} (hash-key: {key})"
    )


def _approximate_tokens(text: str) -> int:
    """Conservative char-to-token heuristic. See score_promotion.py for rationale."""
    if not text:
        return 0
    return max(1, len(text) // 4)


def _usage_unavailable() -> dict:
    return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "source": "unavailable"}


def _usage_approximate(prompt_text: str, completion_text: str) -> dict:
    p = _approximate_tokens(prompt_text)
    c = _approximate_tokens(completion_text)
    if p == 0 and c == 0:
        return _usage_unavailable()
    return {"prompt_tokens": p, "completion_tokens": c, "total_tokens": p + c, "source": "approximate"}


def _usage_exact(input_tokens: int, output_tokens: int) -> dict:
    return {
        "prompt_tokens": int(input_tokens),
        "completion_tokens": int(output_tokens),
        "total_tokens": int(input_tokens) + int(output_tokens),
        "source": "exact",
    }


def _merge_usage(a: dict, b: dict) -> dict:
    src_rank = {"exact": 0, "approximate": 1, "unavailable": 2}
    a_src = a.get("source", "unavailable")
    b_src = b.get("source", "unavailable")
    merged_src = a_src if src_rank[a_src] >= src_rank[b_src] else b_src
    return {
        "prompt_tokens": int(a.get("prompt_tokens") or 0) + int(b.get("prompt_tokens") or 0),
        "completion_tokens": int(a.get("completion_tokens") or 0) + int(b.get("completion_tokens") or 0),
        "total_tokens": int(a.get("total_tokens") or 0) + int(b.get("total_tokens") or 0),
        "source": merged_src,
    }


def invoke_backend(
    backend: str,
    prompt_file: Path,
    input_payload: dict,
    fixture_dir: Path = None,
    fixture_file: Path = None,
    model: str = None,
    max_tokens: int = None,
    timeout: int = 300,
) -> dict:
    """Invoke the model backend. Returns {"raw": <text>, "usage": <token_usage>}."""
    if backend == "file":
        if not (fixture_dir or fixture_file):
            raise ValueError("backend=file requires --fixture-dir or --fixture-file")
        path = _fixture_lookup(
            Path(fixture_dir) if fixture_dir else Path(),
            Path(fixture_file) if fixture_file else None,
            input_payload,
        )
        return {"raw": path.read_text(), "usage": _usage_unavailable()}

    if backend == "claude-code":
        cmd = ["claude", "-p"]
        if model:
            cmd += ["--model", model]
        system_text = prompt_file.read_text()
        user_text = json.dumps(input_payload, indent=2, ensure_ascii=False)
        combined = (
            f"{system_text}\n\n---\n\nInput payload:\n\n```json\n{user_text}\n```\n\n"
            "Respond with the JSON envelope only."
        )
        proc = subprocess.run(
            cmd, input=combined, text=True, capture_output=True, timeout=timeout
        )
        if proc.returncode != 0:
            raise RuntimeError(f"claude-code backend failed (rc={proc.returncode}): {proc.stderr.strip()}")
        return {"raw": proc.stdout, "usage": _usage_approximate(combined, proc.stdout)}

    if backend == "anthropic-sdk":
        try:
            import anthropic
        except ImportError as e:
            raise RuntimeError("anthropic package not installed; pip install anthropic") from e
        client = anthropic.Anthropic()
        system_text = prompt_file.read_text()
        user_text = json.dumps(input_payload, indent=2, ensure_ascii=False)
        resp = client.messages.create(
            model=model or "claude-opus-4-7",
            max_tokens=max_tokens or 8192,
            system=system_text,
            messages=[{"role": "user", "content": user_text}],
        )
        raw = resp.content[0].text
        u = getattr(resp, "usage", None)
        if u is not None:
            usage = _usage_exact(
                getattr(u, "input_tokens", 0) or 0,
                getattr(u, "output_tokens", 0) or 0,
            )
        else:
            usage = _usage_approximate(system_text + user_text, raw)
        return {"raw": raw, "usage": usage}

    raise ValueError(f"unknown backend: {backend}")


def extract_json(raw: str):
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            return json.loads(raw[start:end + 1])
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description="Pass 2 — purifier scoring (canonicalization).")
    ap.add_argument("--clusters", required=True, help="Clusters JSON (from cluster_survivors.py) or '-' for stdin")
    ap.add_argument("--prompt", help="Path to prompts/purifier-pass.md (default: resolved from script location)")
    ap.add_argument("--workspace", help="Workspace root override")
    ap.add_argument("--runtime-dir", help="Runtime dir override (default: <workspace>/runtime)")
    ap.add_argument(
        "--prior-claims",
        help="Path to prior purified-claims.jsonl; if omitted and mode=reconciliation, auto-discover from runtime-dir",
    )
    ap.add_argument("--prior-claims-cap", type=int, default=PRIOR_CLAIMS_CAP, help="Global cap on prior claims returned to Pass 2 (final trim after per-cluster union)")
    ap.add_argument("--prior-per-cluster", type=int, default=PRIOR_PER_CLUSTER, help="Per-cluster top-K taken before global union (guarantees small clusters don't starve)")
    ap.add_argument("--prior-min-score", type=float, default=PRIOR_MIN_SCORE, help="Minimum rank score below which a prior claim is filtered out even if the pool is under-cap")
    ap.add_argument(
        "--same-subject-window-days",
        type=int,
        default=SAME_SUBJECT_BONUS_WINDOW_DAYS,
        help="v1.5.0 B1: priors with matching subject whose updated_at is within this window "
             "receive a bonus score, surfacing long-horizon same-subject matches.",
    )
    ap.add_argument(
        "--contradiction-pressure-bonus",
        type=int,
        default=CONTRADICTION_PRESSURE_BONUS,
        help="v1.5.0 B1: extra top-K slots granted to clusters with borderline priors "
             "(scored just below min_score). Bounded by a global budget so contradiction "
             "pressure can't balloon the context.",
    )
    ap.add_argument("--backend", default=None, help="Model backend: claude-code | anthropic-sdk | file")
    ap.add_argument("--model", help="Model override")
    ap.add_argument("--max-tokens", type=int, help="Max output tokens")
    ap.add_argument("--fixture-dir", help="Fixture directory (backend=file)")
    ap.add_argument("--fixture-file", help="Explicit fixture file path (backend=file)")
    ap.add_argument("--retry", type=int, default=1, help="Retries on validation failure (default: 1)")
    ap.add_argument("--timeout", type=int, default=300, help="Backend call timeout in seconds")
    ap.add_argument("--timezone", help="IANA timezone name (default: from clusters or Asia/Manila)")
    ap.add_argument("--dry-run", action="store_true", help="Validate only; do not write failure records")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="v1.5.0 A3 (Contract 5): split clusters into deterministic chunks of this size "
             "and invoke the backend once per chunk. Each chunk receives the FULL prior_claims_context "
             "— retrieval runs once per run, not per batch. 0 = monolithic call (v1.4.0 behavior). "
             "Chunk boundaries are sorted by cluster_id for rerun stability.",
    )
    ap.add_argument(
        "--oversized-hard-cap",
        type=int,
        default=10,
        help="v1.5.0 A3: maximum number of batches processed per run. Input larger than "
             "batch_size × hard_cap triggers the --oversized-strategy.",
    )
    ap.add_argument(
        "--oversized-strategy",
        default="bounded_batches",
        choices=("bounded_batches", "split_and_queue"),
        help="v1.5.0 A3: oversized handling. `bounded_batches` processes up to hard_cap batches "
             "and reports partial. `split_and_queue` also writes the remainder to a queue file.",
    )

    args = ap.parse_args()

    if args.clusters == "-":
        clusters_obj = json.load(sys.stdin)
    else:
        clusters_obj = json.loads(Path(args.clusters).expanduser().read_text())

    tz_name = args.timezone or clusters_obj.get("timezone") or "Asia/Manila"

    if clusters_obj.get("status") != "ok":
        out = {
            "status": "skipped",
            "reason": f"clusters status is {clusters_obj.get('status')!r}",
            "pass": "purifier",
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    run_id = clusters_obj["run_id"]
    clusters = clusters_obj.get("clusters", [])
    mode = clusters_obj.get("mode") or "incremental"
    profile_scope = clusters_obj.get("profile_scope") or "business"

    if not clusters:
        out = {
            "status": "skipped",
            "reason": "no clusters to score",
            "run_id": run_id,
            "pass": "purifier",
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    script_dir = Path(__file__).resolve().parent
    prompt_path = Path(args.prompt) if args.prompt else (script_dir.parent / "prompts" / "purifier-pass.md")
    if not prompt_path.is_file():
        out = {
            "status": "error",
            "error": f"prompt file not found: {prompt_path}",
            "pass": "purifier",
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    backend = args.backend or os.environ.get("MEMORY_PURIFIER_BACKEND") or DEFAULT_BACKEND

    workspace_hint = args.workspace or clusters_obj.get("workspace") or os.environ.get("WORKSPACE")
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")
    locks_dir = runtime_dir / "locks"

    prior_path = None
    if args.prior_claims:
        prior_path = Path(args.prior_claims).expanduser()
    elif mode == "reconciliation":
        prior_path = runtime_dir / "purified-claims.jsonl"
    prior_claims_context = retrieve_prior_claims(
        prior_path,
        clusters,
        cap=args.prior_claims_cap,
        per_cluster=args.prior_per_cluster,
        min_score=args.prior_min_score,
        mode=mode,
        same_subject_window_days=args.same_subject_window_days,
        contradiction_pressure_bonus=args.contradiction_pressure_bonus,
    ) if prior_path else []
    prior_claim_ids = {c["claim_id"] for c in prior_claims_context if c.get("claim_id")}

    # v1.5.0 A3 (Contract 5): sort clusters deterministically before batching
    # so reruns produce identical chunk boundaries.
    sorted_clusters = sorted(clusters, key=lambda c: str(c.get("cluster_id", "")))
    if args.batch_size and args.batch_size > 0:
        batch_size = args.batch_size
        chunks = [sorted_clusters[i:i + batch_size] for i in range(0, len(sorted_clusters), batch_size)]
    else:
        batch_size = len(sorted_clusters) or 1
        chunks = [sorted_clusters] if sorted_clusters else []

    pending_clusters: list = []
    oversized_truncated = False
    if args.oversized_hard_cap > 0 and len(chunks) > args.oversized_hard_cap:
        processed = chunks[: args.oversized_hard_cap]
        remainder = [cl for chunk in chunks[args.oversized_hard_cap:] for cl in chunk]
        pending_clusters = remainder
        chunks = processed
        oversized_truncated = True

    base_payload = {
        "run_id": run_id,
        "mode": mode,
        "profile_scope": profile_scope,
        "prior_claims_context": prior_claims_context,
    }

    all_claims: list = []
    last_errors: list = []
    raw_response = None
    claims_obj = None
    attempts = 0
    total_usage = _usage_unavailable()
    batch_failed = False

    for chunk_idx, chunk in enumerate(chunks):
        input_payload = {**base_payload, "clusters": chunk}
        chunk_cluster_ids = {str(cl.get("cluster_id", "")) for cl in chunk}
        chunk_claims = None

        for _ in range(args.retry + 1):
            attempts += 1
            try:
                resp = invoke_backend(
                    backend=backend,
                    prompt_file=prompt_path,
                    input_payload=input_payload,
                    fixture_dir=args.fixture_dir,
                    fixture_file=args.fixture_file,
                    model=args.model,
                    max_tokens=args.max_tokens,
                    timeout=args.timeout,
                )
                raw_response = resp["raw"]
                total_usage = _merge_usage(total_usage, resp.get("usage") or _usage_unavailable())
            except Exception as e:
                last_errors = [f"batch {chunk_idx}: backend invocation failed: {type(e).__name__}: {e}"]
                continue

            try:
                parsed = extract_json(raw_response)
            except Exception as e:
                last_errors = [f"batch {chunk_idx}: JSON parse failed: {type(e).__name__}: {e}"]
                continue

            # Defensive filter: file-backend fixtures may carry the full-run
            # canonical_claims set; keep only the ones that map to THIS chunk.
            if args.batch_size and isinstance(parsed, dict) and isinstance(parsed.get("canonical_claims"), list):
                parsed["canonical_claims"] = [
                    c for c in parsed["canonical_claims"]
                    if str(c.get("source_cluster_id", "")) in chunk_cluster_ids
                ]

            ok, errors = validate_claims(parsed, chunk, run_id, profile_scope, prior_claim_ids)
            if ok:
                chunk_claims = parsed.get("canonical_claims", [])
                last_errors = []
                break
            last_errors = [f"batch {chunk_idx}: {e}" for e in errors]

        if chunk_claims is None:
            batch_failed = True
            break
        all_claims.extend(chunk_claims)

    if not batch_failed and chunks:
        claims_obj = {"run_id": run_id, "canonical_claims": all_claims}

    # Restore full-input reference for downstream metadata fields.
    input_payload = {**base_payload, "clusters": clusters}

    if claims_obj is None:
        fail_path = locks_dir / f"purifier-failed-purifier-{run_id}.json"
        fail_payload = {
            "run_id": run_id,
            "pass": "purifier",
            "attempts": attempts,
            "errors": last_errors,
            "raw_response": raw_response,
            "input_payload": input_payload,
            **timestamp_triple(tz_name),
        }
        if not args.dry_run:
            locks_dir.mkdir(parents=True, exist_ok=True)
            fail_path.write_text(json.dumps(fail_payload, indent=2, ensure_ascii=False))

        out = {
            "status": "partial_failure",
            "run_id": run_id,
            "pass": "purifier",
            "backend": backend,
            "attempts": attempts,
            "errors": last_errors,
            "failed_record_path": str(fail_path) if not args.dry_run else None,
            "token_usage": total_usage,
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    canonical_claims = claims_obj.get("canonical_claims", [])

    home_stats: dict = {h: 0 for h in VALID_HOMES}
    status_stats: dict = {s: 0 for s in VALID_STATUSES}
    supersession_count = 0
    contradiction_count = 0
    for claim in canonical_claims:
        home = claim.get("canonical", {}).get("primary_home")
        if home in home_stats:
            home_stats[home] += 1
        st = claim.get("canonical", {}).get("status")
        if st in status_stats:
            status_stats[st] += 1
        if claim.get("supersedes"):
            supersession_count += 1
        if claim.get("contradictions"):
            contradiction_count += 1

    # v1.5.0 A3: oversized metadata; downgrade to partial_failure when truncated.
    oversized_status = None
    partial_batches = None
    if oversized_truncated:
        partial_batches = {
            "pending_cluster_count": len(pending_clusters),
            "pending_cluster_ids": [cl.get("cluster_id") for cl in pending_clusters],
            "strategy": args.oversized_strategy,
        }
        if args.oversized_strategy == "split_and_queue" and not args.dry_run:
            queue_path = runtime_dir / f"pending-clusters-{run_id}.jsonl"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            with queue_path.open("w", encoding="utf-8") as fh:
                for cl in pending_clusters:
                    fh.write(json.dumps(cl, ensure_ascii=False) + "\n")
            partial_batches["queue_path"] = str(queue_path)
        oversized_status = "partial_failure"

    out = {
        "status": oversized_status or "ok",
        "run_id": run_id,
        "pass": "purifier",
        "backend": backend,
        "attempts": attempts,
        "mode": mode,
        "profile_scope": profile_scope,
        "cluster_count": len(clusters),
        "claim_count": len(canonical_claims),
        "home_stats": home_stats,
        "status_stats": status_stats,
        "supersession_count": supersession_count,
        "contradiction_count": contradiction_count,
        "prior_claims_context_used": len(prior_claims_context),
        "canonical_claims": canonical_claims,
        "token_usage": total_usage,
        "batch_count": len(chunks),
        "batch_size": batch_size,
        "partialBatches": partial_batches,
        "dry_run": args.dry_run,
        **timestamp_triple(tz_name),
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
