#!/usr/bin/env python3
"""Persist Pass 2 canonical_claims to the four machine artifacts.

Reads score_purifier.py output (snake_case canonical_claims) and produces:
- <runtime>/purified-claims.jsonl         (full-state, not append-log)
- <runtime>/purified-contradictions.jsonl (full-state)
- <runtime>/purified-entities.json        (entity/alias map)
- <runtime>/purified-routes.json          (primary_home → claim_ids)

Translations snake_case→camelCase per references/prompt-contracts.md §6.
Stable claim_id hashing for '<new>' placeholders ensures rerunning the same
inputs never multiplies claims. All writes are atomic (temp-file + rename).

Writing purified-manifest.json is NOT this script's job — that's write_manifest.py.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _lib.time_utils import timestamp_triple  # noqa: E402


def _stable_claim_id(canonical: dict) -> str:
    """Hash canonical subject/predicate/text/primary_home into a stable id.

    Idempotent across reruns: same canonical content yields the same id, so
    re-running unchanged clusters does not produce duplicate claim records.
    """
    subject = canonical.get("subject") or ""
    predicate = canonical.get("predicate") or ""
    text = canonical.get("text") or ""
    home = canonical.get("primary_home") or ""
    key = f"{subject}|{predicate}|{text}|{home}"
    return "cl-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def _translate_provenance(prov_snake: list) -> list:
    return [
        {
            "source": p.get("source"),
            "lineSpan": p.get("line_span"),
            "type": p.get("type"),
            "capturedAt": p.get("captured_at"),
        }
        for p in (prov_snake or [])
    ]


def _translate_contradictions_field(contras_snake: list, run_id: str) -> list:
    return [
        {
            "competingClaimId": c.get("competing_claim_id"),
            "competingText": c.get("competing_text"),
            "relation": c.get("relation"),
            "flaggedInRunId": run_id,
        }
        for c in (contras_snake or [])
    ]


# v1.4.0: conservative deterministic normalization for reuse-key matching.
# Handles the most common rewording drift (articles, trivial plural,
# limited verb morphology) without bringing in a stemmer library or
# aggressive suffix table.

_LEADING_ARTICLES = ("the ", "a ", "an ")

# Small predicate morphology table: map a limited set of suffix forms back
# to their stem when the stem is >= 3 chars. Deterministic, reviewable.
# Not a general stemmer — only catches the most frequent drift patterns
# (third-person-s, present participle, simple past).
_PRED_SUFFIX_RULES = [
    ("ing", 3),   # preferring -> prefer, running -> run (stem >= 3 chars)
    ("ed", 3),    # preferred -> prefer, noted -> note
    ("s", 4),     # prefers -> prefer; stem floor 4 chars to avoid eating legitimate short words
]


def _normalize_subject(subject: str) -> str:
    """Lowercase + trim + strip leading article + simple plural fold."""
    s = (subject or "").strip().lower()
    if not s:
        return ""
    for art in _LEADING_ARTICLES:
        if s.startswith(art):
            s = s[len(art):].strip()
            break
    # Plural fold: drop trailing 's' only when the stem (len - 1) is >= 3 chars,
    # the char before 's' is a non-s consonant, and the word doesn't end in 'ss'.
    # Examples: "cats" -> "cat" (stem 3 chars, 't' consonant, not 'ss'); "address"
    # stays ('ss' ending); "gas" stays (stem only 2 chars); "cities" stays
    # (ends in vowel + s — rejected by the aeiou check).
    if (len(s) >= 4
            and s.endswith("s")
            and not s.endswith("ss")
            and s[-2] not in "aeiou"):
        s = s[:-1]
    return s


def _normalize_predicate(predicate: str) -> str:
    """Lowercase + trim + limited morphology fold."""
    p = (predicate or "").strip().lower()
    if not p:
        return ""
    for suffix, stem_floor in _PRED_SUFFIX_RULES:
        if p.endswith(suffix) and len(p) - len(suffix) >= stem_floor:
            stem = p[: -len(suffix)]
            # Reject if removing the suffix leaves trailing punctuation or
            # something clearly not a verb stem.
            if stem and stem[-1].isalpha():
                return stem
    return p


def _normalize_home(home: str) -> str:
    """Trim only — homes are controlled enum values, case preserved."""
    return (home or "").strip()


def _normalize_reuse_key(subject: str, predicate: str, home: str) -> tuple:
    """Public spec-named normalizer: returns the canonical 3-tuple
    (normalized_subject, normalized_predicate, normalized_home) for
    semantic-reuse matching. Delegates to the per-field helpers."""
    return (
        _normalize_subject(subject),
        _normalize_predicate(predicate),
        _normalize_home(home),
    )


def _reuse_key(claim_like: dict, home_field: str) -> tuple:
    """Internal: build the 3-tuple reuse key from either a snake-case
    canonical or a camelCase prior claim. `home_field` is `"primary_home"`
    for snake-case input, `"primaryHome"` for persisted prior claims.
    Wraps `_normalize_reuse_key` for dict-shaped callers."""
    return _normalize_reuse_key(
        claim_like.get("subject"),
        claim_like.get("predicate"),
        claim_like.get(home_field),
    )


def _parse_iso_updated_at(value) -> float:
    """Best-effort parse of an ISO 8601 updatedAt into an epoch float for
    deterministic most-recent-wins comparison. Returns 0.0 on failure."""
    if not value or not isinstance(value, str):
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _semantic_reuse_match(canonical: dict, prior_claims: list) -> str:
    """Find an active prior claim whose normalized (subject, predicate,
    primary_home) matches this new canonical's triple. Used to reuse a
    stable id across text rewordings.

    v1.4.0:
    - Normalization handles leading articles, trivial plural fold, and a
      small predicate morphology table (-ing / -ed / -s with stem floors).
    - On multi-match, picks the **most-recently-updated** active prior by
      `updatedAt`. Ties broken by `id` lexicographic. Stable across reruns
      since updatedAt is monotonic.

    Excludes priors with status `superseded` / `retire_candidate` / `stale`
    so reuse never resurrects a retired id.
    """
    new_key = _reuse_key(canonical, "primary_home")
    if not all(new_key):  # any of subject/predicate/home empty → skip reuse
        return None

    matches = []
    for prior in prior_claims:
        if prior.get("status") in {"superseded", "retire_candidate", "stale"}:
            continue
        if _reuse_key(prior, "primaryHome") == new_key:
            matches.append(prior)

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0].get("id")

    # Multi-match: most-recently-updated active prior wins. Stable tiebreak.
    matches.sort(
        key=lambda p: (-_parse_iso_updated_at(p.get("updatedAt")), str(p.get("id") or "")),
    )
    return matches[0].get("id")


# v1.5.0 B2 (Contract 4) — probable-duplicate detection constants.
#
# Graded similarity fires only AFTER ``_semantic_reuse_match`` returns None
# (no exact normalized-triple match). The realistic case this catches is
# paraphrase drift: same subject + same home + strong text overlap + a
# predicate that differs even after normalization (e.g., "moves" vs
# "relocates"). Weights balance so that case hits 0.80 when text-Jaccard
# is high and falls below when text overlap is weak — exactly the
# "uncertain enough to flag, clear enough to keep the new claim" zone.
#
# Wiki reconciler owns final collapse; purifier only surfaces the
# candidate pair for downstream attention.
_PROBABLE_DUP_THRESHOLD = 0.80
_PROBABLE_DUP_WEIGHT_SUBJECT = 0.4
_PROBABLE_DUP_WEIGHT_PREDICATE = 0.2
_PROBABLE_DUP_WEIGHT_HOME = 0.2
_PROBABLE_DUP_WEIGHT_TEXT = 0.2
# Object-match bonus (Contract 4 §33): applied ONLY when both sides carry a
# non-empty, normalized-matching object. Pushes borderline matches clearly
# over threshold when structured object match is present.
_PROBABLE_DUP_OBJECT_BONUS = 0.1

# Stopwords filtered out of text-Jaccard to keep the signal about content,
# not function words. Kept tight to avoid over-eager duplicate collapse on
# short claim texts.
_DUP_STOPWORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been",
    "to", "of", "and", "or", "in", "on", "at", "for", "with", "by",
    "this", "that", "it", "as", "from",
})

_DUP_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _dup_tokens(text: str) -> set:
    """Lowercase word tokens with stopword filter — for text-Jaccard."""
    if not text:
        return set()
    return {t for t in _DUP_TOKEN_RE.findall(str(text).lower()) if t not in _DUP_STOPWORDS}


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    union = a | b
    return (len(a & b) / len(union)) if union else 0.0


def _probable_duplicate_match(canonical: dict, prior_claims: list) -> dict | None:
    """Graded similarity check beyond exact normalized-triple match.

    Scope rule (strict order per Contract 4):
      1. Same primary_home first — cross-home priors are skipped.
      2. Normalize subject + predicate.
      3. Normalize object/value when present on both (Contract 4 §scope-order).
         A matching object is a strong duplicate signal; its absence on
         either side contributes nothing (neutral, not penalty).
      4. Compute composite score; return best match above threshold.

    Returns ``{"claim_id", "score", "dominant_signal"}`` on match, else None.
    ``dominant_signal`` is the largest-weighted signal in this specific
    scoring — useful for the ``duplicateReason`` metadata field.
    """
    canonical_home = _normalize_home(canonical.get("primary_home"))
    canonical_subject = _normalize_subject(canonical.get("subject") or "")
    canonical_predicate = _normalize_predicate(canonical.get("predicate") or "")
    canonical_object = _normalize_subject(canonical.get("object") or "")  # subject rules work for objects
    canonical_text = str(canonical.get("text") or "")
    canonical_text_tokens = _dup_tokens(canonical_text)

    if not canonical_home or not canonical_subject or not canonical_predicate:
        return None

    best_score = 0.0
    best_prior = None
    best_signal = ""

    for prior in prior_claims:
        # Skip inactive priors so resurrected/retired claims don't steal new ids.
        if prior.get("status") in {"superseded", "retire_candidate", "stale"}:
            continue
        prior_home = _normalize_home(prior.get("primaryHome"))
        if prior_home != canonical_home:
            continue  # home gate

        prior_subject = _normalize_subject(prior.get("subject") or "")
        prior_predicate = _normalize_predicate(prior.get("predicate") or "")
        prior_object = _normalize_subject(prior.get("object") or "")
        prior_text = str(prior.get("text") or "")

        subject_score = 1.0 if prior_subject == canonical_subject else 0.0
        predicate_score = 1.0 if prior_predicate == canonical_predicate else 0.0
        home_score = 1.0  # already gated; always 1.0 past the gate
        text_score = _jaccard(canonical_text_tokens, _dup_tokens(prior_text))

        # v1.5.0 object-match bonus (Contract 4 §33): only applies when BOTH
        # sides carry a non-empty object. Neutral otherwise so the absence
        # of structured objects doesn't suppress legitimate duplicates.
        object_bonus = 0.0
        if canonical_object and prior_object and canonical_object == prior_object:
            object_bonus = _PROBABLE_DUP_OBJECT_BONUS

        composite = (
            subject_score * _PROBABLE_DUP_WEIGHT_SUBJECT
            + predicate_score * _PROBABLE_DUP_WEIGHT_PREDICATE
            + home_score * _PROBABLE_DUP_WEIGHT_HOME
            + text_score * _PROBABLE_DUP_WEIGHT_TEXT
            + object_bonus
        )

        if composite >= _PROBABLE_DUP_THRESHOLD and composite > best_score:
            best_score = composite
            best_prior = prior
            # Identify which signal contributed most — supports operator
            # inspection via the duplicateReason metadata field.
            signals = [
                ("subject", subject_score * _PROBABLE_DUP_WEIGHT_SUBJECT),
                ("predicate", predicate_score * _PROBABLE_DUP_WEIGHT_PREDICATE),
                ("home", home_score * _PROBABLE_DUP_WEIGHT_HOME),
                ("text", text_score * _PROBABLE_DUP_WEIGHT_TEXT),
            ]
            signals.sort(key=lambda x: -x[1])
            best_signal = signals[0][0]

    if best_prior is None:
        return None
    return {
        "claim_id": best_prior.get("id"),
        "score": round(best_score, 3),
        "dominant_signal": best_signal,
    }


def _normalization_signature(canonical: dict) -> str:
    """Stable string fingerprint of the normalized reuse key — debug aid."""
    subj, pred, home = _normalize_reuse_key(
        canonical.get("subject"),
        canonical.get("predicate"),
        canonical.get("primary_home"),
    )
    return f"{subj}|{pred}|{home}"


# v1.5.0 B3 — type→home affinity table for per-claim diagnostic fields.
# Kept in sync with ``validate_outputs._TYPE_HOME_AFFINITY`` (same data).
# Duplicated locally so ``assemble_artifacts`` can emit the diagnostic
# fields without pulling validate_outputs as a runtime dependency.
_ROUTE_AFFINITY = {
    "fact":         ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "preference":   ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "constraint":   ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "commitment":   ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
    "identity":     ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "WISHES.md"}),
    "relationship": ("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "WISHES.md"}),
    "lesson":       ("LTMEMORY.md", {"LTMEMORY.md", "PLAYBOOKS.md"},     {"EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "decision":     ("LTMEMORY.md", {"LTMEMORY.md", "PLAYBOOKS.md"},     {"EPISODES.md", "WISHES.md"}),
    "open_question":("LTMEMORY.md", {"LTMEMORY.md"},                     {"PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
    "method":       ("PLAYBOOKS.md", {"PLAYBOOKS.md", "LTMEMORY.md"},    {"EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "procedure":    ("PLAYBOOKS.md", {"PLAYBOOKS.md"},                   {"LTMEMORY.md", "EPISODES.md", "HISTORY.md", "WISHES.md"}),
    "episode":      ("EPISODES.md",  {"EPISODES.md"},                    {"LTMEMORY.md", "PLAYBOOKS.md", "WISHES.md"}),
    "milestone":    ("HISTORY.md",   {"HISTORY.md", "EPISODES.md"},      {"LTMEMORY.md", "PLAYBOOKS.md", "WISHES.md"}),
    "aspiration":   ("WISHES.md",    {"WISHES.md"},                      {"LTMEMORY.md", "PLAYBOOKS.md", "EPISODES.md", "HISTORY.md"}),
}


def _route_diagnostics(ctype: str, home: str) -> dict:
    """Classify a claim's route affinity and return diagnostic fields.

    Returns a dict with ``routeValidationState``, ``routeAffinityScore``,
    and ``routeSuggestedHome``. Embedded in each persisted claim so
    operators can inspect route quality without rerunning the validator.
    """
    entry = _ROUTE_AFFINITY.get(ctype)
    if entry is None:
        return {"routeValidationState": "suspicious", "routeAffinityScore": 0.1, "routeSuggestedHome": None}
    strong_home, acceptable, impossible = entry
    if home in impossible:
        return {"routeValidationState": "impossible", "routeAffinityScore": 0.0, "routeSuggestedHome": strong_home}
    if home == strong_home:
        return {"routeValidationState": "strong", "routeAffinityScore": 1.0, "routeSuggestedHome": None}
    if home in acceptable:
        return {"routeValidationState": "acceptable", "routeAffinityScore": 0.5, "routeSuggestedHome": strong_home}
    return {"routeValidationState": "suspicious", "routeAffinityScore": 0.1, "routeSuggestedHome": strong_home}


def _validate_supersession_chain(new_claims: list, prior_claims: list) -> list:
    """Script-level cross-check: when a new claim's `supersedes` list
    targets a prior that is already itself superseded, surface a warning.

    v1.4.0: does NOT auto-chain to the prior's successor. Reconciliation-
    mode Pass 2 re-scores with wider horizon and resolves in normal course.

    Returns a list of warning strings (may be empty). Callers append these
    to manifest.warnings via write_manifest.py.
    """
    prior_by_id = {p.get("id"): p for p in prior_claims if p.get("id")}
    warnings: list = []
    for new in new_claims:
        new_id = new.get("id")
        for sup_id in (new.get("supersedes") or []):
            prior = prior_by_id.get(sup_id)
            if prior and prior.get("status") == "superseded":
                successor_list = prior.get("supersededBy") or []
                successor = successor_list[-1] if successor_list else "unknown"
                warnings.append(
                    f"supersession_chain_warning: {new_id} supersedes {sup_id} "
                    f"but {sup_id} was already superseded by {successor}"
                )
    return warnings


def translate_claim(
    claim_snake: dict,
    run_id: str,
    profile_scope: str,
    ts: dict,
    prior_claims: list = None,
) -> dict:
    """Translate a Pass-2 snake_case claim into the persisted camelCase shape.

    v1.5.0 B2 (Contract 4): every claim now carries the full
    duplicate-disposition metadata:
      - ``duplicateDisposition`` ∈ {``reuse_existing``, ``probable_duplicate``,
        ``new_claim``}
      - ``duplicateTargetClaimId`` — prior id pointed at (populated for
        reuse_existing and probable_duplicate)
      - ``duplicateReason`` — short rationale tag for operator inspection
      - ``normalizationSignature`` — the normalized 3-tuple, debug aid
    """
    canonical = claim_snake.get("canonical", {}) or {}
    claim_id = claim_snake.get("claim_id")

    # Disposition + target computed once, used for both id assignment and
    # metadata emission.
    disposition = "new_claim"
    duplicate_target = None
    duplicate_reason = None
    status_override = None
    priors = prior_claims or []

    if claim_id == "<new>" or not claim_id:
        reused = _semantic_reuse_match(canonical, priors)
        if reused:
            disposition = "reuse_existing"
            duplicate_target = reused
            duplicate_reason = "exact_normalized_match"
            claim_id = reused
        else:
            # No exact match — check for probable duplicate.
            dup = _probable_duplicate_match(canonical, priors)
            if dup:
                disposition = "probable_duplicate"
                duplicate_target = dup["claim_id"]
                duplicate_reason = (
                    f"composite_{dup['score']:.2f}_{dup['dominant_signal']}"
                )
                # Pass 2 owns status choice, but probable_duplicate is
                # load-bearing metadata for downstream consumers; override
                # the status to make the flag visible on-disk.
                status_override = "probable_duplicate"
                claim_id = _stable_claim_id(canonical)
            else:
                disposition = "new_claim"
                duplicate_reason = "no_candidates_above_threshold"
                claim_id = _stable_claim_id(canonical)

    provenance_camel = _translate_provenance(claim_snake.get("provenance", []))
    cross_surface_support = sorted({p["source"] for p in provenance_camel if p.get("source")})

    final_status = status_override or canonical.get("status")

    # v1.5.0 B3: per-claim route diagnostic fields (state + score + suggestion).
    route_diag = _route_diagnostics(canonical.get("type"), canonical.get("primary_home"))

    return {
        "id": claim_id,
        "sourceClusterId": claim_snake.get("source_cluster_id"),
        "type": canonical.get("type"),
        "status": final_status,
        "text": canonical.get("text"),
        "subject": canonical.get("subject"),
        "predicate": canonical.get("predicate"),
        "object": canonical.get("object"),
        "primaryHome": canonical.get("primary_home"),
        "secondaryTags": canonical.get("secondary_tags") or [],
        "profileScope": profile_scope,
        "scores": claim_snake.get("scores") or {},
        "provenance": provenance_camel,
        "crossSurfaceSupport": cross_surface_support,
        "contradictions": _translate_contradictions_field(claim_snake.get("contradictions", []), run_id),
        "contradictionClusterId": None,
        "supersedes": list(claim_snake.get("supersedes") or []),
        "supersededBy": list(claim_snake.get("superseded_by") or []),
        "freshnessPosture": claim_snake.get("freshness_posture"),
        "confidencePosture": claim_snake.get("confidence_posture"),
        "rationale": claim_snake.get("rationale"),
        "routeRationale": claim_snake.get("route_rationale"),
        # v1.5.0 B2 duplicate-disposition metadata:
        "duplicateDisposition": disposition,
        "duplicateTargetClaimId": duplicate_target,
        "duplicateReason": duplicate_reason,
        "normalizationSignature": _normalization_signature(canonical),
        # v1.5.0 B3 route-diagnostic fields:
        "routeValidationState": route_diag["routeValidationState"],
        "routeAffinityScore": route_diag["routeAffinityScore"],
        "routeSuggestedHome": route_diag["routeSuggestedHome"],
        "updatedInRunId": run_id,
        "updatedAt": ts["timestamp"],
        "updatedAt_utc": ts["timestamp_utc"],
        "timezone": ts["timezone"],
    }


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


# v1.5.0 audit-corrective: atomic writers now delegate to ``_lib/fs.py``
# so fsync/temp-sibling improvements land consistently. Keep the local
# name alias so existing call sites inside this script don't churn.
from _lib.fs import atomic_write_json as _lib_atomic_write_json  # noqa: E402
from _lib.fs import atomic_write_jsonl as _lib_atomic_write_jsonl  # noqa: E402


def atomic_write_jsonl(path: Path, records: list) -> None:
    _lib_atomic_write_jsonl(path, records)


def atomic_write_json(path: Path, obj) -> None:
    _lib_atomic_write_json(path, obj)


def merge_claims(prior_claims: list, new_claims: list, run_id: str) -> list:
    """Merge new_claims into prior_claims by stable id.

    - Duplicate id → replace (update in place).
    - supersedes[] chain → mark referenced prior claims with status='superseded'
      and append the superseding claim id to their supersededBy list.
    """
    by_id = {c["id"]: c for c in prior_claims if c.get("id")}
    for new in new_claims:
        by_id[new["id"]] = new

    for new in new_claims:
        for prior_id in new.get("supersedes") or []:
            if prior_id in by_id and prior_id != new["id"]:
                prior = by_id[prior_id]
                prior["status"] = "superseded"
                sb = prior.get("supersededBy") or []
                if new["id"] not in sb:
                    sb.append(new["id"])
                prior["supersededBy"] = sb
                prior["updatedInRunId"] = run_id

    return list(by_id.values())


def mark_stale_for_removed_sources(
    all_claims: list,
    removed_sources: list,
    run_id: str,
) -> int:
    """Mark claims whose provenance only referenced now-removed sources.

    Rule: a claim is a retire_candidate if every `provenance[*].source` it lists
    is in `removed_sources`. Claims that still have at least one surviving
    provenance entry keep their current status — they're just weakened, not
    orphaned.

    Returns the count of claims marked. Mutates `all_claims` in place.
    """
    if not removed_sources or not all_claims:
        return 0
    removed_set = set(removed_sources)
    touched = 0
    for claim in all_claims:
        # Skip already-superseded or already-retired claims.
        if claim.get("status") in {"superseded", "retire_candidate", "stale"}:
            continue
        prov = claim.get("provenance") or []
        if not prov:
            continue
        claim_sources = {(p or {}).get("source") for p in prov if p and p.get("source")}
        if not claim_sources:
            continue
        if claim_sources.issubset(removed_set):
            # Every source this claim depends on is gone — flag for retirement.
            claim["status"] = "retire_candidate"
            claim["updatedInRunId"] = run_id
            existing_reasons = claim.get("retirementReasons") or []
            existing_reasons.append({
                "runId": run_id,
                "reason": "all_sources_removed",
                "removed_sources": sorted(claim_sources),
            })
            claim["retirementReasons"] = existing_reasons
            touched += 1
    return touched


def build_contradiction_records(new_claims: list, run_id: str, ts: dict) -> list:
    """Flatten new_claims' contradictions into per-relationship records.

    Also stamps `contradictionClusterId` onto the claim dict so that downstream
    reads of the claim can find the cluster. Because `merge_claims` returned a
    list containing references to the same dict instances, this mutation is
    visible to the caller's claim state.
    """
    out: list = []
    for claim in new_claims:
        contras = claim.get("contradictions") or []
        if not contras:
            continue
        cluster_id = claim.get("contradictionClusterId") or ("contra-" + str(uuid.uuid4())[:12])
        claim["contradictionClusterId"] = cluster_id
        for c in contras:
            out.append({
                "contradictionClusterId": cluster_id,
                "claimId": claim["id"],
                "competingClaimId": c.get("competingClaimId"),
                "competingText": c.get("competingText"),
                "relation": c.get("relation"),
                "flaggedInRunId": c.get("flaggedInRunId") or run_id,
                "recordedAt": ts["timestamp"],
                "recordedAt_utc": ts["timestamp_utc"],
                "timezone": ts["timezone"],
            })
    return out


def merge_contradictions(prior: list, new: list) -> list:
    """Dedupe by (clusterId, claimId, competingClaimId, competingText)."""
    seen: dict = {}
    for r in prior + new:
        k = (
            r.get("contradictionClusterId"),
            r.get("claimId"),
            r.get("competingClaimId"),
            r.get("competingText"),
        )
        seen[k] = r
    return list(seen.values())


def build_entities(claims: list) -> dict:
    entities: dict = {}
    for claim in claims:
        subj = claim.get("subject")
        if not subj:
            continue
        entry = entities.setdefault(subj, {
            "canonicalForm": subj,
            "aliases": [],
            "claimIds": [],
        })
        cid = claim.get("id")
        if cid and cid not in entry["claimIds"]:
            entry["claimIds"].append(cid)
    for entry in entities.values():
        entry["claimIds"].sort()
    return entities


def build_routes(claims: list) -> dict:
    routes: dict = {
        "LTMEMORY.md": [],
        "PLAYBOOKS.md": [],
        "EPISODES.md": [],
        "HISTORY.md": [],
        "WISHES.md": [],
    }
    inactive = {"superseded", "stale", "retire_candidate"}
    for claim in claims:
        home = claim.get("primaryHome")
        if home in routes and claim.get("status") not in inactive:
            routes[home].append(claim["id"])
    for lst in routes.values():
        lst.sort()
    return routes


def main() -> int:
    ap = argparse.ArgumentParser(description="Assemble Pass 2 artifacts into runtime JSONL/JSON files.")
    ap.add_argument("--pass2", help="Pass 2 result JSON (from score_purifier.py) or '-' for stdin. Optional when --removed-sources is used for a stale-only sweep.")
    ap.add_argument("--workspace", help="Workspace root override")
    ap.add_argument("--runtime-dir", help="Runtime dir override")
    ap.add_argument("--timezone", help="IANA timezone name")
    ap.add_argument(
        "--removed-sources",
        default="[]",
        help='JSON array of source paths that were present in the prior run\'s sourceInventory but are absent now. '
             'Claims whose provenance depends ONLY on these sources are marked status="retire_candidate".',
    )
    ap.add_argument(
        "--run-id",
        help="Explicit run_id. Overrides the id derived from --pass2 and the synthetic "
             "`sweep-<uuid>` fallback used on stale-only sweeps. Lets stale-sweep inherit "
             "the orchestrator's run_id for end-to-end lineage (v1.5.0 D2).",
    )
    ap.add_argument(
        "--output-dir",
        help="Override for the artifact output directory (v1.5.0 A2). Default = --runtime-dir "
             "(back-compat for standalone invocations). When set to a staging path, the "
             "four machine artifacts (claims.jsonl, contradictions.jsonl, entities.json, "
             "routes.json) are written there instead of the final runtime dir. Prior claims "
             "are still read from the runtime dir.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Translate + merge; do not write any files")

    args = ap.parse_args()

    try:
        removed_sources = json.loads(args.removed_sources) if args.removed_sources else []
        if not isinstance(removed_sources, list):
            removed_sources = []
    except json.JSONDecodeError:
        removed_sources = []

    # Two run shapes: (a) normal — pass2 provides new claims; (b) stale-only sweep —
    # no pass2 but removed_sources is non-empty, so we still rewrite claims to mark retirees.
    pass2 = None
    if args.pass2:
        if args.pass2 == "-":
            pass2 = json.load(sys.stdin)
        else:
            pass2 = json.loads(Path(args.pass2).expanduser().read_text())

    tz_name = args.timezone or (pass2 or {}).get("timezone") or "Asia/Manila"
    ts = timestamp_triple(tz_name)

    if pass2 is None and not removed_sources:
        out = {
            "status": "skipped",
            "reason": "no pass2 input and no removed_sources — nothing to do",
            "pass": "assemble",
            "dry_run": args.dry_run,
            **ts,
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    if pass2 is not None and pass2.get("status") != "ok":
        # Stale sweep can still run even if pass2 wasn't ok, as long as removed_sources
        # is present — for example, scope skipped with removals-only.
        if not removed_sources:
            out = {
                "status": "skipped",
                "reason": f"pass2 status is {(pass2 or {}).get('status')!r}",
                "pass": "assemble",
                "dry_run": args.dry_run,
                **ts,
            }
            print(json.dumps(out, indent=2, ensure_ascii=False))
            return 0

    # v1.5.0 D2: explicit --run-id wins, then pass2-derived, then synthetic fallback
    # (the fallback is preserved for standalone invocations without an orchestrator).
    run_id = args.run_id or (pass2 or {}).get("run_id") or f"sweep-{uuid.uuid4().hex[:12]}"
    profile_scope = (pass2 or {}).get("profile_scope") or "business"
    mode = (pass2 or {}).get("mode") or "incremental"
    canonical_claims_snake = (pass2 or {}).get("canonical_claims") or []

    workspace_hint = (
        args.workspace
        or (pass2 or {}).get("workspace")
        or os.environ.get("WORKSPACE")
    )
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")

    # Prior claims ARE still read from the runtime dir — the most-recently
    # committed state is always there. Only the write target flips to
    # `output_dir` when A2 staging is active.
    prior_claims_path = runtime_dir / "purified-claims.jsonl"
    prior_contras_path = runtime_dir / "purified-contradictions.jsonl"

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else runtime_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    claims_path = output_dir / "purified-claims.jsonl"
    contras_path = output_dir / "purified-contradictions.jsonl"
    entities_path = output_dir / "purified-entities.json"
    routes_path = output_dir / "purified-routes.json"

    prior_claims = load_jsonl(prior_claims_path)
    prior_contras = load_jsonl(prior_contras_path)
    # Translate with prior_claims in scope so semantic-reuse matching can fire.
    new_claims = [translate_claim(c, run_id, profile_scope, ts, prior_claims=prior_claims) for c in canonical_claims_snake]

    # v1.4.0: sanity-check supersession chains before merge. Warnings are
    # surfaced in the result JSON (and in manifest.warnings via write_manifest).
    chain_warnings = _validate_supersession_chain(new_claims, prior_claims)

    merged_claims = merge_claims(prior_claims, new_claims, run_id)
    new_contras = build_contradiction_records(new_claims, run_id, ts)
    merged_contras = merge_contradictions(prior_contras, new_contras)

    # Stale sweep: mark claims whose provenance is fully orphaned by removed sources.
    retire_candidate_count = mark_stale_for_removed_sources(merged_claims, removed_sources, run_id)

    entities = build_entities(merged_claims)
    routes = build_routes(merged_claims)

    artifacts_written: list = []
    if not args.dry_run:
        atomic_write_jsonl(claims_path, merged_claims)
        atomic_write_jsonl(contras_path, merged_contras)
        atomic_write_json(entities_path, entities)
        atomic_write_json(routes_path, routes)
        artifacts_written = [str(claims_path), str(contras_path), str(entities_path), str(routes_path)]

    superseded_count = sum(1 for c in merged_claims if c.get("status") == "superseded")
    stale_count = sum(1 for c in merged_claims if c.get("status") == "stale")
    retire_candidate_total = sum(1 for c in merged_claims if c.get("status") == "retire_candidate")

    out = {
        "status": "ok",
        "run_id": run_id,
        "pass": "assemble",
        "mode": mode,
        "profile_scope": profile_scope,
        "workspace": str(workspace),
        "runtime_dir": str(runtime_dir),
        "claim_count_total": len(merged_claims),
        "claim_count_new": len(new_claims),
        "claim_count_superseded": superseded_count,
        "claim_count_stale": stale_count,
        "claim_count_retire_candidate": retire_candidate_total,
        "claim_count_retired_this_run": retire_candidate_count,
        "removed_sources": removed_sources,
        "contradiction_count_total": len(merged_contras),
        "contradiction_count_new": len(new_contras),
        "entities_count": len(entities),
        "routes_count_per_home": {k: len(v) for k, v in routes.items()},
        "artifacts_written": artifacts_written,
        "warnings": chain_warnings,
        "warning_count": len(chain_warnings),
        "dry_run": args.dry_run,
        **ts,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
