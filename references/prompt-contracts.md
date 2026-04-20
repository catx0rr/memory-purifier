# prompt-contracts.md — Prompt Invocation, Schemas, and Field Semantics

**Reference material for the two scoring passes (narrow schema-bound model calls; the agent supervisor lives at the cron-prompt level, not inside these passes). The executable prompts ([`prompts/promotion-pass.md`](../prompts/promotion-pass.md) and [`prompts/purifier-pass.md`](../prompts/purifier-pass.md)) are intentionally lean — everything schema-tutorial, worked-example, or interpretive belongs here.**

---

## 1. Pass catalogue

| Pass | File | Role | Script invoker |
|---|---|---|---|
| 1 | `prompts/promotion-pass.md` | Survival judgment — score and verdict per candidate | `scripts/score_promotion.py` |
| 2 | `prompts/purifier-pass.md` | Canonicalization — score, wording, routing, contradiction state per cluster | `scripts/score_purifier.py` |

Both passes are stateless. Input JSON → output JSON. The script owns batching, retry, validation, and persistence.

---

## 2. Invocation pattern

`prompts.backend` in `memory-purifier.json` selects the model backend. The invocation shape is identical across backends:

1. Script builds input JSON per the pass's input schema.
2. Script loads the prompt markdown file verbatim as the system message.
3. Script sends the pass's input JSON as the user message.
4. The model returns a JSON string.
5. Script parses, validates against the output schema, persists (or raises partial_failure).

No prompt templating, no variable substitution, no prompt-side path resolution.

---

## 3. Pass 1 — Promotion schemas

### 3.1 Input

```json
{
  "run_id": "<uuid>",
  "mode": "incremental | reconciliation",
  "profile_scope": "business | personal | shared",
  "candidates": [
    {
      "candidate_id": "<stable-hash>",
      "text": "<consolidated unit as plain prose>",
      "type_hint": "<extractor hint, e.g. fact | lesson | method | episode | ...>",
      "source_refs": [
        {"source": "<filename>", "line_span": [n, m], "captured_at": "<iso-8601>"}
      ],
      "adjacent_context": "<optional short nearby text>",
      "prior_verdict": "<optional — reconciliation only>"
    }
  ]
}
```

### 3.2 Field semantics

| Field | Meaning |
|---|---|
| `text` | The semantic payload. Score on this. |
| `type_hint` | Best-effort from extractor. Guidance, not ground truth — Pass 1 does not output a type. |
| `source_refs` | Provenance. Multiple cross-surface entries strengthen `cross_time_persistence`. Validated, not invented. |
| `profile_scope` | Governs eligibility. Business runs must reject personal-only content. |
| `prior_verdict` | Reconciliation only. A prior `defer` that has reinforced may now warrant `compress` or `promote`. |
| `adjacent_context` | Disambiguation only. Do not score on it. |

### 3.3 Output

```json
{
  "run_id": "<echo>",
  "verdicts": [
    {
      "candidate_id": "<echo>",
      "scores": {
        "durability": 0.0, "future_judgment_value": 0.0, "action_value": 0.0,
        "identity_relationship_weight": 0.0, "cross_time_persistence": 0.0, "noise_risk": 0.0
      },
      "strength": 0.0,
      "verdict": "reject | defer | compress | merge | promote",
      "rationale": "<one or two sentences>",
      "merge_candidate_ids": [],
      "compress_target": null
    }
  ]
}
```

### 3.4 Validator checks (enforced by `score_promotion.py`)

- Every input `candidate_id` appears exactly once in `verdicts`.
- `verdict` ∈ `{reject, defer, compress, merge, promote}`.
- `scores` has all six keys, each a float in `[0.0, 1.0]`.
- `strength` equals the formula result within `±0.01` (re-derived by the validator — primary defense against hallucinated scoring).
- `merge_candidate_ids` is `[]` unless `verdict == "merge"`; bidirectional within the batch.
- `compress_target` is `null` unless `verdict == "compress"`.
- `profile_scope == "business"` forbids personal-only content from being marked `promote` or `compress`.

---

## 4. Clustering (between passes)

`scripts/cluster_survivors.py` sits between the two passes. It is deterministic (no model call). It:
- Takes Pass 1 survivors (verdicts: `promote`, `compress`, `merge`).
- Unions candidates via `merge_candidate_ids` (explicit merge hints from Pass 1 carry the scoring pass's semantic overlap decision).
- Produces `clusters[]` and populates `cluster_hints.*` for Pass 2 input.
- Leaves `contradiction_candidates` empty at this stage — the clusterer has no access to prior claims. Population happens downstream in `score_purifier.py::retrieve_prior_claims` (v1.4.0): for every cluster, after ranking prior claims against that cluster's query, the top-N prior ids are merged into `cluster_hints.contradiction_candidates`. This fires whenever prior claims are loaded — reconciliation mode by default, or incremental when invoked with `--prior-claims`. Pass 2 then reads the hint per the prompt contract.

---

## 5. Pass 2 — Purifier schemas

### 5.1 Input

```json
{
  "run_id": "<uuid>",
  "mode": "incremental | reconciliation",
  "profile_scope": "business | personal | shared",
  "clusters": [
    {
      "cluster_id": "<stable-hash>",
      "candidates": [
        {
          "candidate_id": "<echo from Pass 1>",
          "text": "<consolidated unit>",
          "type_hint": "<extractor hint>",
          "source_refs": [{"source": "...", "line_span": [n, m], "captured_at": "<iso>"}],
          "pass_1_verdict": "promote | compress | merge",
          "pass_1_rationale": "<brief>",
          "compress_target": "<one-line digest or null>"
        }
      ],
      "cluster_hints": {
        "shared_entities": ["..."],
        "shared_subject": "<normalized-or-null>",
        "proposed_type": "<type-or-null>",
        "proposed_primary_home": "<filename-or-null>",
        "contradiction_candidates": ["<prior_claim_id>"]
      }
    }
  ],
  "prior_claims_context": [
    {
      "claim_id": "<hash>",
      "text": "<canonical text>",
      "type": "<type>",
      "status": "resolved | contested | unresolved | superseded | stale | retire_candidate",
      "primary_home": "<filename>",
      "provenance": [{"source": "...", "line_span": [n, m], "captured_at": "..."}],
      "updated_at": "<iso>"
    }
  ]
}
```

### 5.2 Field semantics

| Field | Meaning |
|---|---|
| `candidates[*].text` | Payload to canonicalize. If the cluster has multiple candidates, choose the clearest or synthesize a minimal form. |
| `cluster_hints.proposed_type` / `proposed_primary_home` | Guidance only — may be overridden. |
| `cluster_hints.contradiction_candidates` | Prior claim ids the clusterer flagged as potentially conflicting. Verify against `prior_claims_context` before declaring contradiction. |
| `prior_claims_context` | Prior canonical claims ranked by relevance to the current clusters (not the most-recent N). See §5.6 below — `score_purifier.py` retrieves and ranks; the scoring pass consumes a topically-filtered slice. Used for stable `claim_id` reuse, supersession detection, contradiction status. Populated in reconciliation mode. |
| `profile_scope` | Governs eligibility of personal-only primary homes. |

### 5.3 Output

```json
{
  "run_id": "<echo>",
  "canonical_claims": [
    {
      "claim_id": "<new>" | "<prior_claim_id>",
      "source_cluster_id": "<echo>",
      "scores": {
        "semantic_cluster_confidence": 0.0, "canonical_clarity": 0.0,
        "provenance_strength": 0.0, "contradiction_pressure": 0.0,
        "freshness": 0.0, "confidence": 0.0,
        "route_fitness": 0.0, "supersession_confidence": 0.0
      },
      "canonical": {
        "type": "<enum>", "status": "<enum>",
        "text": "<canonical wording>",
        "subject": "<normalized>", "predicate": "<normalized>", "object": "<normalized-or-null>",
        "primary_home": "<enum>",
        "secondary_tags": []
      },
      "provenance": [{"source": "...", "line_span": [n, m], "type": "direct | inferred | merged", "captured_at": "<iso>"}],
      "contradictions": [{"competing_claim_id": "<id-or-null>", "competing_text": "<prose-or-null>", "relation": "contested | stale | superseded"}],
      "supersedes": [], "superseded_by": [],
      "freshness_posture": "fresh | recent | aging | stale",
      "confidence_posture": "high | medium | low | tentative",
      "rationale": "<brief>",
      "route_rationale": "<brief>"
    }
  ]
}
```

### 5.4 Enums

- `type` ∈ `{fact, lesson, decision, commitment, constraint, preference, identity, relationship, method, procedure, episode, aspiration, milestone, open_question}`.
- `status` ∈ `{resolved, contested, unresolved, superseded, stale, retire_candidate}`. Pass 2 emits `resolved`, `contested`, `unresolved`, `superseded`, or `stale`; `retire_candidate` is added by `assemble_artifacts.py` when all of a claim's source provenance entries reference files that have been removed from the workspace (see §5.8).
- `primary_home` ∈ `{LTMEMORY.md, PLAYBOOKS.md, EPISODES.md, HISTORY.md, WISHES.md}`. Personal-only gate applies to `HISTORY.md` / `WISHES.md`.

### 5.5 Validator checks (enforced by `score_purifier.py`)

- Every input `cluster_id` produces exactly one claim with matching `source_cluster_id`.
- `scores` has all eight keys, floats in `[0, 1]`.
- `type`, `status`, `primary_home` are exact enum matches.
- Personal-only primary homes allowed only on `profile_scope == "personal"`.
- `provenance[]` is non-empty; every `source` traces to an input candidate's `source_refs` (no invented provenance).
- `contradictions[]` entries have at least one of `competing_claim_id` or `competing_text` populated.
- `claim_id` is `"<new>"` or matches an entry in `prior_claims_context`. The scoring pass is forbidden from generating stable ids.

### 5.6 Prior-claim retrieval (not recency slicing)

`score_purifier.py` does not feed the most-recent N claims into Pass 2. It **ranks** prior claims by relevance to the current clusters and returns the top N after ranking. Ranking signals (in `_rank_prior_claim`):

- exact `subject` match (strong)
- Jaccard overlap on subject tokens (weaker fallback)
- shared-entity hits inside the claim text
- same `primaryHome` (routing affinity)
- same `type` (type affinity)
- token-level Jaccard overlap on the claim text

Recency is used only as a secondary tiebreaker inside the sort. The final cap (`--prior-claims-cap`, default 50) is applied **after** ranking, not as a blunt global slice.

This matters at scale: as purified state grows into the thousands, a supersession or contradiction signal on an older claim will still reach Pass 2 when a new cluster touches the same subject — the ranker surfaces it even if it's no longer among the most-recent entries.

### 5.7 Semantic claim-id reuse

When `assemble_artifacts.py` translates a Pass 2 claim whose `claim_id == "<new>"`, it first looks for an active prior claim with matching `(subject, predicate, primary_home)`. If one exists, the new claim **reuses that prior id** and becomes an in-place update; text differences are treated as rewording rather than a brand-new claim. Stable-hash minting only happens when no prior match is found. This prevents duplicate artifacts for claims that the scoring pass wrote differently but that represent the same canonical unit.

### 5.8 Stale / retire_candidate handling

When `select_scope.py` detects that a source file in the prior run's `sourceInventory` is absent from the current workspace (`removed_sources[]`), `assemble_artifacts.py` marks every active claim whose provenance depends ONLY on removed sources with `status: "retire_candidate"`. A retirement trace is recorded in the claim's `retirementReasons[]` for audit. Retired claims remain in `purified-claims.jsonl` for traceability but are excluded from `purified-routes.json` and markdown views. They can be fully retired or restored by reconciliation — the purifier never hard-deletes silently.

---

## 6. Batching

| Pass | Batch ceiling | Config key |
|---|---|---|
| 1 | 40 candidates | `limits.max_candidates_per_batch` |
| 2 | 20 clusters | `limits.max_clusters_per_batch` |

Input over the ceiling is split deterministically (sorted by `candidate_id` / `cluster_id`) so reruns reproduce the same batching. Batches are processed sequentially to prevent race conditions on shared state (contradiction clusters, entity normalization). A failed batch records a `partialFailure` in the manifest and the remaining batches continue.

---

## 7. Retry policy

- Malformed JSON from the scoring pass → retry once with a terse correction nudge.
- Schema validation failure → retry once; on second failure, raw response is written to `runtime/locks/failed-<pass>-<run_id>.json` and a `partialFailure` is recorded.
- Timeout or backend error → no retry in V1; recorded as `partialFailure`.

Reruns of the whole pipeline re-process any batches whose outputs are missing, so eventual consistency holds without retry storms.

---

## 8. Prompt → artifact field translation

Scoring-pass output uses `snake_case`. Persisted artifacts use `camelCase`. The translation happens in `assemble_artifacts.py` and is not the prompt's concern.

Canonical mapping (Pass 2 → `purified-claims.jsonl`):

| Scoring-pass `snake_case` | Artifact `camelCase` |
|---|---|
| `claim_id` | `id` |
| `source_cluster_id` | `sourceClusterId` |
| `canonical.primary_home` | `primaryHome` |
| `canonical.secondary_tags` | `secondaryTags` |
| `provenance[*].line_span` | `provenance[*].lineSpan` |
| `provenance[*].captured_at` | `provenance[*].capturedAt` |
| `contradictions[*].competing_claim_id` | `contradictions[*].competingClaimId` |
| `contradictions[*].competing_text` | `contradictions[*].competingText` |
| `superseded_by` | `supersededBy` |
| `freshness_posture` | `freshnessPosture` |
| `confidence_posture` | `confidencePosture` |
| `route_rationale` | `routeRationale` |

Scripts also attach fields the scoring pass does not produce:
- `profileScope` (echoed from run-level `profile_scope`)
- `crossSurfaceSupport` (distinct `source` values in `provenance`)
- `contradictionClusterId` (UUID assigned during contradiction intake)
- `updatedAt`, `updatedAt_utc`, `timezone` (timestamp triple)
- `updatedInRunId` (run_id that last touched the record)

---

## 9. Claim IDs are purifier-local

Stable hash IDs generated by `assemble_artifacts.py` (`cl-<16-hex>`) are **purifier-local artifact identifiers**. They exist to keep artifact state idempotent across reruns — same canonical content → same id, so re-running the same inputs does not multiply claims or break supersession chains.

### The purifier-to-wiki boundary (law)

- Purifier IDs = **local artifact bookkeeping only**.
- Wiki / reconciler IDs = **downstream reconciled identity**.
- The purifier may *suggest* identity by reusing a prior id on a semantic match.
- The **wiki decides** final cross-layer canonical identity when it compiles its vault.

The purifier never claims to own the global truth of a fact. It owns only the artifact-layer identity that makes its own reruns deterministic. The reconciler is free to mint its own scheme, map, merge, or relabel — that's its job.

Treat purifier claim ids as opaque keys useful for:
- idempotent rewrites within purified state
- supersession links within purified state
- contradiction cluster bookkeeping within purified state
- cross-run telemetry correlation

Do not expose them as if they are the global identity of a fact.

---

## 10. Scoring-dimension anchors

### Pass 1 (6 dimensions)

| Dimension | `0.0` means | `1.0` means |
|---|---|---|
| `durability` | Ephemeral, in-flux, will be stale soon | Lasting fact, stable preference, durable lesson |
| `future_judgment_value` | No evaluative or actionable leverage | Clearly shapes how future choices should be weighed |
| `action_value` | Pure description, no procedural lift | Turns into a repeatable playbook step |
| `identity_relationship_weight` | Generic, impersonal detail | Identity-level or relationship-level signal |
| `cross_time_persistence` | Single isolated occurrence | Repeated across many surfaces / timestamps |
| `noise_risk` | Substantive, not noise | Clearly disposable filler |

Verdict formula:
```
strength = durability + future_judgment_value + action_value
         + identity_relationship_weight + cross_time_persistence
         - noise_risk
```

### Pass 2 (8 dimensions)

| Dimension | `0.0` means | `1.0` means |
|---|---|---|
| `semantic_cluster_confidence` | Cluster members are clearly about different things | Same claim in different wordings |
| `canonical_clarity` | Hedged, tangled, ambiguous phrasing | Short, clear, entity-normalized, retrieval-friendly |
| `provenance_strength` | Single weak source, no cross-support | Multiple strong cross-surface sources, recent |
| `contradiction_pressure` | No conflicting signal | Strong, near-symmetric conflict |
| `freshness` | Evidence old, not refreshed | Evidence from the most recent consolidation window |
| `confidence` | Low — better marked `unresolved` or `tentative` | High — suitable for durable recall |
| `route_fitness` | Ambiguous routing | Unambiguously belongs in the chosen home |
| `supersession_confidence` | A prior version likely supersedes this | Clearly the latest, or no prior version exists |

Freshness / confidence posture mapping:

| `freshness` range | `freshness_posture` |
|---|---|
| `>= 0.8` | `fresh` |
| `0.5 – 0.79` | `recent` |
| `0.3 – 0.49` | `aging` |
| `< 0.3` | `stale` |

| `confidence` range | `confidence_posture` |
|---|---|
| `>= 0.8` | `high` |
| `0.5 – 0.79` | `medium` |
| `0.3 – 0.49` | `low` |
| `< 0.3` | `tentative` |

---

## 11. Anti-patterns (for scripts invoking prompts)

- Do NOT template the prompt file with variable substitution. Load verbatim as the system message.
- Do NOT inject data into the prompt's instruction section. Data goes only in the user message.
- Do NOT relax JSON validation because the scoring pass "got close." Schema mismatch → retry or partialFailure.
- Do NOT parallelize batches in V1. Sequential only.
- Do NOT concatenate multiple passes' outputs into a single file. Each pass writes its own artifact stage.
