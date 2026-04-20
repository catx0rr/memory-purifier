# purifier-pass.md — Pass 2 (Canonicalization)

Canonicalize surviving clusters. For each cluster, emit one canonical claim with canonical wording, one primary home, preserved provenance, and explicit contradiction / supersession state.

You do not re-judge survival, render markdown, invent provenance, or generate claim IDs.

Full schema, worked examples, routing logic, and posture thresholds live in [`references/prompt-contracts.md`](../references/prompt-contracts.md) §5, §10 and [`references/routing-rules.md`](../references/routing-rules.md). This prompt carries only what you need at call time.

---

## Ordered actions

For each cluster in `clusters[]`:

1. Choose canonical `text`, `subject`, `predicate`, `object`, `secondary_tags`.
2. Choose `type`, `status`, `primary_home` from the enums below.
3. Echo `provenance[]` from the cluster candidates' `source_refs`.
4. Populate `contradictions[]` from `cluster_hints.contradiction_candidates` and `prior_claims_context`; populate `supersedes[]` when this cluster updates a prior claim.
5. Score all eight dimensions and derive `freshness_posture` / `confidence_posture`.
6. Emit one `canonical_claim` with matching `source_cluster_id`.

Return all claims in a single `canonical_claims[]` array.

---

## Input fields you will reference

From each `clusters[*]`:
`cluster_id`, `candidates[*].{candidate_id, text, type_hint, source_refs, pass_1_verdict, pass_1_rationale, compress_target}`, `cluster_hints.{shared_entities, shared_subject, proposed_type, proposed_primary_home, contradiction_candidates}`.

From `prior_claims_context[*]` (for reuse, supersession, contradiction lookup):
`claim_id`, `text`, `type`, `status`, `primary_home`, `provenance`, `updated_at`.

Top-level: `run_id`, `mode`, `profile_scope`. Full shapes: [`references/prompt-contracts.md §5.1`](../references/prompt-contracts.md#51-input).

---

## Enums

- `type`: `fact | lesson | decision | commitment | constraint | preference | identity | relationship | method | procedure | episode | aspiration | milestone | open_question`
- `status`: `resolved | contested | unresolved | superseded | stale`  _(emit these five; `retire_candidate` is added by the script layer on source removal, not by Pass 2)_
- `primary_home`: `LTMEMORY.md | PLAYBOOKS.md | EPISODES.md | HISTORY.md | WISHES.md`
- `freshness_posture`: `fresh | recent | aging | stale`
- `confidence_posture`: `high | medium | low | tentative`
- `provenance[*].type`: `direct | inferred | merged`
- `contradictions[*].relation`: `contested | stale | superseded`

---

## Hard constraints

- Exactly one `primary_home` per claim.
- `HISTORY.md` / `WISHES.md` → personal profile only.
- `EPISODES.md` text ≤ 500 chars; `provenance[0].source` must start with `episodes/`.
- `claim_id` is `"<new>"` or an id from `prior_claims_context`.
- Every `provenance[*]` entry must trace to a cluster candidate's `source_refs` or to `prior_claims_context`.
- Preserve contradictions with `status: "contested"`; never flatten.
- Every input `cluster_id` produces exactly one output claim.

---

## Output

Return exactly one JSON object. No prose outside the envelope, no markdown fences.

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
        "primary_home": "<enum>", "secondary_tags": []
      },
      "provenance": [{"source": "...", "line_span": [n, m], "type": "<enum>", "captured_at": "<iso>"}],
      "contradictions": [{"competing_claim_id": "<id-or-null>", "competing_text": "<prose-or-null>", "relation": "<enum>"}],
      "supersedes": [], "superseded_by": [],
      "freshness_posture": "<enum>", "confidence_posture": "<enum>",
      "rationale": "<brief>", "route_rationale": "<brief>"
    }
  ]
}
```

---

## Do not

- Invent provenance, claim IDs, or entities.
- Emit `HISTORY.md` or `WISHES.md` on business runs.
- Copy full episode narratives into `EPISODES.md` (digest only).
- Flatten contradictions into a winner.
- Skip clusters or reuse a `source_cluster_id`.
- Emit any prose outside the JSON envelope.
