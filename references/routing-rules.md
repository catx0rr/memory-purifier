# routing-rules.md — Primary-Home Routing & Collision-Zone Rules

**Deterministic router logic for Pass 2. Pass 2's prompt (`prompts/purifier-pass.md`) enforces these rules at semantic level; `score_purifier.py` and `assemble_artifacts.py` enforce them at the validation layer.**

---

## 1. The five primary homes

Every canonical claim lives in exactly ONE primary home.

| Primary home | What goes there | Profile scope |
|---|---|---|
| `LTMEMORY.md` | Durable facts, stable preferences, lasting lessons, stable constraints, long-lived commitments, identity-level durable meaning, relationship-level durable understanding | Shared (both profiles) |
| `PLAYBOOKS.md` | Validated reusable methods, stable procedures, known-good operational recipes | Shared (both profiles) |
| `EPISODES.md` | Canonical **digest** of bounded narrative events. Pointer/index to `<workspace>/episodes/<slug>.md`. Never duplicates full narrative. | Shared (both profiles) |
| `HISTORY.md` | Personal major milestones, turning points, meaningful chronology anchors | **Personal profile only** |
| `WISHES.md` | Stable aspirations, recurring longings, desire trajectories, future-facing value signals | **Personal profile only** |

Secondary surfaces (tags, cross-references) may point to other homes but must never duplicate the canonical unit.

---

## 2. Type → primary home mapping (default routing)

```
type                primary_home     notes
----                ------------     -----
fact                LTMEMORY.md
lesson              LTMEMORY.md
preference          LTMEMORY.md
constraint          LTMEMORY.md
commitment          LTMEMORY.md
identity            LTMEMORY.md
relationship        LTMEMORY.md
decision            LTMEMORY.md      + secondary_tag "PLAYBOOKS.md" if decision produced a reusable method
method              PLAYBOOKS.md
procedure           PLAYBOOKS.md
episode             EPISODES.md      digest only; text points to source episodes/<slug>.md
milestone           HISTORY.md       personal profile; on business → reroute to EPISODES.md or reject
aspiration          WISHES.md        personal profile; on business → reject
open_question       LTMEMORY.md      with status: "unresolved"
```

Ambiguous cases:
- **"decision that produced a method":** primary `LTMEMORY.md` (the decision is the durable unit); `secondary_tags` includes `PLAYBOOKS.md` to cross-index the derived method.
- **"lesson from an episode":** primary `LTMEMORY.md` (the lesson is what persists); `secondary_tags` includes `EPISODES.md` with the source slug.
- **"aspiration that hints at a commitment":** split into two canonical claims — one `aspiration` routed to `WISHES.md`, one `commitment` routed to `LTMEMORY.md`. Do not fuse.

---

## 3. Hard routing constraints

### 3.1 One primary home per claim

Every claim in `purified-claims.jsonl` has exactly one `primaryHome`. Validation fails the run if this is violated.

### 3.2 Personal-only home gating

On `profile_scope: "business"`:
- Scripts must NOT emit claims with `primaryHome: "HISTORY.md"` or `primaryHome: "WISHES.md"`.
- If Pass 2 emits such a claim despite profile, `assemble_artifacts.py` reroutes it to `LTMEMORY.md` with `status: "unresolved"` and appends a manifest warning. The claim is not dropped, but it does not reach the personal-only surface.

### 3.3 EPISODES.md is a digest, not a duplicate

The canonical claim's `text` field for `primaryHome: "EPISODES.md"` must:
- Be a short digest (1-3 sentences).
- Reference the source file in `provenance[0].source` (typically `episodes/<slug>.md`).
- Never include the full narrative body from the source.

Render rules enforce this: `render_views.py` will never copy paragraphs from `episodes/*.md` into `EPISODES.md`.

### 3.4 Supersession preserves primary home

When a claim supersedes a prior claim, the new claim inherits the prior's `primaryHome` by default. Cross-home supersession (e.g., a `LTMEMORY.md` claim superseded by a `PLAYBOOKS.md` claim) is allowed but requires the new claim's `route_rationale` to explicitly justify the move.

---

## 4. Four collision-zone rules

These are the specific duplicate/conflict patterns `cluster_survivors.py` and Pass 2 must handle. Locked from spec 01.

### Zone A — `MEMORY.md` ↔ `RTMEMORY.md`

**Pattern:** the same fact appears as a factual statement in `MEMORY.md` and as reflective commentary in `RTMEMORY.md`.

**Rule:**
- Promote ONE canonical claim to `LTMEMORY.md`.
- Preserve BOTH provenance refs (direct from `MEMORY.md`, inferred from `RTMEMORY.md`).
- If the reflection is materially distinct (e.g., a lesson derived from the fact, not a restatement of it), split into two claims: one `fact` and one `lesson`, both in `LTMEMORY.md`, cross-linked via `secondary_tags`.

### Zone B — `CHRONICLES.md` ↔ `episodes/*.md`

**Pattern:** the same event is journaled personally in `CHRONICLES.md` and structured as a bounded narrative in `episodes/<slug>.md`.

**Rule (personal profile):**
- Route the event to `EPISODES.md` as a digest.
- Route any personal-significance milestone to `HISTORY.md` as a separate claim (one canonical unit per role).
- Do NOT duplicate the narrative into both homes.
- `HISTORY.md` entry may reference the `EPISODES.md` digest via `secondary_tags`.

**Rule (business profile):**
- Route to `EPISODES.md` only. Any personal-significance extraction is rejected as profile-ineligible.

### Zone C — `DREAMS.md` → `WISHES.md`

**Pattern:** dream output mixes stable aspirations with emotional residue, symbols, and one-off imagery.

**Rule:**
- Only **stable aspiration patterns** (evidenced across multiple dream entries or reinforced by `CHRONICLES.md`) route to `WISHES.md`.
- One-off emotional residue or imagery → `reject` in Pass 1.
- Ambiguous middle ground → `defer` in Pass 1; re-evaluated in reconciliation mode once more evidence accumulates.
- A single dream entry is NEVER sufficient grounds for a `WISHES.md` promotion. Cross-time persistence ≥ 0.6 is the floor.

### Zone D — `PROCEDURES.md` ↔ episodic/postmortem narrative

**Pattern:** a method is stated procedurally in `PROCEDURES.md` and repeated as part of a narrative lesson in `episodes/*.md` or `RTMEMORY.md`.

**Rule:**
- Promote the reusable method to `PLAYBOOKS.md` as the single canonical unit.
- Other surfaces' references become `secondary_tags` entries; their provenance contributes to `provenance[]` with `type: "inferred"` or `type: "merged"` as appropriate.
- The narrative claim (if it carries independent meaning beyond the method) stays in `LTMEMORY.md` as a `lesson` with `secondary_tags: ["PLAYBOOKS.md"]`.

---

## 5. Router precedence order

When Pass 2's `cluster_hints.proposed_primary_home` and the type-based routing disagree, `score_purifier.py` applies precedence in this order:

1. **Profile-scope gate** — highest. Personal-only homes are rejected on business runs regardless of any other signal.
2. **Hard constraint 3.3** — EPISODES.md stays a digest. Full narratives go elsewhere or get truncated.
3. **Collision-zone rule (§4)** — if the cluster matches a zone pattern, that rule wins over the type mapping.
4. **Type-based routing (§2)** — default when no zone rule applies.
5. **`cluster_hints.proposed_primary_home`** — a hint only; does not override any of the above.

---

## 6. Secondary tags

`secondary_tags` exist for retrieval and multi-indexing without duplication. Rules:

- Tags are strings; free-form but consistent per session.
- A tag may be a home filename (e.g., `"PLAYBOOKS.md"`) to indicate "this claim is of interest to that home's consumers".
- A tag may be a domain/topic label (e.g., `"debugging"`, `"communication"`, `"retrieval"`).
- Tags must NOT be used as a workaround to stash the claim in a second home. The single canonical unit exists once, in `primaryHome`, and only there.

---

## 7. Validator checks (for `validate_outputs.py`)

For each claim in `purified-claims.jsonl`:
- [ ] `primaryHome` is one of the five allowed values
- [ ] Personal-only `primaryHome` appears only when `profileScope == "personal"`
- [ ] `EPISODES.md` claims have `text` length ≤ digest threshold (configurable; default 500 chars) and `provenance[0].source` starts with `episodes/`
- [ ] Claims with `supersedes[]` populated have matching `prior_claim` entries elsewhere in the artifact with `status: "superseded"` and `superseded_by[]` pointing back
- [ ] `secondary_tags` does not contain the same filename as `primaryHome` (would be redundant)

Violations produce manifest `warnings[]` with the offending `claim_id` and a human-readable message.
