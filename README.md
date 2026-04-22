# memory-purifier

**Post-consolidation, non-destructive canonicalization layer for OpenClaw memory substrates.** Sits between the consolidators (reflections + native dreaming) and the downstream wiki reconciler. Reads consolidated lower-substrate memory, emits purified machine-facing artifacts and human-facing markdown views, leaves wiki reconciliation downstream.

Non-goals: it is not a consolidator, not the reconciler, not authority control, not a turn-time layer. It fires on cron or explicit operator command only.

---

## What it does

Runs a cron-driven pipeline of deterministic scripts with two scoring passes (narrow schema-bound model calls; the orchestrating agent lives at the cron-supervisor level):

1. **Pass 1 — Promotion scoring:** for each candidate memory unit, decide survival (`reject` / `defer` / `compress` / `merge` / `promote`).
2. **Pass 2 — Canonicalization:** for each surviving cluster, assign canonical wording, one primary home, provenance, contradiction state, and freshness/confidence posture.

Scripts own orchestration, file I/O, validation, and retry. Prompts own semantic judgment only.

---

## How scoring works

Purifier is not "summarizing memory" — it runs **deterministic orchestration** around two narrow **scoring passes**. The model's job is bounded judgment within a schema; the scripts' job is everything else (sourcing, batching, validation, retry, merging, routing, manifest commit). Scores drive bounded decisions, not open-ended behavior.

### Pass 1 — Promotion scoring

For every extracted candidate memory unit, Pass 1 emits a six-dimension score profile and a single verdict. Scripts then route the candidate: survivors enter Pass 2, non-survivors land in `rejected-candidates.jsonl` / `deferred-candidates.jsonl`.

**Scoring dimensions (each ∈ [0.0, 1.0]):**

| Dimension | What it measures |
|---|---|
| `durability` | How long this memory unit stays true — minutes vs. months vs. permanent |
| `future_judgment_value` | Whether knowing this later will change a decision |
| `action_value` | Whether it unlocks or constrains specific future actions |
| `identity_relationship_weight` | Whether it anchors who/what the operator is or relates to |
| `cross_time_persistence` | Expected recurrence across unrelated contexts |
| `noise_risk` | Penalty — how likely this is a one-off artifact or transient chatter |

Strength is a deterministic re-derivation: `sum(first five) − noise_risk`. The validator recomputes it and rejects Pass 1 output whose emitted `strength` drifts more than ±0.01 from the formula — so the model can't freelance on scoring math.

**Verdicts (mutually exclusive):**

| Verdict | Meaning | Next step |
|---|---|---|
| `reject` | Noise, transient, zero downstream value | Persisted to `rejected-candidates.jsonl` for audit; never re-considered |
| `defer` | Not enough signal yet; revisit when a later candidate provides context | Persisted to `deferred-candidates.jsonl`; natural re-promotion on future runs |
| `compress` | Duplicate/redundant with another candidate in the same batch | Folded into the `compress_target` (required field); source candidate is subsumed |
| `merge` | Semantically linked with one or more siblings; becomes one cluster | Joined via `merge_candidate_ids[]`; survives together into Pass 2 |
| `promote` | Stands on its own; goes to canonicalization | Survives into Pass 2 as a single-candidate cluster |

### Pass 2 — Canonicalization and adjudication

For each surviving cluster, Pass 2 produces one canonical claim with an eight-dimension score profile plus explicit routing/reuse/supersession decisions:

| Dimension | What it measures |
|---|---|
| `semantic_cluster_confidence` | How confident we are that the clustered candidates genuinely belong together |
| `canonical_clarity` | How crisp and unambiguous the canonical wording is |
| `provenance_strength` | Quality + quantity of source pointers (direct > inferred > merged) |
| `contradiction_pressure` | How much the cluster disagrees with active prior claims |
| `freshness` | How recent the underlying material is |
| `confidence` | Model's posterior confidence in the canonical text |
| `route_fitness` | How well the chosen `primary_home` matches the claim type |
| `supersession_confidence` | If the claim supersedes a prior, how confident the link is |

### Deterministic scripts do the load-bearing decisions

The scoring passes produce structured judgment; deterministic script code downstream uses **bounded, rerunnable rules** to turn that judgment into artifact state. Examples:

- **Reuse** — `(normalized_subject, normalized_predicate, normalized_home)` triple match against active prior claims → stable id reuse; multi-match broken by most-recent `updatedAt`.
- **Probable-duplicate** — graded similarity (composite 0.4 × subject + 0.2 × predicate + 0.2 × home + 0.2 × text_jaccard + 0.1 object bonus) above a 0.80 threshold → flag as `probable_duplicate` with back-pointer; wiki reconciler decides final collapse.
- **Route affinity** — deterministic `(type, home)` table decides strong / acceptable / suspicious / impossible routing; impossible pairs hard-fail, suspicious ones warn.
- **Prior-claim retrieval for Pass 2** — per-cluster top-K hard floor + global cap + same-subject bonus window + bounded contradiction-pressure widening → rerun-deterministic.
- **Supersession-chain sanity** — warn (but don't auto-chain) when a new claim supersedes an already-superseded prior; reconciliation-mode Pass 2 resolves in its normal course.

Scores inform decisions; scripts enforce the boundaries. Neither replaces the other.

---

## Inputs

Read-only, at `<workspace>/`:

- `MEMORY.md` (OpenClaw native dreaming)
- `RTMEMORY.md`, `PROCEDURES.md`, `episodes/*.md` (reflections-hybrid)
- `CHRONICLES.md`, `DREAMS.md` (personal profile only)

Never read: `memory/*.md` (raw logs), authority docs (`CONSTITUTION.md` etc.), already-purified outputs, other packages' runtime state. Full boundary: [`references/source-contract.md`](references/source-contract.md).

---

## Outputs

**Machine artifacts** — authoritative, at `<workspace>/runtime/`:

- `purified-claims.jsonl`, `purified-contradictions.jsonl`
- `purified-entities.json`, `purified-routes.json`
- `purified-manifest.json`, `purifier-last-run-summary.json`
- `deferred-candidates.jsonl`, `rejected-candidates.jsonl`

**Human markdown views** — derived from artifact state, at `<workspace>/`:

- `LTMEMORY.md`, `PLAYBOOKS.md`, `EPISODES.md` (always)
- `HISTORY.md`, `WISHES.md` (personal profile only)

Views are regenerated every run. Do not edit them by hand.

**Telemetry + reports:**

- **Shared memory-log** (primary) — `~/.openclaw/telemetry/memory-log-YYYY-MM-DD.jsonl`, append-only. Every run appends one event with `domain: "memory"`, `component: "memory-purifier.purifier"`, and `event ∈ {run_started, run_completed, run_skipped, run_failed}`. The log is shared across memory plugins (reflections consolidator, purifier, etc.) so filters on `component` / `domain` cross-query the whole memory stack.
- **Latest-run report** — `~/.openclaw/telemetry/memory-purifier/last-run.md`, overwritten each run. Deterministic operator-facing markdown (run id, counts, warnings, downstream flag, token-usage line). Not a log — a convenience surface.
- **Package telemetry dir** — holds `last-run.md` only; no per-package JSONL is written.

**Token usage** is **scoring-pass-only**: only Pass 1 and Pass 2 model invocations contribute. Deterministic script work (discover, scope, extract, cluster, assemble, render, validate) is never counted. The runtime reports `source: "exact"` when the provider returns usage metadata (e.g. anthropic-sdk), `"approximate"` when computed from actual prompt/completion char counts, `"unavailable"` when no real model was invoked (fixture-backed runs).

**Reporting modes** (in `<workspace>/runtime/memory-state.json` under `memoryPurifier.reporting`):

- `enabled: bool` — hard gate; `false` (default) means fully silent in chat regardless of mode. Seeded at install time from `--cron-announce`.
- `mode: "silent" | "summary" | "full"` — when enabled, shapes chat output
  - `silent` — nothing in chat
  - `summary` (default) — one-line compact report
  - `full` — bullet-list report with a `Token Usage` block (omitted when `tokenUsage.source == "unavailable"`)

Telemetry and `last-run.md` are written **regardless** of reporting settings — only chat delivery is gated.

Cron delivery mode (`--no-deliver` vs announce) and `reporting.enabled` must agree for chat output to actually reach the operator. `scripts/sync_cron_delivery.py` is the single deterministic actor that reconciles drift; the cron supervisor prompts call it each fire so the next run is aligned. It reads **only** the one boolean — the full config is never loaded into the prompt's context.

---

## Runtime split

| Layer | Created when | Committed to repo? |
|---|---|---|
| Install-time seeded (control-plane JSON) | `install.sh` creates them empty-but-valid: `purifier-metadata.json`, `purified-manifest.json`, `purifier-last-run-summary.json`, `locks/` | No — seeded into the live workspace |
| First-run populated (live artifacts) | The first successful `run_purifier.py` creates them: `purified-claims.jsonl`, `purified-contradictions.jsonl`, `purified-entities.json`, `purified-routes.json`, `deferred-candidates.jsonl`, `rejected-candidates.jsonl` | No — live runtime state |
| Markdown views | Each successful run regenerates them atomically from artifact state | No — derived, not source |

---

## Cadence

Default timezone: **Asia/Manila** (overridable via `--cron-tz`). Incremental expressions exclude Wed + Sun so reconciliation owns its slot on those days without collision.

| Profile | Incremental | Reconciliation |
|---|---|---|
| `business` | `15 13 * * 1,2,4,5,6` (Mon/Tue/Thu/Fri/Sat 13:15) | `15 13 * * 3,0` (Wed/Sun 13:15) |
| `personal` | `15 5 * * 1,2,4,5,6` + `15 17 * * *` (morning excludes Wed/Sun; evening daily) | `15 5 * * 3,0` (Wed/Sun 05:15) |

Cron fires a short launcher message — `Run memory purifier. Read <prompt path> and follow every step strictly.` — pointing at the step-by-step execution prompt (`prompts/incremental-purifier-prompt.md` or `prompts/reconciliation-purifier-prompt.md`). The prompt runs `scripts/run_purifier.py` as the orchestrator. See [`references/cadence-profiles.md`](references/cadence-profiles.md).

---

## Installation

```bash
# Optional — choose where the skill package lands.
# Defaults to $HOME/.openclaw/workspace/skills/ when unset.
export SKILLS_PATH="$HOME/.openclaw/workspace/skills"

# Uses default profile (personal), tz Asia/Manila, announce=false, timeout 1200s:
curl -fsSL https://raw.githubusercontent.com/catx0rr/memory-purifier/main/install.sh | bash

# Or specify explicitly:
curl -fsSL https://raw.githubusercontent.com/catx0rr/memory-purifier/main/install.sh | \
  bash -s -- --agent-profile business --cron-tz Asia/Manila --cron-announce false --timeout-seconds 1200
```

Other path overrides follow the same pattern — `export CONFIG_ROOT=…`, `export WORKSPACE=…`, `export TELEMETRY_ROOT=…` before the `curl | bash` line. Full list in [`INSTALL.md §2`](INSTALL.md).

Installer flags:

| Flag | Default | Effect |
|---|---|---|
| `--agent-profile business\|personal` | `personal` | Seeded profile + cadence. |
| `--local` | off | Install from the directory containing `install.sh` (offline; no git). |
| `--cron-tz <IANA>` | `Asia/Manila` | Timezone for cron registration. Minimum-shape validation. |
| `--cron-announce true\|false` | `false` | `true` registers cron with OpenClaw's explicit announce delivery (`--announce --channel <channel> --to <id>`) and seeds `reporting.enabled = true`; `false` registers with `--no-deliver` (silent) and seeds `reporting.enabled = false`. |
| `--timeout-seconds <int>` | `1200` | Positive-integer per-run timeout passed to `openclaw cron add`. |
| `--skip-cron` | off | Skip cron registration. |
| `--force-config` | off | Overwrite `memory-purifier.json`; also reseeds `reporting.enabled` from `--cron-announce`. |

`install.sh` installs the package skeleton, seeds control-plane JSONs, and registers cron. It does **not** create live artifacts — follow [`INSTALL.md`](INSTALL.md) to complete first-time initialization (verify → dry-run → first live run → confirm cron).

---

## Package layout

```
memory-purifier/
├─ README.md, INSTALL.md, SKILL.md, install.sh
├─ references/           (schemas, contracts, routing + render rules)
├─ prompts/
│  ├─ incremental-purifier-prompt.md     (cron entrypoint — lean execution prompt)
│  ├─ reconciliation-purifier-prompt.md  (cron entrypoint — lean execution prompt)
│  ├─ promotion-pass.md                  (scoring sub-prompt — Pass 1, execution-oriented)
│  └─ purifier-pass.md                   (scoring sub-prompt — Pass 2, execution-oriented)
├─ scripts/              (orchestration scripts, entrypoint: run_purifier.py;
│                         also scripts/sync_cron_delivery.py for delivery drift)
└─ runtime/              (repo scaffold: .gitkeep only)
```

Prompt philosophy: cron entrypoints are strictly execution prompts (no architecture essays). Pass prompts are execution-oriented with minimum necessary enum/schema references; long explanations live in [`references/prompt-contracts.md`](references/prompt-contracts.md). Further trimming may continue as the package matures.

## Claim IDs — purifier-local bookkeeping only

Stable hash IDs in `purified-claims.jsonl` (`cl-<16-hex>`) are **purifier-local artifact identifiers**. They exist for idempotency, supersession linkage, and contradiction cluster bookkeeping within purified state. They are **not** the canonical truth identifiers used by the downstream reconciler or wiki — the reconciler mints its own identity scheme when it compiles the wiki vault. The purifier *may suggest* identity (by reusing a prior id on a semantic `(subject, predicate, primary_home)` match), but the wiki decides final cross-layer canonical identity.

## Maintenance behaviors (v1.6.0)

Emergency backend-correction patch. Scope: scoring backend default + invocation surface only. Zero purifier-architecture changes.

- **Default scoring backend is now `openclaw`.** v1.5.0 seeded `prompts.backend = "claude-code"` which assumed the Claude CLI on PATH — wrong for the OpenClaw deployment target. Fresh installs now seed `"openclaw"` at all three config surfaces (`install.sh`, `references/config-template.md`, `_lib/backend.DEFAULT_BACKEND`).
- **OpenClaw invocation uses the documented headless-inference CLI:**

  ```
  openclaw infer model run \
    --prompt "<assembled prompt + JSON payload>" \
    --json \
    [--model <provider/model>]
  ```

  The `infer model run` surface is stateless and matches the batched Pass 1 / Pass 2 scoring shape. Two earlier iterations (`openclaw prompt --session isolated` and `openclaw agent --local --session-id ...`) were wrong and have been corrected — see CHANGELOG for the iteration history.

- **Backend preflight fails loud and early.** `scripts/_lib/backend.py::preflight_backend()` runs BEFORE any subprocess invocation. If the selected backend's binary / SDK / fixture path is missing, it raises `BackendUnavailableError` with an operator-readable message naming the missing prerequisite and the config path to edit. No more opaque mid-retry `FileNotFoundError`.

- **Legacy `claude-code` config surfaces a deprecation warning.** Installs whose `prompts.backend` still carries the pre-v1.6.0 default emit `backend_deprecated_default` in `manifest.warnings[]` on every run. Silent auto-migration of on-disk config was deliberately rejected in favor of transparency. If the `claude` binary IS still installed, runs proceed; if it's missing, the preflight fails with a clear "switch to `openclaw`" instruction.

- **`MEMORY_PURIFIER_OPENCLAW_CMD` is a base-command override only.** Replaces the `openclaw` executable prefix (useful for wrapper scripts or `env VAR=X openclaw`-style prefixes), but the dispatch ALWAYS appends the required `infer model run --prompt ... --json --model?` tail. The override can never silently drop the scoring payload.

- **All four backends remain supported:** `openclaw` (default), `claude-code`, `anthropic-sdk`, `file`. Argparse choices, `_lib.backend.BACKEND_CHOICES`, and `CANONICAL_TOP_LEVEL_STATUS_SET` all single-sourced so the three contract surfaces (manifest writer, validator, orchestrator) can never drift.

- **Upgrade gate fires automatically for v1.5.0 → v1.6.0.** The logic-version bump (1.5.0 → 1.6.0) triggers the Contract 3 refuse-and-lock state machine on the first cron fire after upgrade. Operators run `python3 scripts/run_purifier.py --acknowledge-upgrade` once to clear the lock; that run forces reconciliation under v1.6.0 semantics and future cron fires resume normally.

- **New diagnostic fields on scoring output:** `backend_model` + `token_usage_source` now surface on Pass 1 / Pass 2 JSON. Top-level orchestrator report adds `backend`, `backendModel`, `tokenUsageSource` for cron-supervisor and last-run.md visibility.

- **Test coverage:** 35 new regression tests (default suite 121 → 157) covering fresh-install default, backend choice enum, preflight failures (openclaw missing, claude-code missing, bogus backend, file backend missing fixture), `openclaw infer model run` CLI contract (17 targeted assertions including base-command override, `--prompt` body, `--json` required, `--model` passthrough, envelope parsing), legacy-config deprecation warning, no-regression file-backend smoke, version-bump assertions.

For older maintenance behavior history (v1.5.0 and earlier), see [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).

Operator procedural contract: [`SKILL.md`](SKILL.md).
Schema/contract reference: [`references/`](references/).
