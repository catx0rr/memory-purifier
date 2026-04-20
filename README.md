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
  - `full` — bullet-list report with a `🪙 Token Usage` block (omitted when `tokenUsage.source == "unavailable"`)

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

## Maintenance behaviors (v1.4.0)

- **Prior-claim lookup quality improvements (capped context).** Tokenizer filters a small English stopword set. Ranker gains a predicate-match signal (+1.0 when prior predicate tokens appear in cluster text, +0.5 × Jaccard otherwise). `retrieve_prior_claims` takes per-cluster top-K (default 5) before unioning, so every cluster gets its own most-relevant priors rather than competing for a global budget. A minimum-score threshold (default 0.5) prevents noise slots. `cluster_hints.contradiction_candidates` now carries each cluster's top-3 ranked priors (Pass 2 already consumes this field). The global cap (default 50) still exists — this round raises retrieval quality within the cap, not beyond it.
- **Reworded-claim reuse hardened.** Conservative deterministic subject/predicate normalization handles leading articles, trivial plural fold (`cats` ↔ `cat`), and a small predicate morphology table (`prefers` ↔ `prefer`, `running` ↔ `run`). Multi-match picks the most-recently-updated active prior — stable across reruns because `updatedAt` is monotonic.
- **Supersession chain sanity check.** `_validate_supersession_chain` warns into `manifest.warnings[]` when a new claim supersedes a prior that is already superseded, rather than silently accepting a stale link. Does not auto-chain — reconciliation-mode Pass 2 resolves in its normal course.
- **Weighted skip recall.** `recallSurface` scores candidates by `age × 0.1 + status_weight + provenance × 0.3 + recurrence × 0.2` (age capped at 14 days so status weight dominates). A contested claim with strong provenance can outrank an old orphan. `recallScore` field is exposed in the surface for debugging.
- **Test coverage expanded.** Eight new regression tests cover announce validation, skip enrichment, weighted recall scoring, sync-helper edge cases, long-horizon prior lookup, reworded-claim reuse normalization, supersession-chain warning, and the v1.3.0 stale-sweep render bugfix (locked).
- **Self-contained docs.** Stale external-doctrine citations removed from `scripts/discover_sources.py`, `references/config-template.md`, and `references/source-contract.md`; relevant rules restated inline so the package reads standalone.

## Maintenance behaviors (v1.3.0)

- **OpenClaw announce delivery semantics.** Cron registration now uses the explicit `--announce --channel <channel> --to <id>` form when `--cron-announce true`, matching OpenClaw cron docs. `--cron-announce false` continues to use `--no-deliver`. `scripts/sync_cron_delivery.py` mirrors this when flipping delivery mode. Both `--cron-announce-channel` and `--cron-announce-to` are required non-empty when announce is on; OpenClaw owns channel validation at registration time, and the installer surfaces its errors without swallowing them.
- **Smart-skip enrichment.** Runs that skip on no-new-work (`skipped` / `skipped_superseded`) now include `claimsTotal`, `nextSchedule`, and a bounded `recallSurface` (one oldest unresolved / contested / retire_candidate claim, if any) in the deterministic summary and local `last-run.md`. Chat behavior unchanged — skips stay silent; the recall surface is local-report-only.
- **Prior-claim context is ranked, not recency-sliced.** Pass 2 receives the top-N prior claims by relevance to the current clusters (subject match, entity overlap, home affinity, text Jaccard), not the most-recent N. See [`references/prompt-contracts.md §5.6`](references/prompt-contracts.md).
- **Source removal triggers `retire_candidate`, never silent delete.** When a source file disappears from `sourceInventory`, `assemble_artifacts.py` marks claims whose provenance depends only on that source with `status: "retire_candidate"` and records a `retirementReasons[]` trace. Retired claims remain in `purified-claims.jsonl` for audit but are excluded from routes and rendered views.
- **Semantic reuse on rewording.** When Pass 2 emits `claim_id: "<new>"`, `assemble_artifacts.py` first checks for an active prior claim with matching `(subject, predicate, primary_home)`; if found, the new claim reuses that id and becomes an in-place update rather than a duplicate.
- **Runtime supersession guard.** Incremental runs that fall inside a reconciliation window (per `cadence.reconciliation[]`) skip cleanly with `status: "skipped_superseded"` regardless of cron drift.

## Testing

```bash
bash tests/run_tests.sh
```

Runs the six regression scenarios (idempotency, supersession, profile routing, deletion, prior-claim ranking, downstream metric) end-to-end using real subprocess invocations of `run_purifier.py` against file-backed fixtures.

Operator procedural contract: [`SKILL.md`](SKILL.md).
Schema/contract reference: [`references/`](references/).
