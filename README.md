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

## Maintenance behaviors (v1.5.0)

Production-readiness patch. Six locked contracts across runtime integrity, semantic quality, upgrade safety, and operational hygiene.

- **Single-source timezone helper.** `scripts/_lib/time_utils.timestamp_triple()` replaces every per-script helper. Emitted timestamps use explicit `zoneinfo.ZoneInfo(tz_name)` — no host-TZ drift. `discover_sources.py` gained `--timezone` override so orchestrator tz propagates into every step.
- **Transactional artifact commit with manifest-as-commit-marker.** Artifacts + views stage under `<staging>/publish/`; validation runs against the staged set; promotion is atomic in order JSONL → views → manifest LAST. `publishCommitted: true` is the single-file answer to "did this run publish?" Validation failure leaves prior runtime state untouched. `trigger_wiki` gates on `publishCommitted && status == "ok"` — defense-in-depth.
- **Real batching.** `max_candidates_per_batch` / `max_clusters_per_batch` config limits are now consumed. Oversized runs obey `oversized_run_hard_cap`; strategies `"bounded_batches"` (partial) or `"split_and_queue"` (writes `pending-candidates-<run_id>.jsonl` for the next run). Deterministic chunk boundaries via sorted-id ordering.
- **Adaptive retrieval widening.** Same-subject 30-day bonus window; contradiction-pressure widening (+3 extra slots per pressured cluster, globally capped at 30); reconciliation mode uses wider caps (120/10). Per-cluster top-K stays a hard floor — no cluster starvation.
- **Probable-duplicate detection.** Every claim carries `duplicateDisposition ∈ {reuse_existing, probable_duplicate, new_claim}` + back-pointer + reason + normalization signature. Paraphrase drift surfaces as `probable_duplicate` with pointer; wiki reconciler owns final collapse — purifier never forces it.
- **Type-home affinity validation.** Impossible pairs hard-fail (e.g., `method → HISTORY.md`). Acceptable pairs warn. Every claim carries `routeValidationState` / `routeAffinityScore` / `routeSuggestedHome` for inspection without rerunning the validator.
- **Upgrade refuse-and-lock (Contract 3).** Four-version model (`logicVersion`, `manifestSchemaVersion`, `artifactSchemaVersion`, `runtimeStateVersion`) + `lastSuccessfulLogicVersion`. Version mismatch → refuse run, write `<locks_dir>/purifier-upgrade-pending-{from}-{to}.json`, exit 2. Operator unblocks with one manual `python3 scripts/run_purifier.py --acknowledge-upgrade` — forces reconciliation, clears lock. Future cron fires resume.
- **Full cleanup matrix (Contract 6).** `run-<run_id>.lock` released on EVERY exit path. Staging preserved on failure; cleaned on `ok` / `skipped`. Pass-failure records preserved on failure. Debug retention via `--keep-staging` or `PURIFIER_DEBUG_RETAIN=1`.
- **Config-defaulted warnings.** Missing optional config fields emit `config_defaulted` warnings in `manifest.warnings[]` instead of silently defaulting.
- **Stale-sweep run-id propagation.** Stale-sweep inherits the orchestrator's run_id; no more synthetic `sweep-<uuid>`.
- **Test coverage:** 56 new regression tests (default suite 34 → 90) covering timezone correctness + DST, transactional commit + failure modes, upgrade refuse-and-lock, batching + oversized, adaptive widening + anti-starvation, duplicate dispositions, type-home affinity, cleanup matrix, config-defaulted warnings, stale-sweep lineage.

For older maintenance behavior history (v1.4.0 and earlier), see [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).

## Testing

```bash
bash tests/run_tests.sh
```

Runs the six regression scenarios (idempotency, supersession, profile routing, deletion, prior-claim ranking, downstream metric) end-to-end using real subprocess invocations of `run_purifier.py` against file-backed fixtures.

Operator procedural contract: [`SKILL.md`](SKILL.md).
Schema/contract reference: [`references/`](references/).
