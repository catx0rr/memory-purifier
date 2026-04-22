# Changelog

All notable changes to `memory-purifier` are recorded here in reverse-chronological order. The most recent entry is also surfaced in [`README.md`](README.md) under "Maintenance behaviors"; older entries live only in this file.

---

## v1.6.0

Emergency backend-correction patch. Scope: scoring-backend default only — zero purifier architecture changes.

**Problem:** v1.5.0 seeded `prompts.backend = "claude-code"`, which assumed the Claude CLI binary on PATH. memory-purifier lives inside the OpenClaw stack where `openclaw` is the canonical execution path, so Pass 1 / Pass 2 failed before purifier semantics even ran on hosts without the Claude CLI.

**Fix:** Default backend is now `"openclaw"`. All four backends (`openclaw`, `claude-code`, `anthropic-sdk`, `file`) remain supported. Backend preflight runs BEFORE any subprocess invocation, so a missing binary fails with an operator-readable message instead of an opaque mid-retry `FileNotFoundError`.

**Changes:**

- **`scripts/_lib/backend.py`** — NEW. Single-source `DEFAULT_BACKEND = "openclaw"`, `BACKEND_CHOICES = ("openclaw", "claude-code", "anthropic-sdk", "file")`, and `preflight_backend()` which raises `BackendUnavailableError` with an explicit message when the selected backend's prerequisite (binary / SDK / fixture path) is missing.
- **`scripts/score_promotion.py`** + **`scripts/score_purifier.py`** — both scoring scripts gain:
  - `openclaw` dispatch case: invokes `openclaw infer model run --prompt <body> --json` (the documented headless-inference surface; see the dispatch contract section below for the full shape and history of the earlier mis-targeted iterations). Override via `MEMORY_PURIFIER_OPENCLAW_CMD` env var for non-standard CLI variants.
  - argparse `--backend` choices now explicit list via `_lib.backend.BACKEND_CHOICES`.
  - Preflight hook runs right after backend resolution, before the batching loop.
  - New diagnostic fields on the output JSON: `backend_model` + `token_usage_source` for operator diagnosis.
- **`scripts/run_purifier.py`** — emits `backend_deprecated_default` warning in `manifest.warnings[]` when config still carries `"claude-code"` (nudge toward migrating, without silently rewriting the config). Surfaces `backend`, `backendModel`, `tokenUsageSource` on the top-level orchestrator JSON.
- **`install.sh`** — config seed now `"backend": "openclaw"`. Version strings bumped to `"1.6.0"`.
- **`references/config-template.md`** — example + field table document `openclaw` as default.
- **`_lib/version.py`** — `PURIFIER_PACKAGE_VERSION` and `PURIFIER_LOGIC_VERSION` both bumped to `"1.6.0"`. The logic-version bump means existing v1.5.0 installs hit the **refuse-and-lock upgrade gate** (Contract 3) on next cron fire. Operator runs `python3 scripts/run_purifier.py --acknowledge-upgrade` once to clear the lock and force a reconciliation under v1.6.0 semantics.

**Migration policy chosen:** explicit-block-on-missing-binary (not silent auto-migrate of on-disk config). Rationale: silently rewriting an operator's config file is magic; preflight failing with a clear "switch `prompts.backend` to `openclaw` in memory-purifier.json" message is transparent. For installs where the legacy backend's binary IS still available, runs continue and a non-blocking `backend_deprecated_default` warning surfaces in the manifest.

**OpenClaw dispatch contract — documented ``openclaw infer model run`` headless-inference CLI:**

```
openclaw infer model run \
  --prompt "<assembled prompt + JSON payload>" \
  --json \
  [--model <provider/model>]
```

Shape notes (all from OpenClaw docs):

- Command path is **`openclaw infer model run`** — the documented headless-inference surface. Stateless by design, correct for batched Pass 1 / Pass 2 scoring where no conversational state is needed.
- Prompt body is passed via `--prompt`; stdin is NOT the documented input path for this command.
- `--json` yields a structured envelope; the dispatch parses it and extracts the model response from the first non-empty of `response` / `message` / `content` / `text` / `output` / `result`. If the envelope shape differs (override case, schema drift), raw stdout is used as fallback.
- `--model <value>` — optional provider/model override. Accepts the `provider/model` form documented by OpenClaw (e.g. `anthropic/claude-opus-4-7`). Appended only when `prompts.model` / `--model` is set. The `prompts.model` config key now also applies to the openclaw backend (in addition to `anthropic-sdk` and `claude-code`).
- **NOT passed** — agent-surface flags that are not documented on `infer model run`: `--session-id`, `--local`, `--message`, `--timeout`. The outer subprocess timeout continues to apply as a safety.

**Override hatch:** `MEMORY_PURIFIER_OPENCLAW_CMD="<base command>"` replaces the entire base command. Use when the installed `openclaw infer` variant differs from the documented shape.

**Iteration history:**

- **First v1.6.0 iteration** used `openclaw prompt --session isolated --no-deliver` — not a documented OpenClaw command. Wrong.
- **Second v1.6.0 iteration** used `openclaw agent --local --session-id ... --message ...` — the interactive agent-turn / session surface. Documented but heavier than needed for stateless scoring; correct for interactive agent flows, wrong for headless batched scoring.
- **Third (current) v1.6.0 iteration** — the above `openclaw infer model run` shape. `infer model run` is the documented headless-inference surface that reuses the agent runtime under the hood while staying stateless for batched scoring. Final for v1.6.0.

**Tests:** 18 new regression tests in `tests/test_backend_v160.py` covering: fresh-install default, backend-choice enum, preflight failures (openclaw missing, claude-code missing, bogus backend, file backend missing fixture), argparse acceptance, legacy-config deprecation warning, no-regression file backend, diagnostic-field emission. Default suite expands from 121 → 139 tests.

**Zero architecture drift:** runtime lifecycle unchanged, staged publish contract unchanged, manifest shape unchanged beyond the additive diagnostic fields, batching/widening/duplicate/route logic unchanged.

---

## v1.5.0

Production-readiness patch. Six locked contracts across runtime integrity, semantic quality, upgrade safety, and operational hygiene. 56 new regression tests; default suite expands from 34 → 90 tests.

**Runtime integrity + prod safety:**

- **Single-source timezone helper.** `scripts/_lib/time_utils.timestamp_triple()` replaces 12 duplicated helper implementations (plan anticipated 7; actual scope larger). All emitted local timestamps use explicit `zoneinfo.ZoneInfo(tz_name)` — host TZ drift no longer mislabels outputs. Ten prior bypass sites fixed, including `acquire_lock`, `_build_recall_surface`, `_next_cron_fire`, `_is_reconciliation_window`, `_build_next_schedule`. DST-sensitive regression test locks offset correctness across America/New_York DST transitions. `discover_sources.py::resolve_timezone` gained `--timezone` CLI override so orchestrator-level overrides propagate into every step's timestamps.
- **Transactional artifact commit with manifest-as-commit-marker.** Assemble + render write to `<staging>/publish/`; `validate_outputs` runs against the staged set; promotion on success is atomic in order JSONL → markdown views → manifest LAST. `publishCommitted: true` is the single-file answer to "did this run publish?" New manifest fields: `publishCommitted`, `publishedAt`, `publishedArtifactSet`, `publishedViewSet`, `downstreamWikiSignalEmitted`, `commitRunId`. Validation failure on the staged set sets `status: partial_failure` + `partialFailures[].code: transactional_commit_failed` and leaves prior runtime artifacts untouched. `trigger_wiki.py` gates on `publishCommitted && status == "ok"` — defense-in-depth against manual invocations on uncommitted manifests.
- **Real batching wired to config limits.** `score_promotion.py` and `score_purifier.py` now consume `max_candidates_per_batch` / `max_clusters_per_batch`. Batch order deterministic (sorted by candidate/cluster id). Oversized runs respect `oversized_run_hard_cap` (default 10 batches): `"bounded_batches"` processes what fits and marks `partial_failure` with `partialBatches.pending_candidate_ids`; `"split_and_queue"` also writes `pending-candidates-<run_id>.jsonl` for the next run to pick up. File-backend fixture filter lets batched pipeline tests run without per-batch hash-keyed fixtures.

**Semantic quality:**

- **Adaptive retrieval widening (Contract 5).** Three bounded widening signals layered on top of v1.4.0 per-cluster top-K retrieval: same-subject bonus window (priors with matching subject within 30 days get +1.0 bonus); contradiction-pressure widening (clusters with borderline priors get +3 extra slots, capped by global `widening_max_total_bonus = 30`); reconciliation-mode wider caps (default 120 global / 10 per-cluster). Anti-starvation hard floor preserved. Deterministic reruns.
- **Full duplicate-disposition outcome set (Contract 4).** Every claim carries `duplicateDisposition ∈ {reuse_existing, probable_duplicate, new_claim}` + `duplicateTargetClaimId` + `duplicateReason` + `normalizationSignature`. Probable-duplicate detection fires on graded similarity (composite 0.4×subject + 0.2×predicate + 0.2×home + 0.2×text_jaccard, threshold 0.80) — paraphrase drift ("operator moves" vs "operator relocates") surfaces as `probable_duplicate` status with back-pointer to the prior, not a silent new-claim mint. Wiki reconciler owns final collapse; purifier never forces it.
- **Type-home affinity validation with diagnostics (B3).** Three-level classifier: strong / acceptable / suspicious / impossible. Per-claim fields `routeValidationState`, `routeAffinityScore`, `routeSuggestedHome` emitted on every claim for operator inspection. Validator hard-fails impossible pairs (e.g., `method → HISTORY.md`), warns on suspicious, silent on strong. Route quality inspectable without rerunning the validator.

**Upgrade safety:**

- **Four-version model + dedicated `upgrade_required` status (Contract 3).** Manifest tracks `manifestSchemaVersion`, `logicVersion`, `artifactSchemaVersion`, `runtimeStateVersion` + `lastSuccessfulLogicVersion` memo. Upgrade detected on any version mismatch → refuse-and-lock: no staging dir created, no passes run, no partial progress. Writes `<locks_dir>/purifier-upgrade-pending-{from}-{to}.json`, prints operator-facing stderr with unblock instructions, exits with code 2. Operator clears with one manual invocation: `python3 scripts/run_purifier.py --acknowledge-upgrade` — that run forces `mode=reconciliation`, rebuilds state under the new logic, clears the lock. Future cron fires resume normally. Acknowledged upgrade emits `upgrade_acknowledged` warning in manifest.
- **Config-defaulted warnings.** Missing optional config fields emit `{"code": "config_defaulted", "field": ..., "default": ...}` warnings in `manifest.warnings[]` instead of silently defaulting. First tracked field: `cron.timeout_seconds`.

**Operational hygiene:**

- **Full cleanup matrix per Contract 6.** Six statuses × six cleanup targets, explicit. `run-<run_id>.lock` released on EVERY exit path (including `upgrade_required` and `partial_failure`). Staging dirs + temp files cleaned on benign statuses (`ok`, `skipped`, `skipped_superseded`), preserved on failure paths for forensics. Pass-failure records cleaned only on `ok`. Upgrade-pending locks cleared only on successful `--acknowledge-upgrade`. Debug retention via `--keep-staging` or `PURIFIER_DEBUG_RETAIN=1`.
- **Stale-sweep run-id propagation.** Stale-sweep `assemble_artifacts` invocation now accepts `--run-id` and inherits the orchestrator's run_id. No more synthetic `sweep-<uuid>` entries under orchestrated runs.
- **Shared helper extraction (minimal, correctness-only).** `scripts/_lib/` consolidates `time_utils.timestamp_triple` + `resolve_timezone`, `fs.atomic_write_json/jsonl/load_json_safe`, `version.PURIFIER_{PACKAGE,LOGIC,MANIFEST_SCHEMA,ARTIFACT_SCHEMA}_VERSION`. No broad refactor — the release stayed low-churn.

**Final-status taxonomy (Contract 1, locked):** `ok`, `skipped`, `skipped_superseded`, `partial_failure`, `failed`, `upgrade_required`. `validation_failed` kept as an edge-case backward-compat string.

**Tests:** 56 new regression tests across 13 new files (`test_timezone_correctness`, `test_timezone_dst`, `test_stale_sweep_run_id`, `test_staging_cleanup_on_skip`, `test_cleanup_matrix`, `test_upgrade_detection`, `test_upgrade_acknowledge`, `test_config_defaulted_warning`, `test_transactional_commit`, `test_publish_contract`, `test_failure_modes`, `test_batching`, `test_adaptive_widening`, `test_duplicate_dispositions`, `test_type_home_affinity`). Default suite: 34 → 90 tests.

---

## v1.4.0

- **Prior-claim lookup quality improvements (capped context).** Tokenizer filters a small English stopword set. Ranker gains a predicate-match signal (+1.0 when prior predicate tokens appear in cluster text, +0.5 × Jaccard otherwise). `retrieve_prior_claims` takes per-cluster top-K (default 5) before unioning, so every cluster gets its own most-relevant priors rather than competing for a global budget. A minimum-score threshold (default 0.5) prevents noise slots. `cluster_hints.contradiction_candidates` now carries each cluster's top-3 ranked priors (Pass 2 already consumes this field). The global cap (default 50) still exists — this round raises retrieval quality within the cap, not beyond it.
- **Reworded-claim reuse hardened.** Conservative deterministic subject/predicate normalization handles leading articles, trivial plural fold (`cats` ↔ `cat`), and a small predicate morphology table (`prefers` ↔ `prefer`, `running` ↔ `run`). Multi-match picks the most-recently-updated active prior — stable across reruns because `updatedAt` is monotonic.
- **Supersession chain sanity check.** `_validate_supersession_chain` warns into `manifest.warnings[]` when a new claim supersedes a prior that is already superseded, rather than silently accepting a stale link. Does not auto-chain — reconciliation-mode Pass 2 resolves in its normal course.
- **Weighted skip recall.** `recallSurface` scores candidates by `age × 0.1 + status_weight + provenance × 0.3 + recurrence × 0.2` (age capped at 14 days so status weight dominates). A contested claim with strong provenance can outrank an old orphan. `recallScore` field is exposed in the surface for debugging.
- **Test coverage expanded.** Eight new regression tests cover announce validation, skip enrichment, weighted recall scoring, sync-helper edge cases, long-horizon prior lookup, reworded-claim reuse normalization, supersession-chain warning, and the v1.3.0 stale-sweep render bugfix (locked).
- **Self-contained docs.** Stale external-doctrine citations removed from `scripts/discover_sources.py`, `references/config-template.md`, and `references/source-contract.md`; relevant rules restated inline so the package reads standalone.

---

## v1.3.0

- **OpenClaw announce delivery semantics.** Cron registration now uses the explicit `--announce --channel <channel> --to <id>` form when `--cron-announce true`, matching OpenClaw cron docs. `--cron-announce false` continues to use `--no-deliver`. `scripts/sync_cron_delivery.py` mirrors this when flipping delivery mode. Both `--cron-announce-channel` and `--cron-announce-to` are required non-empty when announce is on; OpenClaw owns channel validation at registration time, and the installer surfaces its errors without swallowing them.
- **Smart-skip enrichment.** Runs that skip on no-new-work (`skipped` / `skipped_superseded`) now include `claimsTotal`, `nextSchedule`, and a bounded `recallSurface` (one oldest unresolved / contested / retire_candidate claim, if any) in the deterministic summary and local `last-run.md`. Chat behavior unchanged — skips stay silent; the recall surface is local-report-only.
- **Prior-claim context is ranked, not recency-sliced.** Pass 2 receives the top-N prior claims by relevance to the current clusters (subject match, entity overlap, home affinity, text Jaccard), not the most-recent N. See [`references/prompt-contracts.md §5.6`](references/prompt-contracts.md).
- **Source removal triggers `retire_candidate`, never silent delete.** When a source file disappears from `sourceInventory`, `assemble_artifacts.py` marks claims whose provenance depends only on that source with `status: "retire_candidate"` and records a `retirementReasons[]` trace. Retired claims remain in `purified-claims.jsonl` for audit but are excluded from routes and rendered views.
- **Semantic reuse on rewording.** When Pass 2 emits `claim_id: "<new>"`, `assemble_artifacts.py` first checks for an active prior claim with matching `(subject, predicate, primary_home)`; if found, the new claim reuses that id and becomes an in-place update rather than a duplicate.
- **Runtime supersession guard.** Incremental runs that fall inside a reconciliation window (per `cadence.reconciliation[]`) skip cleanly with `status: "skipped_superseded"` regardless of cron drift.
