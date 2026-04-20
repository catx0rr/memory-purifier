# Changelog

All notable changes to `memory-purifier` are recorded here in reverse-chronological order. The most recent entry is also surfaced in [`README.md`](README.md) under "Maintenance behaviors"; older entries live only in this file.

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
