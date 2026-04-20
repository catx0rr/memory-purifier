# SKILL.md — Memory Purifier Runtime Contract

**Procedural guide for the agent running memory-purifier tasks.** When to fire, what mode, what surfaces, where output goes.

---

## Fire conditions

Cron or operator command only. Never turn-by-turn, never on memory writes.

| Trigger | Mode | Condition |
|---|---|---|
| Cron incremental | `incremental` | Business 13:15 Mon/Tue/Thu/Fri/Sat; personal 05:15 Mon/Tue/Thu/Fri/Sat + 17:15 daily. Cron fires `prompts/incremental-purifier-prompt.md`. |
| Cron reconciliation | `reconciliation` | Business 13:15 Wed/Sun; personal 05:15 Wed/Sun. Cron fires `prompts/reconciliation-purifier-prompt.md`. |
| Manual | either | `python3 scripts/run_purifier.py --mode <mode> [--dry-run]` |

## Do not fire

- On conversation turns.
- When a lock exists in `<workspace>/runtime/locks/run.lock` and is less than `stale_lock_hours` old (default 2h).
- When `discover_sources.py` returns zero inputs — exit as `skipped`.

## Surfaces

Inputs (read-only): `MEMORY.md`, `RTMEMORY.md`, `PROCEDURES.md`, `episodes/*.md`; plus `CHRONICLES.md`, `DREAMS.md` on personal profile. Full contract: [`references/source-contract.md`](references/source-contract.md).

Never read: `memory/*.md`, authority docs, already-purified outputs.

Outputs:
- machine artifacts at `<workspace>/runtime/`
- markdown views at `<workspace>/`
- shared memory-log telemetry at `~/.openclaw/telemetry/memory-log-YYYY-MM-DD.jsonl` (append-only; `component: "memory-purifier.purifier"`)
- latest-run report at `~/.openclaw/telemetry/memory-purifier/last-run.md` (overwritten each run)

Token usage is scoring-pass-only (Pass 1 + Pass 2); never counts deterministic script work. Source is `exact` / `approximate` / `unavailable` per run.

## Execution order

Cron fires a short launcher message (`Run memory purifier. Read <prompt path> and follow every step strictly.`) that points at the appropriate execution prompt (`prompts/incremental-purifier-prompt.md` or `prompts/reconciliation-purifier-prompt.md`). The prompt reads paths, reconciles cron delivery state via `scripts/sync_cron_delivery.py` (for the NEXT run), and runs `scripts/run_purifier.py`. The runner chains:

1. Acquire lock, resolve profile + cursor
2. `discover_sources.py` → `select_scope.py` → `extract_candidates.py`
3. `score_promotion.py` → `cluster_survivors.py` → `score_purifier.py`
4. `assemble_artifacts.py` → `render_views.py`
5. `write_manifest.py` → `validate_outputs.py` → `trigger_wiki.py`
6. Release lock

Machine artifacts precede markdown views. Validation runs before downstream wiki handoff.

## Output discipline

- **Quiet.** No chat narration. Manifest and telemetry carry the state.
- Warnings and partial failures go into `manifest.warnings[]` and `manifest.partialFailures[]`.
- Never modify lower-substrate inputs. Never bypass the profile gate.

## Modes

- `incremental` — delta since `lastSuccessfulCursor`; cheap, recurring.
- `reconciliation` — full inventory, widened horizon; re-cluster, repair supersession, re-score against new evidence.
- `--dry-run` (flag) — every phase runs; no files persisted.

## Manual execution

```bash
# Dry-run preview
python3 "$SKILL_ROOT/scripts/run_purifier.py" --mode incremental --dry-run

# Wet run
python3 "$SKILL_ROOT/scripts/run_purifier.py" --mode incremental

# Reconciliation
python3 "$SKILL_ROOT/scripts/run_purifier.py" --mode reconciliation

# Profile / workspace override
python3 "$SKILL_ROOT/scripts/run_purifier.py" \
    --mode incremental --workspace "$HOME/alt-workspace" --profile personal
```

## Inspection

```bash
cat "$WORKSPACE/runtime/purifier-last-run-summary.json" | jq
cat "$WORKSPACE/runtime/purified-manifest.json" | jq
tail -n 5 "$HOME/.openclaw/telemetry/memory-log-$(date +%Y-%m-%d).jsonl" | jq 'select(.component == "memory-purifier.purifier")'
cat "$HOME/.openclaw/telemetry/memory-purifier/last-run.md"
python3 "$SKILL_ROOT/scripts/validate_outputs.py" --workspace "$WORKSPACE"
```

## Failure handling

| Condition | Behavior |
|---|---|
| Lock present, not stale | Exit `skipped`; no artifact writes. |
| Zero inputs | Exit `skipped`. |
| Pass 1 or Pass 2 schema-invalid response | One retry; on second failure, raw response written to `locks/failed-<pass>-<run_id>.json`, `partial_failure` recorded, cursor not advanced. |
| Artifact write failure | Render skipped; previous views remain intact. |
| Validate errors | Manifest updated to reflect; downstream wiki signal suppressed. |

## See also

- [README.md](README.md) — package overview
- [INSTALL.md](INSTALL.md) — install + first-time initialization
- [references/](references/) — source, cadence, routing, render, config, prompt contracts
- [prompts/](prompts/) — cron entrypoints (`incremental-purifier-prompt.md`, `reconciliation-purifier-prompt.md`) + Pass-1/Pass-2 scoring sub-prompts (`promotion-pass.md`, `purifier-pass.md`)
