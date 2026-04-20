# Incremental Purifier — Cron Execution Prompt

You are running an **incremental** memory-purifier cycle. Execute the deterministic pipeline quietly and report only the final status from the runner's own JSON output.

---

## Reporting contract (read first)

- Run `scripts/run_purifier.py` for the requested mode and profile.
- Use the runner's **final JSON result** as the authoritative success/report source. Parse its `ok`, `status`, `claimsNew`, `claimsTotal`, `contradictionCount`, `supersessionCount`, `warningCount`, `partialFailureCount`, `downstreamWikiIngestSuggested` fields.
- Do not inspect intermediate step output unless the final JSON is missing, unparseable, or malformed.
- Do not narrate progress. Do not explain architecture. Do not retry.
- Only emit a compact final report in chat if reporting is enabled (see §Step 3).

## Role

Supervise the orchestrator. Do not narrate progress. Do not explain architecture. Do not retry failed runs.

## Resolve paths

| Variable | Order (first non-empty wins) |
|---|---|
| `SKILL_ROOT` | `$SKILL_ROOT` env → `$HOME/.openclaw/workspace/skills/memory-purifier` |
| `WORKSPACE` | `$WORKSPACE` env → `$HOME/.openclaw/workspace` |
| `CONFIG` | `$HOME/.openclaw/memory-purifier/memory-purifier.json` |
| `TELEMETRY_ROOT` | `$TELEMETRY_ROOT` env → `$HOME/.openclaw/telemetry/memory-purifier` |

## Step 1 — Invoke the orchestrator

Run this exact command. Capture stdout as a single JSON object.

```bash
python3 "$SKILL_ROOT/scripts/run_purifier.py" \
    --mode incremental \
    --workspace "$WORKSPACE" \
    --config "$CONFIG" \
    --telemetry-root "$TELEMETRY_ROOT"
```

`run_purifier.py` chains: discover → scope → extract → Pass 1 → cluster → Pass 2 → assemble → render → manifest → validate → trigger. It always emits exactly one final JSON object even on skip/failure.

## Step 2 — Read the final report

Parse the JSON. The `status` field is authoritative:

| Status | Meaning | Action |
|---|---|---|
| `ok` | Pipeline completed, validate passed, downstream signalled | Exit silently (or one-line summary per Step 3) |
| `skipped` | Nothing to do (no new inputs, or another run is locked) | Exit silently |
| `skipped_superseded` | Incremental run fell inside a reconciliation window and was skipped by the runtime guard | Exit silently |
| `validation_failed` | Pipeline completed but `validate_outputs` reported errors; downstream signal suppressed | Report the `haltReason` or `validate.errorCount` in one line |
| `partial_failure` | Pass 1 or Pass 2 produced invalid output; cursor not advanced; failure record under `locks/failed-*.json` | Report `haltReason` in one line |
| `error` | A fundamental step failed | Report `haltReason` in one line |

## Step 2.5 — Sync cron delivery for the NEXT run (token-thrifty)

Before chat delivery, reconcile cron registration with `memoryPurifier.reporting.enabled` so future runs obey the current toggle. Read **only** the one boolean — do **not** load the full config into the model context.

```bash
REPORTING_ENABLED="$(python3 -c "import json,sys; d=json.load(open('$WORKSPACE/runtime/memory-state.json')); print(str(d.get('memoryPurifier',{}).get('reporting',{}).get('enabled', False)).lower())" 2>/dev/null || echo false)"
python3 "$SKILL_ROOT/scripts/sync_cron_delivery.py" --workspace "$WORKSPACE" > /dev/null 2>&1 || true
```

Notes:
- The helper is the single deterministic actor that mutates cron delivery state. Do **not** hand-rebuild `openclaw cron` commands from this prompt.
- Any sync error is non-fatal — proceed with this run's actual delivery behavior (decided by whatever the cron scheduler already chose when it fired).
- Drift is corrected for the NEXT fire, not this one.

## Step 3 — Chat delivery (static templates)

This step uses only the `REPORTING_ENABLED` captured in Step 2.5. If you need the mode (`silent` / `summary` / `full`), read it with a narrow jq/python query — never load the whole memory-state file:

```bash
REPORTING_MODE="$(python3 -c "import json; d=json.load(open('$WORKSPACE/runtime/memory-state.json')); print(d.get('memoryPurifier',{}).get('reporting',{}).get('mode','summary'))" 2>/dev/null || echo summary)"
```

- `REPORTING_ENABLED == false` → emit nothing in chat. Telemetry and manifest still record the run.
- `REPORTING_ENABLED == true`, `REPORTING_MODE == "silent"` → emit nothing in chat.
- `REPORTING_ENABLED == true`, `REPORTING_MODE == "summary"` → emit the `ok` summary line below (one line) on `ok`.
- `REPORTING_ENABLED == true`, `REPORTING_MODE == "full"` → emit the `ok` full template below (up to three lines) on `ok`.
- Any halt state (`validation_failed`, `partial_failure`, `error`) always emits the halt template when reporting is enabled, regardless of mode.
- `skipped` and `skipped_superseded` never emit anything in chat, regardless of mode.

All summary values **must** come from the runner's final JSON (or `$WORKSPACE/runtime/purifier-last-run-summary.json` as a deterministic fallback). Do not recompute. Do not improvise alternative wording.

### Templates

`ok` + `summary` — exactly one line:

```
♨️ Memory purifier incremental completed. {claimsNew} new, {claimsTotal} total.
```

`ok` + `full` — multi-line bullet report. Omit the entire `🪙 Token Usage` block (header + three bullets) when `tokenUsage.source == "unavailable"`:

```
♨️ Memory purifier incremental completed.
• Claims: {claimsNew} new · {claimsTotal} total
• Supersessions: {supersessionCount}
• Contradictions: {contradictionCount}
• Wiki ingest suggested: {downstreamWikiIngestSuggested}
🪙 Token Usage
• Prompt: {tokenUsage.prompt_tokens}
• Completion: {tokenUsage.completion_tokens}
• Total: {tokenUsage.total_tokens} ({tokenUsage.source})
```

`validation_failed` / `partial_failure` / `error` — exactly two lines in every mode. Do not add claims/supersession/contradiction/token lines on halt:

```
♨️ Memory purifier incremental halted.
🛑 Reason: {haltReason}
```

`skipped` / `skipped_superseded` — emit nothing in chat, regardless of mode. Telemetry and the manifest still record the run.

Substitute field values verbatim from the final JSON. Use `true`/`false` for `downstreamWikiIngestSuggested`. Do not add prose before or after the template lines.

## Do not

- Do not compute counts yourself — read them from the runner's final JSON.
- Do not call `discover_sources.py`, `score_promotion.py`, or any sub-script individually.
- Do not retry partial failures. The next cron fire will re-process.
- Do not write to `$WORKSPACE/MEMORY.md`, `RTMEMORY.md`, `PROCEDURES.md`, or `episodes/*.md`.
- Do not modify `$CONFIG` without explicit operator instruction.
- Do not narrate intermediate progress.
