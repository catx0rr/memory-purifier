# Reconciliation Purifier — Cron Execution Prompt

You are running a **reconciliation** memory-purifier cycle. Execute the deterministic pipeline quietly and report only the final status from the runner's own JSON output.

Reconciliation widens the processing horizon: the full source inventory is re-evaluated against prior purified claims for contradiction repair, supersession chain updates, and re-scoring.

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
    --mode reconciliation \
    --workspace "$WORKSPACE" \
    --config "$CONFIG" \
    --telemetry-root "$TELEMETRY_ROOT"
```

Reconciliation differs from incremental at two points:
- **Scope**: ignores the incremental cursor; re-reads the full inventory.
- **Pass 2 context**: loads `prior_claims_context` from `$WORKSPACE/runtime/purified-claims.jsonl` so Pass 2 can detect supersession and re-score against prior canonical state.

Everything else (assemble, render, manifest, validate, trigger) is identical to incremental.

## Step 2 — Read the final report

Parse the JSON. The `status` field is authoritative:

| Status | Meaning | Action |
|---|---|---|
| `ok` | Full horizon re-evaluated; supersession / contradictions repaired; artifacts + views rewritten | Exit silently (or one-line summary per Step 3) |
| `skipped` | Another run was locked, or there are no inputs | Exit silently |
| `validation_failed` | Pipeline completed but `validate_outputs` reported errors | Report the `haltReason` or `validate.errorCount` in one line |
| `partial_failure` | Pass 1 or Pass 2 invalid response; cursor preserved | Report `haltReason` in one line |
| `error` | A fundamental step failed | Report `haltReason` in one line |

Note: `skipped_superseded` does not apply to reconciliation runs — the runtime guard only skips incremental mode.

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
- `skipped` never emits anything in chat, regardless of mode.

All summary values **must** come from the runner's final JSON (or `$WORKSPACE/runtime/purifier-last-run-summary.json` as a deterministic fallback). Do not recompute. Do not improvise alternative wording. The word **reconciliation** is mandatory — do not swap it for "incremental".

### Templates

`ok` + `summary` — exactly one line:

```
⚗️ Memory purifier reconciliation completed. {claimsNew} new, {claimsTotal} total.
```

`ok` + `full` — multi-line bullet report. Omit the entire `🪙 Token Usage` block (header + three bullets) when `tokenUsage.source == "unavailable"`:

```
⚗️ Memory purifier reconciliation completed.
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
⚗️ Memory purifier reconciliation halted.
🛑 Reason: {haltReason}
```

`skipped` — emit nothing in chat, regardless of mode. Telemetry and the manifest still record the run.

Substitute field values verbatim from the final JSON. Use `true`/`false` for `downstreamWikiIngestSuggested`. Do not add prose before or after the template lines.

## Do not

- Do not compute counts yourself — read them from the runner's final JSON.
- Do not call sub-scripts individually — `run_purifier.py` chains them.
- Do not retry. Reconciliation runs twice a week; the next fire will resume.
- Do not write to lower-substrate inputs.
- Do not narrate intermediate progress.
