# INSTALL.md — Guided First-Time Initialization

**Follow this doc top-to-bottom, pause at each `❓` prompt for a short answer, and run the commands as-written using deterministic values only.**

---

## 0. Prerequisites

- OpenClaw installed, `openclaw` CLI on `PATH`.
- `python3` available (no third-party packages required).
- `jq` recommended for inspection.
- Upstream consolidators active: [`reflections-hybrid`](https://github.com/catx0rr/reflections-hybrid) and OpenClaw native dreaming.

> ❓ **Do you want me to install memory-purifier now, or read what it is for first and give insights?**
>
> - **Install now** → continue to §0.1.
> - **Explain first** → summarize [`README.md`](README.md) (post-consolidation, non-destructive canonicalization; two-pass scoring with an agent supervisor on top; reads `MEMORY.md`/`RTMEMORY.md`/`PROCEDURES.md`/`episodes/`; emits `LTMEMORY.md`/`PLAYBOOKS.md`/`EPISODES.md`), then re-ask this question.

---

## 0.1. Skip-check — is it already installed?

Run this detection before §1. If the operator ran `install.sh` earlier, §1 and §2 can be skipped.

```bash
SKILLS_PATH="${SKILLS_PATH:-$HOME/.openclaw/workspace/skills}"
SKILL_ROOT="$SKILLS_PATH/memory-purifier"
CONFIG_ROOT="${CONFIG_ROOT:-$HOME/.openclaw}"
WORKSPACE="${WORKSPACE:-$HOME/.openclaw/workspace}"

DETECTED=0
[ -f "$SKILL_ROOT/SKILL.md" ] && DETECTED=$((DETECTED+1))
[ -f "$CONFIG_ROOT/memory-purifier/memory-purifier.json" ] && DETECTED=$((DETECTED+1))
[ -f "$WORKSPACE/runtime/purifier-metadata.json" ] && DETECTED=$((DETECTED+1))
python3 -c "import json; d=json.load(open('$WORKSPACE/runtime/memory-state.json')); exit(0 if 'memoryPurifier' in d else 1)" 2>/dev/null && DETECTED=$((DETECTED+1))
CRON_COUNT="$(openclaw cron list --json 2>/dev/null | python3 -c "import json,sys; print(sum(1 for j in json.load(sys.stdin) if j.get('name','').startswith('memory-purifier-')))" 2>/dev/null || echo 0)"

printf 'components_detected=%s/4\ncron_jobs_registered=%s\n' "$DETECTED" "$CRON_COUNT"
```

- `components_detected=0/4` and `cron_jobs_registered=0` → fresh system; continue to §1.
- `components_detected>=2` or `cron_jobs_registered>=1` → memory-purifier is already installed.

### If already installed

> ❓ **Memory purifier is already installed on this system (detected {components_detected}, {cron_jobs_registered} cron jobs). Do you still want me to run INSTALL.md?**
>
> - **No** → skip §1 and §2 (the installer). Proceed to §3 (Verify seeded runtime), §4 (Reflections gate), §5 (First live incremental), and §8 (optional first reconciliation).
> - **Yes** → continue to §1. `install.sh` is idempotent — existing config is preserved unless `--force-config` is passed.

---

## 1. Gated install questions

Ask the operator the questions below in order, one at a time, then pass the answers to `install.sh`. When announce is enabled, ask two follow-ups for channel + target; when disabled, skip them.

> ❓ **Which agent profile do you want: `business` or `personal`?**  _(default: `personal`)_
>
> ❓ **Which IANA timezone should I use for cron configuration?**  _(default: `Asia/Manila`; examples: `America/Los_Angeles`, `Europe/Berlin`, `Etc/UTC`)_
>
> ❓ **Do you want cron announce enabled (reports appear in chat) or no-deliver (silent)?**  _(default: `no-deliver` = `false`)_
>
> If announce enabled, ask:
>
> ❓ **What channel should I announce to?**  _(examples: `whatsapp`, `telegram`, `last`; OpenClaw owns channel validity at cron-add time)_
>
> ❓ **What target ID should I announce to?**  _(host-specific peer/group id, non-empty)_
>
> ❓ **Do you want to use the default timeout of 1200 seconds, or specify a custom timeout?**  _(default: `1200`)_

Map the answers:

| Answer | Installer flag |
|---|---|
| profile | `--agent-profile <business\|personal>` |
| timezone | `--cron-tz <IANA>` |
| announce = enabled | `--cron-announce true` |
| announce = no-deliver | `--cron-announce false` |
| announce channel | `--cron-announce-channel <channel>` _(required when announce=true)_ |
| announce target | `--cron-announce-to <id>` _(required when announce=true)_ |
| custom timeout | `--timeout-seconds <int>` |

---

## 2. Run the installer

```bash
# specify your skill root defaults to workspace/skills/ directory
export SKILLS_PATH="$HOME/.openclaw/workspace/skills"
SKILL_ROOT="$SKILLS_PATH/memory-purifier"

# Remote install — announce disabled (silent; default).
# `--non-interactive` is required: the agent-facing INSTALL flow already asked
# the gated questions, so the installer must not stop for its own y/N prompt.
curl -fsSL https://raw.githubusercontent.com/catx0rr/memory-purifier/main/install.sh | \
  bash -s -- \
    --non-interactive \
    --agent-profile <business|personal> \
    --cron-tz <IANA> \
    --cron-announce false \
    --timeout-seconds <int>

# Remote install — announce enabled (chat-delivering; channel + target required)
curl -fsSL https://raw.githubusercontent.com/catx0rr/memory-purifier/main/install.sh | \
  bash -s -- \
    --non-interactive \
    --agent-profile <business|personal> \
    --cron-tz <IANA> \
    --cron-announce true \
    --cron-announce-channel <channel> \
    --cron-announce-to <id> \
    --timeout-seconds <int>

# Or from a local checkout — omit --non-interactive if you want the y/N preview.
# Append --cron-announce-channel / --cron-announce-to when announce=true.
cd <path-to-package>
bash install.sh --agent-profile <business|personal> --cron-tz <IANA> --cron-announce false --timeout-seconds <int>
```

The installer reads `SKILLS_PATH` from the environment, so `export`-ing it before the `curl | bash` line above redirects where the package lands. Same pattern applies to the other env-var overrides listed below.

### CLI flag reference

| Flag | Default | Effect |
|---|---|---|
| `--agent-profile business\|personal` | `personal` | Seeded profile + cadence. |
| `--local` | off | Install from the directory containing `install.sh` (offline; no git). |
| `--cron-tz <IANA>` | `Asia/Manila` | Timezone for cron registration. Shape-validated before use. |
| `--cron-announce true\|false` | `false` | `true` = cron `--announce --channel <X> --to <Y>` + `reporting.enabled=true`; `false` = cron `--no-deliver` + `reporting.enabled=false`. |
| `--cron-announce-channel <channel>` | _(none)_ | Required non-empty when `--cron-announce true`. Delivery channel (e.g. `whatsapp`, `telegram`, `last`). OpenClaw owns validity. |
| `--cron-announce-to <id>` | _(none)_ | Required non-empty when `--cron-announce true`. Delivery target (peer/group id). |
| `--timeout-seconds <int>` | `1200` | Positive-int per-run timeout. |
| `--skip-cron` | off | Skip cron registration. |
| `--force-config` | off | Overwrite `memory-purifier.json`; also reseeds `reporting.enabled` + delivery from install flags. |
| `--non-interactive` | off | Skip the y/N install-plan confirmation (also auto-set when stdin is not a TTY). |

### Path overrides (env vars)

| Env var | Default |
|---|---|
| `CONFIG_ROOT` | `$HOME/.openclaw` |
| `WORKSPACE` | `$HOME/.openclaw/workspace` |
| `SKILLS_PATH` | `$HOME/.openclaw/workspace/skills` |
| `TELEMETRY_ROOT` | `$HOME/.openclaw/telemetry/memory-purifier` |

### What `install.sh` does

1. Clones/updates `$SKILL_ROOT` (or uses an in-place checkout when `--local`).
2. Creates `$WORKSPACE/runtime/` and `locks/`.
3. Creates `$CONFIG_ROOT/memory-purifier/` and `$TELEMETRY_ROOT/`.
4. Seeds `memory-purifier.json` with profile cadence + `cron` block (`tz`, `timeout_seconds`, `announce`, `announce_channel`, `announce_to`).
5. Seeds control-plane JSONs: `purifier-metadata.json`, `purified-manifest.json`, `purifier-last-run-summary.json`.
6. Merges `memoryPurifier` namespace into `$WORKSPACE/runtime/memory-state.json` — seeding `reporting.enabled` from `--cron-announce` and `delivery.{channel,to}` from the announce channel/target (or `last` / `null` when announce=false).
7. Registers cron jobs with a short **launcher message** pointing at the correct prompt file. Delivery form is explicit per OpenClaw docs: `--announce --channel <X> --to <Y>` when announce=true, `--no-deliver` when announce=false. Reconciliation owns Wed + Sun via day-of-week exclusion.
8. Verifies all seeded files parse; prints next-step instructions pointing back here.

The installer does **not** create live artifacts — those come from the first live run (§5).

### Cron launcher message

Registration passes a short launcher — not the prompt body:

```
Run memory purifier.

Read `<abs-path>/prompts/incremental-purifier-prompt.md` and follow every step strictly.
```

### Cron delivery ↔ reporting.enabled sync

`--cron-announce` seeds both the cron delivery form (`--announce --channel <X> --to <Y>` or `--no-deliver`) and `reporting.enabled` consistently. Drift later is reconciled by [`scripts/sync_cron_delivery.py`](scripts/sync_cron_delivery.py), called by the cron supervisor prompts each fire. The helper reads memory-state's `reporting.{enabled, delivery.channel, delivery.to}` and falls back to `cron.announce_channel` / `cron.announce_to` in the config; it **refuses to mutate** and records `skipped_missing_announce_target` if flipping to announce while channel or target are empty in both sources. That helper is the single deterministic actor mutating cron delivery after install.

---

## 3. Verify the seeded runtime

```bash
# specify your skill root defaults to workspace/skills/ directory
SKILLS_PATH="${SKILLS_PATH:-$HOME/.openclaw/workspace/skills}"
SKILL_ROOT="$SKILLS_PATH/memory-purifier"
CONFIG_ROOT="${CONFIG_ROOT:-$HOME/.openclaw}"
WORKSPACE="${WORKSPACE:-$HOME/.openclaw/workspace}"
RUNTIME_DIR="$WORKSPACE/runtime"

ls -la "$RUNTIME_DIR/" "$RUNTIME_DIR/locks/"
jq '.profile, .cadence, .cron' "$CONFIG_ROOT/memory-purifier/memory-purifier.json"
python3 -c "import json; print('ok' if 'memoryPurifier' in json.load(open('$WORKSPACE/runtime/memory-state.json')) else 'missing')"
```

Expected: seeded JSONs present, config parses, `memoryPurifier` namespace merged. Live artifacts (`purified-claims.jsonl`, etc.) are **not yet expected**.

---

## 4. Reflections-aware test gate

Detect whether `$WORKSPACE/RTMEMORY.md` and `$WORKSPACE/PROCEDURES.md` both exist.

```bash
if [ -f "$WORKSPACE/RTMEMORY.md" ] && [ -f "$WORKSPACE/PROCEDURES.md" ]; then
    echo "both present"
else
    echo "one or both missing"
fi
```

### If both are present

> ❓ **I detected RTMEMORY.md and PROCEDURES.md. Do you want me to test memory-purifier in a disposable workspace first and show the results?**
>
> - **Yes** → run the disposable-workspace test below, report the summary JSON, then re-ask whether to continue with real initialization.
> - **No** → skip to §5.

```bash
# Disposable test (does not touch the live workspace)
export TEST_WS="/tmp/mp-first-run"
mkdir -p "$TEST_WS/episodes"
echo "Operator prefers terse responses." > "$TEST_WS/MEMORY.md"
echo "# RTMEMORY" > "$TEST_WS/RTMEMORY.md"
echo "# PROCEDURES" > "$TEST_WS/PROCEDURES.md"

python3 "$SKILL_ROOT/scripts/run_purifier.py" \
    --mode incremental --workspace "$TEST_WS" --profile business --dry-run | jq '.status, .steps'
```

Expected: `"status": "ok"` with each step `status: "ok"` or `"skipped"`. If anything reports `error` or `partial_failure`, stop and inspect.

### If one or both are missing

> **I did not detect RTMEMORY.md and PROCEDURES.md. Install [`reflections-hybrid`](https://github.com/catx0rr/reflections-hybrid) first if you want memory-purifier to complement it.**

Skip to §5 without the disposable-workspace test.

---

## 5. First live incremental initialization

```bash
python3 "$SKILL_ROOT/scripts/run_purifier.py" --mode incremental
```

Reads live consolidated substrate, runs both scoring passes, writes artifacts, renders markdown views, writes manifest, validates, signals downstream. On success the orchestrator prints a summary JSON with `"status": "ok"`.

---

## 6. Verify live artifacts

```bash
ls -la "$RUNTIME_DIR/"
# Expect:
#   purified-claims.jsonl, purified-contradictions.jsonl, purified-entities.json,
#   purified-routes.json, purified-manifest.json, purifier-last-run-summary.json
#   (deferred-candidates.jsonl, rejected-candidates.jsonl if any)

jq '.status, .runId, .lastSuccessfulCursor, .downstreamWikiIngestSuggested' "$RUNTIME_DIR/purified-manifest.json"
head -n 5 "$WORKSPACE/LTMEMORY.md" "$WORKSPACE/PLAYBOOKS.md" "$WORKSPACE/EPISODES.md"
# Personal profile also: "$WORKSPACE/HISTORY.md" "$WORKSPACE/WISHES.md"
```

Expected: `status: "ok"`, populated `lastSuccessfulCursor`, `downstreamWikiIngestSuggested: true`, each markdown view beginning with the header + regeneration line.

---

## 7. Confirm cron is registered

```bash
openclaw cron list --json | jq '.[] | select(.name | startswith("memory-purifier"))'
```

Expected — business profile: `memory-purifier-incremental`, `memory-purifier-reconciliation`. Personal profile: plus `memory-purifier-incremental-evening`. Each has `cron`, the chosen `tz`, `session: "isolated"`, the chosen `deliver` flag, `timeout_seconds`, and a short launcher message referencing the prompt file.

Manual registration (only needed if `--skip-cron` was used): see [`references/cadence-profiles.md §5`](references/cadence-profiles.md).

---

## 8. Optional first reconciliation

After §5 succeeds, the first reconciliation round can be invoked on demand rather than waiting for the Wed/Sun slot.

> ❓ **Do you want me to run the reconciliation once now?**
>
> - **Yes** → run the command below, then render the first-init report template from the final JSON.
> - **No** → stop here; installation is complete. The next reconciliation will still fire automatically on its cron schedule (business / personal both: `15 13 * * 3,0` or `15 5 * * 3,0` respectively). Notify the operator with a line like:
>
>   ```
>   🔄 Skipping the one-off reconciliation. The next scheduled {incremental|reconciliation} run will fire at {next cron execution schedule}.
>   ```
>
>   Fill the values deterministically from `openclaw cron list --json` (earliest upcoming fire across the `memory-purifier-*` jobs + its mode label).

```bash
python3 "$SKILL_ROOT/scripts/run_purifier.py" --mode reconciliation | tee /tmp/mp-recon-first-init.json | jq '.status'
```

Expected: `"status": "ok"` with full-horizon re-read and any supersession/contradiction state updated.

### First reconciliation initialization report (static template)

Emit this exactly once, after the first successful reconciliation. Fill values **only from the final JSON** (or `$WORKSPACE/runtime/purifier-last-run-summary.json` as deterministic fallback). Omit the entire `🪙 Token Usage` block when `tokenUsage.source == "unavailable"`.

```
⚗️ First Memory purifier reconciliation initialized!
• Claims: {claimsNew} new · {claimsTotal} total
• Supersessions: {supersessionCount}
• Contradictions: {contradictionCount}
• Wiki ingest suggested: {downstreamWikiIngestSuggested}
🪙 Token Usage
• Prompt: {tokenUsage.prompt_tokens}
• Completion: {tokenUsage.completion_tokens}
• Total: {tokenUsage.total_tokens} ({tokenUsage.source})

🔄 Next memory purifier schedule:
• {next cron execution schedule}
• {type of cron that will execute next}
```

Resolve the next schedule deterministically from `openclaw cron list --json`, picking the earliest upcoming fire across the `memory-purifier-*` jobs and labeling its mode (`incremental` vs `reconciliation`). Do not improvise alternative wording.

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `install.sh` fails at cron step | `openclaw` on PATH? Try `--skip-cron` then register manually per [`references/cadence-profiles.md §5`](references/cadence-profiles.md). |
| Dry-run returns `status: skipped` at scope | `$WORKSPACE/MEMORY.md` or `RTMEMORY.md` present with content? |
| Personal views not emitting on personal profile | `personal_surfaces.enabled: true` in config? `CHRONICLES.md` / `DREAMS.md` present? |
| Duplicate claims on reruns | `lastSuccessfulCursor` populated in manifest? Claim ids are content-hashed — reruns with unchanged inputs must not produce duplicates. |
| Cron registration refuses | Existing entry with same name? `openclaw cron delete --name memory-purifier-incremental` then retry. |
| Chat delivery toggled but nothing posts | Cron `deliver` flag still `--no-deliver`? The next cron fire runs `sync_cron_delivery.py` and reconciles — the fire after that should deliver. |

Deeper issues: inspect

- `$RUNTIME_DIR/purified-manifest.json` — `warnings[]`, `partialFailures[]` for the latest run
- `$RUNTIME_DIR/locks/failed-*.json` — raw model responses on Pass 1 / Pass 2 validation failure
- `$HOME/.openclaw/telemetry/memory-log-YYYY-MM-DD.jsonl` — shared memory-log; filter with `jq 'select(.component == "memory-purifier.purifier")'`
- `$TELEMETRY_ROOT/last-run.md` — deterministic human-readable snapshot of the last run

**Telemetry shape:** every event carries `domain: "memory"`, `component: "memory-purifier.purifier"`, `event ∈ {run_started, run_completed, run_skipped, run_failed}`, plus a `token_usage` block. Token usage counts only Pass 1 + Pass 2 scoring calls — `exact` when the provider returns usage metadata, `approximate` when computed from char counts, `unavailable` when no real model was invoked (fixture runs, `run_started` events).

**Chat reporting** lives in `<workspace>/runtime/memory-state.json` under `memoryPurifier.reporting`:

- `enabled: false` (seeded from `--cron-announce false`) — silent in chat; telemetry + `last-run.md` still written
- `enabled: true` (seeded from `--cron-announce true`) + `mode: "silent" | "summary" | "full"`

---

## Upgrading

```bash
cd "$SKILL_ROOT"
git pull --ff-only
bash install.sh  # idempotent; preserves existing config unless --force-config is passed
```

## Uninstall

```bash
openclaw cron list --json | jq -r '.[] | select(.name | startswith("memory-purifier")) | .name' | \
    xargs -I{} openclaw cron delete --name {}
rm -rf "$WORKSPACE/runtime/" "$TELEMETRY_ROOT" "$CONFIG_ROOT/memory-purifier" "$SKILL_ROOT"
# Optional — prune the memoryPurifier namespace from shared state:
python3 -c "
import json
p = '$WORKSPACE/runtime/memory-state.json'
d = json.load(open(p)); d.pop('memoryPurifier', None)
json.dump(d, open(p, 'w'), indent=2)
"
```

Rendered markdown views (`LTMEMORY.md`, etc.) are left in place as human-readable snapshots. Remove manually if desired.
