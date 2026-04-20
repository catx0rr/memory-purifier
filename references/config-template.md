# config-template.md — `memory-purifier.json` Shape

**Canonical shape of the skill config file. Lives at `~/.openclaw/memory-purifier/memory-purifier.json` by default. Seeded by `install.sh`; editable by the operator.**

This file is namespace-owned by `memory-purifier` — it carries only purifier-specific keys. Shared runtime state (reporting delivery, etc.) lives in `<workspace>/runtime/memory-state.json` under the `memoryPurifier` namespace; never merge shared-runtime fields into this file.

---

## 1. Full shape

```json
{
  "version": "1.4.0",
  "profile": "business",
  "timezone": "Asia/Manila",
  "cadence": {
    "incremental": ["15 13 * * 1,2,4,5,6"],
    "reconciliation": ["15 13 * * 3,0"]
  },
  "paths": {
    "workspace": null,
    "runtime_dir": null,
    "telemetry_root": null,
    "config_root": null
  },
  "prompts": {
    "backend": "claude-code",
    "model": null,
    "max_tokens": null
  },
  "limits": {
    "max_candidates_per_batch": 40,
    "max_clusters_per_batch": 20,
    "oversized_run_strategy": "bounded_batches"
  },
  "cron": {
    "tz": "Asia/Manila",
    "timeout_seconds": 1200,
    "announce": false,
    "announce_channel": null,
    "announce_to": null
  },
  "personal_surfaces": {
    "enabled": false,
    "sources": {
      "chronicles": "CHRONICLES.md",
      "dreams": "DREAMS.md"
    },
    "targets": {
      "history": "HISTORY.md",
      "wishes": "WISHES.md"
    }
  },
  "lastRun": {
    "incremental": null,
    "reconciliation": null,
    "cursor": null
  }
}
```

## 2. Field reference

| Field | Type | Default | Meaning |
|---|---|---|---|
| `version` | string | `"1.4.0"` | Config schema version |
| `profile` | `"business" \| "personal"` | `"personal"` | Drives input eligibility (personal surfaces, personal-only homes). `install.sh --agent-profile <value>` controls the seeded value; omitting the flag seeds `personal`. |
| `timezone` | IANA name | `"Asia/Manila"` | Used for cron registration and timestamp triples |
| `cadence.incremental[]` | cron-expression strings | profile default | One or more cron expressions for incremental runs |
| `cadence.reconciliation[]` | cron-expression strings | profile default | One or more cron expressions for reconciliation runs |
| `paths.workspace` | abs path or `null` | `null` → `~/.openclaw/workspace` | Explicit workspace override |
| `paths.runtime_dir` | abs path or `null` | `null` → `<workspace>/runtime` | Explicit live-runtime override (flat layout — purifier files live directly at this level with `purifier-` / `purified-` prefixes) |
| `paths.telemetry_root` | abs path or `null` | `null` → `~/.openclaw/telemetry/memory-purifier` | Explicit telemetry override |
| `paths.config_root` | abs path or `null` | `null` → `~/.openclaw/memory-purifier` | Explicit config-dir override |
| `prompts.backend` | `"claude-code" \| "openclaw" \| "anthropic-sdk"` | `"claude-code"` | Which model backend the scoring passes invoke |
| `prompts.model` | string or `null` | `null` → backend default | Model override for both passes |
| `prompts.max_tokens` | int or `null` | `null` → backend default | Output token cap per pass invocation |
| `limits.max_candidates_per_batch` | int | `40` | Pass 1 batching ceiling |
| `limits.max_clusters_per_batch` | int | `20` | Pass 2 batching ceiling |
| `limits.oversized_run_strategy` | `"bounded_batches" \| "split_and_queue"` | `"bounded_batches"` | How to handle runs that exceed batch limits |
| `cron.tz` | IANA name | `"Asia/Manila"` | Timezone used by `openclaw cron add`. Seeded by `install.sh --cron-tz`. Fallback for `sync_cron_delivery.py` when the live cron listing omits tz. |
| `cron.timeout_seconds` | positive int | `1200` | Per-run timeout used by `openclaw cron add`. Seeded by `install.sh --timeout-seconds`. Fallback for `sync_cron_delivery.py`. |
| `cron.announce` | bool | `false` | Install-time seed for `memoryPurifier.reporting.enabled`. Documentary after install — the live toggle lives in `memory-state.json`, which `sync_cron_delivery.py` reads. |
| `cron.announce_channel` | string or `null` | `null` | Delivery channel used when `announce=true` (e.g. `"whatsapp"`, `"telegram"`, `"last"`). Seeded by `install.sh --cron-announce-channel`. Fallback for `sync_cron_delivery.py` when memory-state's `delivery.channel` is empty. `null` means "no announce configured"; the sync helper refuses to register announce mode when the channel is missing. |
| `cron.announce_to` | string or `null` | `null` | Delivery target id (peer/group id). Seeded by `install.sh --cron-announce-to`. Same fallback + refusal semantics as `announce_channel`. |
| `personal_surfaces.enabled` | bool | `false` on business, `true` on personal | Whether `CHRONICLES.md`/`DREAMS.md` are read and `HISTORY.md`/`WISHES.md` are written |
| `personal_surfaces.sources` | map | see default | Source filenames — allow relocation if the operator wants non-standard names |
| `personal_surfaces.targets` | map | see default | Target filenames — same relocation rule |
| `lastRun.incremental` | iso-8601 or `null` | `null` | Last successful incremental run — updated by `write_manifest.py` |
| `lastRun.reconciliation` | iso-8601 or `null` | `null` | Last successful reconciliation run |
| `lastRun.cursor` | opaque string or `null` | `null` | Incremental delta cursor — defined by scope selection |

## 3. Profile defaults (seeded by `install.sh`)

### Business default

```json
{
  "profile": "business",
  "cadence": {
    "incremental": ["15 13 * * 1,2,4,5,6"],
    "reconciliation": ["15 13 * * 3,0"]
  },
  "personal_surfaces": { "enabled": false }
}
```

### Personal default

```json
{
  "profile": "personal",
  "cadence": {
    "incremental": ["15 5 * * 1,2,4,5,6", "15 17 * * *"],
    "reconciliation": ["15 5 * * 3,0"]
  },
  "personal_surfaces": { "enabled": true }
}
```

Incremental expressions exclude Wed+Sun (cron day-of-week `3,0`) so reconciliation owns its slot on those days without collision. The cadence arrays are documentary — the `openclaw cron` registrations in `install.sh` are the scheduler truth.

## 4. Shared runtime state (`memory-state.json`)

The reporting configuration lives in the shared workspace state file at `<workspace>/runtime/memory-state.json` under the `memoryPurifier` namespace — not in `memory-purifier.json`. This allows multiple memory plugins to coexist with their own reporting toggles.

Shape:

```json
{
  "memoryPurifier": {
    "reporting": {
      "enabled": false,
      "mode": "summary",
      "delivery": {
        "channel": "last",
        "to": null
      }
    }
  }
}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `reporting.enabled` | bool | `false` | Whether the cron agent may post anything to chat. `false` = fully silent (telemetry + manifest still record). |
| `reporting.mode` | `"silent" \| "summary" \| "full"` | `"summary"` | When `enabled: true`: `silent` = nothing in chat; `summary` = compact one-liner; `full` = two-to-three line bounded report with token usage line. |
| `reporting.delivery.channel` | string | `"last"` | Which chat route to post to. `"last"` asks the host to reuse the most recent route. |
| `reporting.delivery.to` | string or `null` | `null` | Explicit target override. |

Cron prompts read these fields only — all summary numbers come from the runner's final JSON (see `prompts/incremental-purifier-prompt.md §3`). In Step 2.5 the prompt reads **only** `reporting.enabled` via a narrow deterministic query; the full memory-state file is never loaded into the model context.

### Reporting ↔ cron delivery synchronization

`install.sh --cron-announce <true|false>` seeds `reporting.enabled` and the cron delivery form consistently — `true` registers with `--announce --channel <X> --to <Y>` (per OpenClaw cron docs, announce is an explicit delivery mode, not merely the absence of `--no-deliver`); `false` registers with `--no-deliver`. If the toggle drifts later (operator flips `reporting.enabled` without re-registering), the cron supervisor prompts invoke `scripts/sync_cron_delivery.py` before chat delivery. The helper:

- reads only `memoryPurifier.reporting.{enabled, delivery.channel, delivery.to}` from `memory-state.json`
- lists current `memory-purifier-*` cron jobs via `openclaw cron list --json`
- for any job whose delivery form disagrees with the toggle, `openclaw cron delete` + `openclaw cron add` re-registers with the corrected form, preserving cron expression, tz, session, launcher message, and timeout
- when flipping to announce, uses memory-state's `delivery.channel` / `delivery.to` first; falls back to `cron.announce_channel` / `cron.announce_to` in this config file if memory-state is empty; **refuses to mutate** and records `skipped_missing_announce_target` if neither source provides both
- falls back to `cron.tz` and `cron.timeout_seconds` in this config file when the live listing omits those fields

The sync applies to the **next** fire. The current fire's delivery is whatever the scheduler already decided when it started the session.

## 5. Rules

- **Profile is the source of truth** for surface eligibility. Scripts must not emit `HISTORY.md` / `WISHES.md` on `profile: "business"` unless the operator sets `personal_surfaces.enabled: true` as an explicit override (rare).
- **Profile fallback to reflections:** if this config is missing or lacks a `profile` field, scripts read `profile` from `~/.openclaw/reflections/reflections.json` before defaulting to `"personal"`.
- **Paths are resolution-laddered:** CLI flag → env var → this file → hardcoded default. Never bake the config values into scripts.
- **Cadence arrays allow multi-schedule:** personal profile has two incremental times (05:15, 17:15) — represented as two cron strings in the array.
- **`lastRun` is machine-written:** operators should not edit these fields. `write_manifest.py` stamps them after each successful run.
- **Reporting is opt-in:** default is `enabled: false`. Telemetry and `last-run.md` are written regardless — only chat delivery is gated.
