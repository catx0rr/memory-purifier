# cadence-profiles.md — Cron Cadence by Profile

**Locked cron schedules for both profiles. Default timezone is `Asia/Manila`; the operator can override per install via `install.sh --cron-tz <IANA>`. Cron passes a short launcher message (`Run memory purifier. Read <prompt path> and follow every step strictly.`) that points the agent at a step-by-step execution prompt from `prompts/`; the prompt runs `scripts/run_purifier.py` as the orchestrator.**

---

## 1. Upstream cadence context

`memory-purifier` runs **after** its consolidators have finished their window. Upstream layers on this machine fire host-cron at:

| Layer | Profile | Host-cron fire times (Asia/Manila) |
|---|---|---|
| `reflections-hybrid` | business | `05:30, 12:30, 18:30, 22:30` |
| `reflections-hybrid` | personal | `04:30, 10:30, 16:30, 22:30` |
| native dreaming (observed by `memory-core`) | both | typically daily at `03:00` |

Mode (`rem` / `deep` / `core`) inside each reflections fire is gated by elapsed time (6 / 12 / 24 hours) against `lastRun.{mode}`, not by clock time. Purifier cadence is chosen to slot into quiet windows after an upstream fire and before the next.

---

## 2. Business profile

Config (`memory-purifier.json` cadence section is documentary — cron entries are authoritative):

```json
{
  "profile": "business",
  "cadence": {
    "incremental": ["15 13 * * 1,2,4,5,6"],
    "reconciliation": ["15 13 * * 3,0"]
  }
}
```

| Job | Cron expression | Fires | Rationale |
|---|---|---|---|
| `memory-purifier-incremental` | `15 13 * * 1,2,4,5,6` | Mon, Tue, Thu, Fri, Sat at 13:15 | Daily delta — runs 45 min after the 12:30 reflections fire, sits in the quiet early-afternoon window, finishes well before the 18:30 fire. **Wed + Sun are excluded** so reconciliation owns those days. |
| `memory-purifier-reconciliation` | `15 13 * * 3,0` | Wed + Sun at 13:15 | Wider-horizon sweep — re-cluster, re-score, repair supersession chains. Shares the 13:15 slot on its two days. |

---

## 3. Personal profile

```json
{
  "profile": "personal",
  "cadence": {
    "incremental": ["15 5 * * 1,2,4,5,6", "15 17 * * *"],
    "reconciliation": ["15 5 * * 3,0"]
  }
}
```

| Job | Cron expression | Fires | Rationale |
|---|---|---|---|
| `memory-purifier-incremental-morning` | `15 5 * * 1,2,4,5,6` | Mon, Tue, Thu, Fri, Sat at 05:15 | Captures the 04:30 reflections fire (`rem`+`deep`) and the 03:00 native-dreaming window. Wed + Sun excluded — reconciliation owns that slot. |
| `memory-purifier-incremental-evening` | `15 17 * * *` | Every day at 17:15 | 45 min after the 16:30 reflections fire. Never collides with reconciliation. |
| `memory-purifier-reconciliation` | `15 5 * * 3,0` | Wed + Sun at 05:15 | Wider-horizon sweep on the quieter morning slot. |

Personal profile has three cron entries (two incremental slots + one reconciliation) because the evening slot runs daily without conflict.

---

## 4. Reconciliation-over-incremental enforcement

Reconciliation must win on overlap days. This is enforced at the **cron-expression level** — not by runtime lock racing:

- Incremental expressions exclude the days reconciliation fires (`3,0` = Wed + Sun).
- Reconciliation owns Wed + Sun in its slot.
- The two jobs cannot collide because their day-of-week sets are disjoint.

Runtime lock serialization still guards against overlap with the opposite slot (personal evening vs reconciliation morning, for example), but the common-case Wed/Sun collision is prevented at the scheduler.

---

## 5. Registration via `openclaw cron add`

Cron hands the agent a **short launcher message** that tells it which top-level prompt file to read. The prompt itself reads paths, runs `scripts/run_purifier.py`, interprets the status, reconciles cron delivery drift via `scripts/sync_cron_delivery.py`, and reports conditionally.

Per OpenClaw cron docs, announce is an **explicit delivery mode** (`--announce --channel <X> --to <Y>`), not merely the absence of `--no-deliver`. The examples below show the two forms.

```bash
# --- No-deliver (silent): announce=false ---
# Business incremental, default tz, timeout=1200
INCR_PROMPT="<abs-path>/prompts/incremental-purifier-prompt.md"
openclaw cron add \
  --name "memory-purifier-incremental" \
  --cron "15 13 * * 1,2,4,5,6" \
  --tz "Asia/Manila" \
  --session isolated \
  --timeout-seconds 1200 \
  --no-deliver \
  --message "Run memory purifier.

Read \`$INCR_PROMPT\` and follow every step strictly."

# Reconciliation (no-deliver)
RECON_PROMPT="<abs-path>/prompts/reconciliation-purifier-prompt.md"
openclaw cron add \
  --name "memory-purifier-reconciliation" \
  --cron "15 13 * * 3,0" \
  --tz "Asia/Manila" \
  --session isolated \
  --timeout-seconds 1200 \
  --no-deliver \
  --message "Run memory purifier.

Read \`$RECON_PROMPT\` and follow every step strictly."

# --- Announce (chat-enabled): announce=true ---
# Business incremental delivering to WhatsApp peer +6312345
openclaw cron add \
  --name "memory-purifier-incremental" \
  --cron "15 13 * * 1,2,4,5,6" \
  --tz "Asia/Manila" \
  --session isolated \
  --timeout-seconds 1200 \
  --announce --channel whatsapp --to "+6312345" \
  --message "Run memory purifier.

Read \`$INCR_PROMPT\` and follow every step strictly."
```

Required flags:
- `--tz "$CRON_TZ"` — always explicit. Default `Asia/Manila`; `install.sh --cron-tz` overrides.
- `--session isolated` — each run gets a fresh isolated session.
- `--timeout-seconds "$TIMEOUT_SECONDS"` — default `1200`; `install.sh --timeout-seconds` overrides.
- Delivery mode — **pick one**:
  - `--no-deliver` — silent; registered when `install.sh --cron-announce false` (default).
  - `--announce --channel <channel> --to <id>` — chat-enabled; registered when `install.sh --cron-announce true` (both `--cron-announce-channel` and `--cron-announce-to` required).
- `--message` contains a short launcher that **references** the prompt file path with backticks — never the prompt body inline.

Idempotency (implemented by `install.sh`):

```bash
openclaw cron list --json | jq -r '.[].name' | grep -q '^memory-purifier-incremental$' \
  && echo "[cron] memory-purifier-incremental already registered — skipping" \
  || openclaw cron add --name "memory-purifier-incremental" ...
```

---

## 6. Collision avoidance (downstream reconciler)

`memory-reconciler` runs Wed + Sun at 23:00. The purifier's Wed/Sun reconciliation at 13:15 finishes well before that, giving the reconciler a fresh purified substrate to ingest without race conditions.

Do not move the purifier's reconciliation slot past 18:00 on Wed/Sun — this would compress the gap below safety margin for the reconciler's ingestion phase.

---

## 7. Operator override

Operators may customize cadence by editing `memory-purifier.json` `cadence.incremental[]` / `cadence.reconciliation[]`. After edit, re-register cron entries (`openclaw cron delete` then `openclaw cron add` with the new expression). The config arrays are documentary; the cron registration is the scheduler truth.

If you change the day-of-week exclusion, keep the incremental and reconciliation sets disjoint so the two modes never collide.

## 8. Delivery sync over time

`install.sh --cron-announce <true|false>` seeds both the cron registration (explicit `--announce --channel <X> --to <Y>` or `--no-deliver`) and `memoryPurifier.reporting.{enabled, delivery.channel, delivery.to}` in `<workspace>/runtime/memory-state.json`. Those can drift later — the operator flips `reporting.enabled` in memory-state.json without editing cron.

`scripts/sync_cron_delivery.py` is the single deterministic actor that mutates cron delivery after install. The cron supervisor prompts call it **before** chat delivery each fire. It reads `memoryPurifier.reporting.{enabled, delivery.channel, delivery.to}` from memory-state.json and `cron.{announce_channel, announce_to}` from `memory-purifier.json` (as fallback), and:

- If a memory-purifier cron job's delivery form already matches the toggle → no-op.
- If it mismatches → `openclaw cron delete` + `openclaw cron add` preserving cron expression, tz, session, launcher message, and timeout, flipping to the correct delivery form (`--announce --channel --to` ↔ `--no-deliver`).
- If the flip is toward announce but `delivery.channel` / `delivery.to` is empty in both memory-state and config fallback → refuses to mutate and records `skipped_missing_announce_target` in its result. Operator action required.

The reconciliation applies to the **next** fire; the current fire's delivery is whatever the scheduler already chose when it wrote the prompt into the session.
