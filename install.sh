#!/usr/bin/env bash
set -euo pipefail

# Memory Purifier — Operator Install Script
#
# Installs the package skeleton, copies scripts/prompts/references, seeds
# the runtime control-plane JSONs, and optionally registers cron jobs that
# invoke scripts/run_purifier.py via a short launcher message that points
# to the correct top-level prompt file. This installer does NOT run a
# purifier pass or populate live artifacts — follow INSTALL.md for the
# first live initialization sequence.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/catx0rr/memory-purifier/main/install.sh | bash
#
# CLI flags:
#   --agent-profile business|personal   Profile for seeded config and cron.
#                                       Default when omitted: personal.
#   --local                             Install from the directory containing
#                                       this install.sh. Skips git clone/pull,
#                                       no network. Use for offline installs
#                                       and the test harness.
#   --cron-tz <IANA>                    IANA timezone for cron registration.
#                                       Default: Asia/Manila.
#   --cron-announce true|false          true  -> register with --announce
#                                                 --channel <X> --to <Y>
#                                                 and seed reporting.enabled=true
#                                       false -> register with --no-deliver
#                                                 and seed reporting.enabled=false
#                                       Default: false.
#   --cron-announce-channel <channel>   Announce delivery channel (e.g. whatsapp,
#                                       telegram, last). REQUIRED and non-empty
#                                       when --cron-announce true. OpenClaw owns
#                                       channel validity at cron-add time.
#   --cron-announce-to <id>             Announce target id (peer/group id).
#                                       REQUIRED and non-empty when
#                                       --cron-announce true.
#   --timeout-seconds <int>             Per-run timeout used for cron
#                                       registration (positive integer).
#                                       Default: 1200.
#   --skip-cron                         Do not register cron jobs
#   --force-config                      Overwrite existing memory-purifier.json
#   --non-interactive                   Skip the interactive y/N confirmation
#                                       and proceed with the install plan.
#                                       Also auto-set when stdin is not a TTY
#                                       (e.g. `curl … | bash`).
#   --help                              Print usage and exit
#
# Runtime layout (created by this installer at $WORKSPACE/runtime/):
#   purifier-metadata.json            — seeded version + install timestamp
#   purified-manifest.json            — seeded manifest shell
#   purifier-last-run-summary.json    — seeded summary shell
#   locks/                            — lock directory (purifier-run.lock)
#   memory-state.json                 — shared; memoryPurifier namespace merged in
# First-run artifacts (purified-claims.jsonl, purified-*.json, etc.) are
# created at the same flat level by the first successful run.
#
# Environment-variable overrides (flags take precedence):
#   CONFIG_ROOT       default: $HOME/.openclaw
#   WORKSPACE         default: $HOME/.openclaw/workspace
#   SKILLS_PATH       default: $HOME/.openclaw/workspace/skills
#   TELEMETRY_ROOT    default: $HOME/.openclaw/telemetry/memory-purifier
#   PROFILE           default: personal     (flag --agent-profile preferred)
#   CRON_TZ           default: Asia/Manila  (flag --cron-tz preferred)
#   CRON_ANNOUNCE     default: false        (flag --cron-announce preferred)
#   CRON_ANNOUNCE_CHANNEL default: (empty)  (flag --cron-announce-channel preferred; required when announce=true)
#   CRON_ANNOUNCE_TO  default: (empty)      (flag --cron-announce-to preferred; required when announce=true)
#   TIMEOUT_SECONDS   default: 1200         (flag --timeout-seconds preferred)
#   SKIP_CRON         default: 0
#   FORCE_CONFIG      default: 0
#   NON_INTERACTIVE   default: 0           (flag --non-interactive preferred)

REPO_URL="https://github.com/catx0rr/memory-purifier.git"

CONFIG_ROOT="${CONFIG_ROOT:-$HOME/.openclaw}"
WORKSPACE="${WORKSPACE:-$HOME/.openclaw/workspace}"
SKILLS_PATH="${SKILLS_PATH:-$HOME/.openclaw/workspace/skills}"
SKILL_ROOT="$SKILLS_PATH/memory-purifier"

PROFILE="${PROFILE:-personal}"
CRON_TZ="${CRON_TZ:-Asia/Manila}"
CRON_ANNOUNCE="${CRON_ANNOUNCE:-false}"
CRON_ANNOUNCE_CHANNEL="${CRON_ANNOUNCE_CHANNEL:-}"
CRON_ANNOUNCE_TO="${CRON_ANNOUNCE_TO:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-1200}"
SKIP_CRON="${SKIP_CRON:-0}"
FORCE_CONFIG="${FORCE_CONFIG:-0}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"
LOCAL_INSTALL=0

while [ $# -gt 0 ]; do
    case "$1" in
        --agent-profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --agent-profile=*)
            PROFILE="${1#--agent-profile=}"
            shift
            ;;
        --cron-tz)
            CRON_TZ="${2:-}"
            shift 2
            ;;
        --cron-tz=*)
            CRON_TZ="${1#--cron-tz=}"
            shift
            ;;
        --cron-announce)
            CRON_ANNOUNCE="${2:-}"
            shift 2
            ;;
        --cron-announce=*)
            CRON_ANNOUNCE="${1#--cron-announce=}"
            shift
            ;;
        --cron-announce-channel)
            CRON_ANNOUNCE_CHANNEL="${2:-}"
            shift 2
            ;;
        --cron-announce-channel=*)
            CRON_ANNOUNCE_CHANNEL="${1#--cron-announce-channel=}"
            shift
            ;;
        --cron-announce-to)
            CRON_ANNOUNCE_TO="${2:-}"
            shift 2
            ;;
        --cron-announce-to=*)
            CRON_ANNOUNCE_TO="${1#--cron-announce-to=}"
            shift
            ;;
        --timeout-seconds)
            TIMEOUT_SECONDS="${2:-}"
            shift 2
            ;;
        --timeout-seconds=*)
            TIMEOUT_SECONDS="${1#--timeout-seconds=}"
            shift
            ;;
        --local)
            LOCAL_INSTALL=1
            shift
            ;;
        --skip-cron)
            SKIP_CRON=1
            shift
            ;;
        --force-config)
            FORCE_CONFIG=1
            shift
            ;;
        --non-interactive)
            NON_INTERACTIVE=1
            shift
            ;;
        --help|-h)
            sed -n '4,71p' "$0"
            exit 0
            ;;
        *)
            echo "Error: unknown flag: $1"
            echo "Run: bash install.sh --help"
            exit 1
            ;;
    esac
done

TELEMETRY_ROOT="${TELEMETRY_ROOT:-$HOME/.openclaw/telemetry/memory-purifier}"
CONFIG_DIR="$CONFIG_ROOT/memory-purifier"
CONFIG_FILE="$CONFIG_DIR/memory-purifier.json"
# Flat runtime layout: purifier files live directly under <workspace>/runtime/
# with `purifier-` / `purified-` prefixes. No nested memory-purifier/ subdir.
RUNTIME_DIR="$WORKSPACE/runtime"
LOCKS_DIR="$RUNTIME_DIR/locks"
MEM_STATE="$RUNTIME_DIR/memory-state.json"

# If --local is set, resolve the package source directory as the directory
# containing this install.sh, so the installer works offline from a checkout.
HERE="$(cd "$(dirname "$0")" && pwd)"

# ── Input screening (profile, tz, announce, timeout) ──────────────────

if [ "$PROFILE" != "business" ] && [ "$PROFILE" != "personal" ]; then
    echo "Error: invalid --agent-profile value: '$PROFILE'"
    echo ""
    echo "Valid values:"
    echo "  business  — incremental 13:15 Mon/Tue/Thu/Fri/Sat; reconciliation 13:15 Wed/Sun"
    echo "  personal  — incremental 05:15 Mon/Tue/Thu/Fri/Sat + 17:15 daily; reconciliation 05:15 Wed/Sun (default)"
    echo ""
    echo "Usage:"
    echo "  bash install.sh                          # uses default profile: personal"
    echo "  bash install.sh --agent-profile business"
    echo "  bash install.sh --agent-profile personal"
    echo ""
    echo "Run 'bash install.sh --help' for the full flag list."
    exit 1
fi

# Minimum IANA shape screening — not a full database check, just enough to
# reject typos and obvious garbage before they reach `openclaw cron add`.
if ! printf '%s' "$CRON_TZ" | grep -Eq '^[A-Za-z_+-]+/[A-Za-z0-9_+.-]+(/[A-Za-z0-9_+.-]+)*$'; then
    echo "Error: invalid --cron-tz value: '$CRON_TZ'"
    echo "Expected an IANA-shaped zone like 'Asia/Manila', 'America/Los_Angeles', or 'Etc/UTC'."
    exit 1
fi

case "$CRON_ANNOUNCE" in
    true|false) ;;
    *)
        echo "Error: invalid --cron-announce value: '$CRON_ANNOUNCE'"
        echo "Expected: true  (register cron with --announce --channel <X> --to <Y>, seed reporting.enabled=true)"
        echo "          false (register cron with --no-deliver, seed reporting.enabled=false)"
        exit 1
        ;;
esac

# When announce is on, BOTH channel and target are required non-empty.
# OpenClaw owns validity of the channel value at cron-add time — the
# installer passes the value through.
if [ "$CRON_ANNOUNCE" = "true" ]; then
    if [ -z "$CRON_ANNOUNCE_CHANNEL" ]; then
        echo "Error: --cron-announce-channel is required when --cron-announce true"
        echo "Pass a channel name (e.g. 'whatsapp', 'telegram', 'last') via --cron-announce-channel <channel>."
        exit 1
    fi
    if [ -z "$CRON_ANNOUNCE_TO" ]; then
        echo "Error: --cron-announce-to is required when --cron-announce true"
        echo "Pass the peer/group id that reports should be delivered to via --cron-announce-to <id>."
        exit 1
    fi
else
    # announce=false: ignore any values passed for channel/to and blank them
    # so the seeded config and memory-state carry null cleanly.
    CRON_ANNOUNCE_CHANNEL=""
    CRON_ANNOUNCE_TO=""
fi

if ! printf '%s' "$TIMEOUT_SECONDS" | grep -Eq '^[1-9][0-9]*$'; then
    echo "Error: invalid --timeout-seconds value: '$TIMEOUT_SECONDS'"
    echo "Expected a positive integer (default 1200)."
    exit 1
fi

echo "Memory Purifier installer"
echo "========================="
echo ""
echo "Configuration"
echo "-------------"
echo "  PROFILE:          $PROFILE"
echo "  CRON_TZ:          $CRON_TZ"
echo "  CRON_ANNOUNCE:    $CRON_ANNOUNCE    (reporting.enabled will be seeded to this)"
if [ "$CRON_ANNOUNCE" = "true" ]; then
    echo "  ANNOUNCE_CHANNEL: $CRON_ANNOUNCE_CHANNEL"
    echo "  ANNOUNCE_TO:      $CRON_ANNOUNCE_TO"
fi
echo "  TIMEOUT_SECONDS:  ${TIMEOUT_SECONDS}s"
echo "  SKIP_CRON:        $SKIP_CRON"
echo "  FORCE_CONFIG:     $FORCE_CONFIG"
if [ "$LOCAL_INSTALL" = "1" ]; then
    echo "  SOURCE:           --local (offline sync from $HERE)"
else
    echo "  SOURCE:           git clone/update from $REPO_URL"
fi
echo ""
echo "Directories to create (mkdir -p; existing dirs left alone)"
echo "----------------------------------------------------------"
echo "  Package root:     $SKILL_ROOT"
echo "  Config dir:       $CONFIG_DIR"
echo "  Runtime dir:      $RUNTIME_DIR"
echo "  Locks dir:        $LOCKS_DIR"
echo "  Telemetry dir:    $TELEMETRY_ROOT"
echo "  Workspace subs:   $WORKSPACE/episodes, $WORKSPACE/memory"
echo ""
echo "Seed files (idempotent — existing files preserved unless --force-config)"
echo "------------------------------------------------------------------------"
echo "  $CONFIG_FILE"
echo "  $RUNTIME_DIR/purifier-metadata.json"
echo "  $RUNTIME_DIR/purified-manifest.json"
echo "  $RUNTIME_DIR/purifier-last-run-summary.json"
echo "  $MEM_STATE  (memoryPurifier namespace merged in)"
echo ""
echo "Cron registration"
echo "-----------------"
if [ "$SKIP_CRON" = "1" ]; then
    echo "  SKIPPED (--skip-cron). Register manually per INSTALL.md §7."
elif [ "$PROFILE" = "personal" ]; then
    echo "  memory-purifier-incremental-morning   15 5 * * 1,2,4,5,6   ($CRON_TZ)"
    echo "  memory-purifier-incremental-evening   15 17 * * *          ($CRON_TZ)"
    echo "  memory-purifier-reconciliation        15 5 * * 3,0         ($CRON_TZ)"
else
    echo "  memory-purifier-incremental           15 13 * * 1,2,4,5,6  ($CRON_TZ)"
    echo "  memory-purifier-reconciliation        15 13 * * 3,0        ($CRON_TZ)"
fi
if [ "$CRON_ANNOUNCE" = "true" ]; then
    echo "  Delivery:                             --announce --channel $CRON_ANNOUNCE_CHANNEL --to $CRON_ANNOUNCE_TO"
else
    echo "  Delivery:                             --no-deliver (silent; telemetry still recorded)"
fi
echo ""
echo "This installer does NOT run a purifier pass — follow INSTALL.md for first-time initialization."
echo ""

# Interactive confirmation. Skipped when --non-interactive is passed, NON_INTERACTIVE=1,
# or stdin is not a TTY (so `curl ... | bash` is not blocked by an unreadable
# prompt — the operator already opted in by piping).
if [ "$NON_INTERACTIVE" != "1" ] && [ -t 0 ]; then
    printf "Proceed with install? [y/N]: "
    read -r reply
    case "$reply" in
        [yY]|[yY][eE][sS]) ;;
        *)
            echo "Aborted — no changes made."
            exit 0
            ;;
    esac
    echo ""
fi

# ── A. Install/update the repo ────────────────────────────────────────

mkdir -p "$SKILLS_PATH"

if [ "$LOCAL_INSTALL" = "1" ]; then
    # Offline / local-checkout install: copy the package from the directory
    # containing this install.sh into $SKILL_ROOT. No git, no network.
    if [ ! -f "$HERE/SKILL.md" ]; then
        echo "Error: --local requires install.sh to run from a complete package directory."
        echo "Expected $HERE/SKILL.md to exist."
        exit 1
    fi
    if [ "$HERE" = "$SKILL_ROOT" ]; then
        echo "[repo] --local: SKILL_ROOT == source directory; using in-place at $SKILL_ROOT"
    else
        echo "[repo] --local: syncing package from $HERE to $SKILL_ROOT"
        mkdir -p "$SKILL_ROOT"
        # Copy package contents (excluding .git and runtime scaffolds) with rsync if
        # available, otherwise fall back to cp -R.
        if command -v rsync >/dev/null 2>&1; then
            rsync -a --delete \
                --exclude='.git' \
                --exclude='runtime/locks/*.lock' \
                --exclude='__pycache__' \
                "$HERE/" "$SKILL_ROOT/"
        else
            cp -R "$HERE/." "$SKILL_ROOT/"
        fi
        echo "[repo] --local: sync complete."
    fi
elif [ -d "$SKILL_ROOT/.git" ]; then
    echo "[repo] Existing git installation found. Updating..."
    cd "$SKILL_ROOT"
    git pull --ff-only || {
        echo "Warning: fast-forward pull failed. Manual resolution may be needed."
        echo "Location: $SKILL_ROOT"
        exit 1
    }
    echo "[repo] Updated successfully."
elif [ -d "$SKILL_ROOT" ] && [ -f "$SKILL_ROOT/SKILL.md" ]; then
    echo "[repo] Existing non-git installation detected at $SKILL_ROOT. Using as-is."
elif [ -d "$SKILL_ROOT" ]; then
    echo "Error: Directory exists but contains no SKILL.md or .git: $SKILL_ROOT"
    echo "Remove it manually or choose a different SKILLS_PATH, then re-run."
    echo "(Hint: pass --local to install from this directory without using git.)"
    exit 1
else
    echo "[repo] Cloning memory-purifier..."
    git clone "$REPO_URL" "$SKILL_ROOT"
    echo "[repo] Cloned successfully."
fi

if [ ! -f "$SKILL_ROOT/SKILL.md" ]; then
    echo "Error: SKILL.md not found at $SKILL_ROOT/SKILL.md"
    echo "Installation may be incomplete."
    exit 1
fi

# ── B. Initialize workspace topology ──────────────────────────────────

echo ""
echo "[init] Initializing workspace topology..."

mkdir -p "$CONFIG_DIR"
mkdir -p "$WORKSPACE/episodes"
mkdir -p "$WORKSPACE/memory"
mkdir -p "$RUNTIME_DIR"
mkdir -p "$LOCKS_DIR"
mkdir -p "$TELEMETRY_ROOT"

# ── C. Seed skill config (profile-aware) ──────────────────────────────

seed_config() {
    local profile="$1"
    local incremental_cron reconciliation_cron personal_enabled
    local announce_channel_json announce_to_json

    # Cadence arrays are documentary — cron registration in step F is authoritative.
    # Expressions exclude Wed+Sun from incremental so reconciliation owns those slots.
    if [ "$profile" = "personal" ]; then
        incremental_cron='"15 5 * * 1,2,4,5,6", "15 17 * * *"'
        reconciliation_cron='"15 5 * * 3,0"'
        personal_enabled="true"
    else
        incremental_cron='"15 13 * * 1,2,4,5,6"'
        reconciliation_cron='"15 13 * * 3,0"'
        personal_enabled="false"
    fi

    # Announce channel/to render as JSON strings when set, null when empty.
    if [ -n "$CRON_ANNOUNCE_CHANNEL" ]; then
        announce_channel_json="\"$CRON_ANNOUNCE_CHANNEL\""
    else
        announce_channel_json="null"
    fi
    if [ -n "$CRON_ANNOUNCE_TO" ]; then
        announce_to_json="\"$CRON_ANNOUNCE_TO\""
    else
        announce_to_json="null"
    fi

    cat > "$CONFIG_FILE" <<CFGEOF
{
  "version": "1.6.0",
  "profile": "$profile",
  "timezone": "$CRON_TZ",
  "cadence": {
    "incremental": [$incremental_cron],
    "reconciliation": [$reconciliation_cron]
  },
  "paths": {
    "workspace": null,
    "runtime_dir": null,
    "telemetry_root": null,
    "config_root": null
  },
  "prompts": {
    "backend": "openclaw",
    "model": null,
    "max_tokens": null
  },
  "limits": {
    "max_candidates_per_batch": 40,
    "max_clusters_per_batch": 20,
    "oversized_run_strategy": "bounded_batches"
  },
  "cron": {
    "tz": "$CRON_TZ",
    "timeout_seconds": $TIMEOUT_SECONDS,
    "announce": $CRON_ANNOUNCE,
    "announce_channel": $announce_channel_json,
    "announce_to": $announce_to_json
  },
  "personal_surfaces": {
    "enabled": $personal_enabled,
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
CFGEOF
}

if [ ! -f "$CONFIG_FILE" ]; then
    echo "[init] Creating $CONFIG_FILE (profile: $PROFILE, tz: $CRON_TZ, announce: $CRON_ANNOUNCE, timeout: $TIMEOUT_SECONDS)"
    seed_config "$PROFILE"
elif [ "$FORCE_CONFIG" = "1" ]; then
    echo "[init] FORCE_CONFIG=1 — overwriting $CONFIG_FILE"
    seed_config "$PROFILE"
else
    echo "[init] $CONFIG_FILE already exists — skipping (set FORCE_CONFIG=1 to overwrite)"
fi

# ── D. Seed runtime metadata (idempotent) ─────────────────────────────

seed_json_if_absent() {
    local path="$1"
    local content="$2"
    if [ ! -f "$path" ]; then
        echo "[init] Creating $path"
        printf '%s\n' "$content" > "$path"
    else
        echo "[init] $path already exists — skipping"
    fi
}

INSTALL_TS_LOCAL="$(date -Iseconds)"
INSTALL_TS_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

seed_json_if_absent "$RUNTIME_DIR/purifier-metadata.json" "$(cat <<METAEOF
{
  "version": "1.6.0",
  "installed_at": "$INSTALL_TS_LOCAL",
  "installed_at_utc": "$INSTALL_TS_UTC",
  "timezone": "$CRON_TZ",
  "profile": "$PROFILE"
}
METAEOF
)"

# v1.5.0 C1 (Contract 3): seed the four-version block so fresh installs
# start at current logic/schema state — no upgrade detection needed on
# first cron fire. The orchestrator overwrites these on every successful
# finalize; the seed only matters until the first ok run lands.
seed_json_if_absent "$RUNTIME_DIR/purified-manifest.json" "$(cat <<MANIEOF
{
  "version": "1.6.0",
  "packageVersion": "1.6.0",
  "logicVersion": "1.6.0",
  "manifestSchemaVersion": "1",
  "artifactSchemaVersion": "1",
  "lastSuccessfulLogicVersion": "1.6.0",
  "runId": null,
  "mode": null,
  "startedAt": null,
  "finishedAt": null,
  "profileScope": "$PROFILE",
  "sourceInventory": [],
  "processedSegments": [],
  "promotionStats": {
    "reject": 0,
    "defer": 0,
    "compress": 0,
    "merge": 0,
    "promote": 0
  },
  "claimStats": {
    "resolved": 0,
    "contested": 0,
    "unresolved": 0,
    "superseded": 0,
    "stale": 0
  },
  "warnings": [],
  "partialFailures": [],
  "lastSuccessfulCursor": null,
  "downstreamWikiIngestSuggested": false
}
MANIEOF
)"

seed_json_if_absent "$RUNTIME_DIR/purifier-last-run-summary.json" "$(cat <<SUMEOF
{
  "ok": false,
  "status": null,
  "mode": null,
  "profile": "$PROFILE",
  "runId": null,
  "startedAt": null,
  "finishedAt": null,
  "durationSeconds": null,
  "claimsNew": 0,
  "claimsTotal": 0,
  "contradictionCount": 0,
  "supersessionCount": 0,
  "warnings": [],
  "partialFailures": [],
  "warningCount": 0,
  "partialFailureCount": 0,
  "viewsRendered": [],
  "downstreamWikiIngestSuggested": false,
  "manifestPath": null
}
SUMEOF
)"

# ── E. Merge shared runtime state (namespace-preserving) ──────────────
# reporting.enabled is seeded from --cron-announce so cron delivery mode
# and the chat-reporting toggle are in sync at install time. Future drift
# is reconciled by scripts/sync_cron_delivery.py.

# Render delivery.channel / delivery.to for the memory-state.json seed.
# announce=true → pass through install-time channel + to.
# announce=false → legacy default ("last", null) — kept so an operator who flips
# reporting.enabled later doesn't start from an empty delivery.
if [ "$CRON_ANNOUNCE" = "true" ]; then
    MEM_CHANNEL_JSON="\"$CRON_ANNOUNCE_CHANNEL\""
    MEM_TO_JSON="\"$CRON_ANNOUNCE_TO\""
else
    MEM_CHANNEL_JSON='"last"'
    MEM_TO_JSON='null'
fi

if [ ! -f "$MEM_STATE" ]; then
    echo "[init] Creating $MEM_STATE (new shared state with memoryPurifier namespace)"
    cat > "$MEM_STATE" <<MEMEOF
{
  "memoryPurifier": {
    "reporting": {
      "enabled": $CRON_ANNOUNCE,
      "mode": "summary",
      "delivery": {
        "channel": $MEM_CHANNEL_JSON,
        "to": $MEM_TO_JSON
      }
    }
  }
}
MEMEOF
elif ! python3 -c "import json,sys; d=json.load(open('$MEM_STATE')); sys.exit(0 if 'memoryPurifier' in d else 1)" 2>/dev/null; then
    echo "[init] Merging memoryPurifier namespace into existing $MEM_STATE (reporting.enabled=$CRON_ANNOUNCE)"
    MEM_STATE_PATH="$MEM_STATE" \
        CRON_ANNOUNCE_VAL="$CRON_ANNOUNCE" \
        ANN_CHANNEL="$CRON_ANNOUNCE_CHANNEL" \
        ANN_TO="$CRON_ANNOUNCE_TO" \
        python3 - <<'PYEOF'
import json, os
p = os.environ["MEM_STATE_PATH"]
announce = os.environ["CRON_ANNOUNCE_VAL"].lower() == "true"
channel = os.environ.get("ANN_CHANNEL") or None
to = os.environ.get("ANN_TO") or None
if announce:
    delivery = {"channel": channel or "last", "to": to}
else:
    delivery = {"channel": "last", "to": None}
with open(p) as f:
    d = json.load(f)
d["memoryPurifier"] = {
    "reporting": {
        "enabled": announce,
        "mode": "summary",
        "delivery": delivery,
    }
}
with open(p, "w") as f:
    json.dump(d, f, indent=2)
    f.write("\n")
PYEOF
else
    # Namespace already present — reconcile reporting.enabled + delivery to
    # the install choice only when --force-config is passed. Otherwise leave
    # the existing toggle + delivery untouched so prior operator intent sticks.
    if [ "$FORCE_CONFIG" = "1" ]; then
        echo "[init] FORCE_CONFIG=1 — syncing reporting.enabled=$CRON_ANNOUNCE + delivery in $MEM_STATE"
        MEM_STATE_PATH="$MEM_STATE" \
            CRON_ANNOUNCE_VAL="$CRON_ANNOUNCE" \
            ANN_CHANNEL="$CRON_ANNOUNCE_CHANNEL" \
            ANN_TO="$CRON_ANNOUNCE_TO" \
            python3 - <<'PYEOF'
import json, os
p = os.environ["MEM_STATE_PATH"]
announce = os.environ["CRON_ANNOUNCE_VAL"].lower() == "true"
channel = os.environ.get("ANN_CHANNEL") or None
to = os.environ.get("ANN_TO") or None
with open(p) as f:
    d = json.load(f)
mp = d.setdefault("memoryPurifier", {})
rp = mp.setdefault("reporting", {"mode": "summary", "delivery": {"channel": "last", "to": None}})
rp["enabled"] = announce
if announce:
    rp["delivery"] = {"channel": channel or "last", "to": to}
else:
    rp["delivery"] = {"channel": "last", "to": None}
with open(p, "w") as f:
    json.dump(d, f, indent=2)
    f.write("\n")
PYEOF
    else
        echo "[init] memoryPurifier namespace already present in $MEM_STATE — preserving existing reporting + delivery (set --force-config to reseed)"
    fi
fi

# ── F. Register cron jobs ─────────────────────────────────────────────
# Cron registration passes a short LAUNCHER message (not the prompt body)
# that points the cron agent at the correct top-level prompt file. The prompt
# file owns all operational instructions; cron metadata stays small.

if [ "$SKIP_CRON" = "1" ]; then
    echo "[cron] SKIP_CRON=1 — skipping cron registration."
else
    if ! command -v openclaw >/dev/null 2>&1; then
        echo "[cron] Warning: 'openclaw' CLI not found on PATH. Skipping cron registration."
        echo "[cron] Register manually after installing OpenClaw. See INSTALL.md."
    else
        INCR_PROMPT="$SKILL_ROOT/prompts/incremental-purifier-prompt.md"
        RECON_PROMPT="$SKILL_ROOT/prompts/reconciliation-purifier-prompt.md"
        INCR_LAUNCHER="Run memory purifier.

Read \`$INCR_PROMPT\` and follow every step strictly."
        RECON_LAUNCHER="Run memory purifier.

Read \`$RECON_PROMPT\` and follow every step strictly."
        EXISTING_JOBS="$(openclaw cron list --json 2>/dev/null || echo '[]')"

        # Build the shared cron-add flag set once. Per OpenClaw cron docs,
        # announce is an explicit delivery mode requiring --announce --channel
        # <X> --to <Y>, not merely the absence of --no-deliver.
        CRON_COMMON_FLAGS=(--tz "$CRON_TZ" --session isolated --timeout-seconds "$TIMEOUT_SECONDS")
        if [ "$CRON_ANNOUNCE" = "true" ]; then
            CRON_COMMON_FLAGS+=(--announce --channel "$CRON_ANNOUNCE_CHANNEL" --to "$CRON_ANNOUNCE_TO")
        else
            CRON_COMMON_FLAGS+=(--no-deliver)
        fi

        register_cron() {
            local name="$1"
            local cron_expr="$2"
            local message="$3"
            if echo "$EXISTING_JOBS" | EXISTING_JOBS_NAME="$name" python3 -c "
import sys, json, os
jobs = json.load(sys.stdin)
target = os.environ['EXISTING_JOBS_NAME']
sys.exit(0 if any(j.get('name') == target for j in jobs) else 1)
" 2>/dev/null; then
                echo "[cron] '$name' already registered — skipping (delivery sync runs at prompt time)"
            else
                if [ "$CRON_ANNOUNCE" = "true" ]; then
                    echo "[cron] Registering '$name' ($cron_expr, tz=$CRON_TZ, announce=true --channel $CRON_ANNOUNCE_CHANNEL --to $CRON_ANNOUNCE_TO, timeout=${TIMEOUT_SECONDS}s)"
                else
                    echo "[cron] Registering '$name' ($cron_expr, tz=$CRON_TZ, announce=false --no-deliver, timeout=${TIMEOUT_SECONDS}s)"
                fi
                # Capture stderr from openclaw cron add so an invalid channel
                # or target surfaces cleanly instead of being silently swallowed.
                if ! openclaw cron add \
                        --name "$name" \
                        --cron "$cron_expr" \
                        "${CRON_COMMON_FLAGS[@]}" \
                        --message "$message"; then
                    echo "[cron] ERROR: openclaw cron add failed for '$name'. See the output above for OpenClaw's rejection reason (invalid channel, invalid target, etc.)."
                    exit 1
                fi
            fi
        }

        # Cron-expression split enforces reconciliation-over-incremental on overlap days:
        # reconciliation owns Wed (3) + Sun (0) in its slot; incremental in that slot
        # excludes those days so the two never collide.
        if [ "$PROFILE" = "personal" ]; then
            # Morning slot 05:15 overlaps with reconciliation on Wed/Sun → exclude those days
            register_cron "memory-purifier-incremental-morning" "15 5 * * 1,2,4,5,6" "$INCR_LAUNCHER"
            # Evening slot 17:15 never collides with reconciliation → run every day
            register_cron "memory-purifier-incremental-evening" "15 17 * * *" "$INCR_LAUNCHER"
            register_cron "memory-purifier-reconciliation" "15 5 * * 3,0" "$RECON_LAUNCHER"
        else
            # Business: single 13:15 slot → exclude Wed/Sun from incremental
            register_cron "memory-purifier-incremental" "15 13 * * 1,2,4,5,6" "$INCR_LAUNCHER"
            register_cron "memory-purifier-reconciliation" "15 13 * * 3,0" "$RECON_LAUNCHER"
        fi
    fi
fi

# ── G. Final verification ─────────────────────────────────────────────

echo ""
echo "[verify] Final checks..."

fail=0
check_file() {
    if [ -f "$1" ]; then
        echo "  ok   $1"
    else
        echo "  FAIL $1 (missing)"
        fail=1
    fi
}
check_dir() {
    if [ -d "$1" ]; then
        echo "  ok   $1/"
    else
        echo "  FAIL $1/ (missing)"
        fail=1
    fi
}

check_file "$CONFIG_FILE"
check_file "$RUNTIME_DIR/purifier-metadata.json"
check_file "$RUNTIME_DIR/purified-manifest.json"
check_file "$RUNTIME_DIR/purifier-last-run-summary.json"
check_file "$MEM_STATE"
check_dir  "$LOCKS_DIR"
check_dir  "$TELEMETRY_ROOT"

if ! python3 -c "import json; json.load(open('$CONFIG_FILE'))" 2>/dev/null; then
    echo "  FAIL $CONFIG_FILE is not valid JSON"
    fail=1
fi
if ! python3 -c "import json; json.load(open('$RUNTIME_DIR/purified-manifest.json'))" 2>/dev/null; then
    echo "  FAIL $RUNTIME_DIR/purified-manifest.json is not valid JSON"
    fail=1
fi

echo ""
if [ "$fail" = "0" ]; then
    echo "Install complete. Live artifacts are NOT yet created — the first live run in INSTALL.md populates them."
    echo ""
    echo "Where things landed:"
    echo "  Package:         $SKILL_ROOT"
    echo "  Config file:     $CONFIG_FILE"
    echo "  Runtime seeds:   $RUNTIME_DIR"
    echo "                     purifier-metadata.json, purified-manifest.json, purifier-last-run-summary.json, locks/"
    echo "  Shared state:    $MEM_STATE   (memoryPurifier namespace)"
    echo "  Local report:    $TELEMETRY_ROOT/last-run.md   (overwritten each run)"
    echo "  Memory-log dir:  $(dirname "$TELEMETRY_ROOT")   (shared memory-log-YYYY-MM-DD.jsonl is appended here each run)"
    echo ""
    echo "Next: follow INSTALL.md for guided first-time initialization."
    echo "  - Quick dry-run:   python3 $SKILL_ROOT/scripts/run_purifier.py --mode incremental --dry-run"
    echo "  - First live run:  python3 $SKILL_ROOT/scripts/run_purifier.py --mode incremental"
    echo "  - Confirm cron:    openclaw cron list --json | jq '.[] | select(.name | startswith(\"memory-purifier\"))'"
    echo ""
    exit 0
else
    echo "Install finished with warnings — see FAIL lines above."
    exit 1
fi
