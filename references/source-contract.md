# source-contract.md — Input File Discovery & Exclusion Contract

**Authoritative list of what `discover_sources.py` may read and what it must never read. This is the locked input boundary for the purifier.**

---

## 1. Allowed inputs (consolidated lower-substrate only)

All paths are relative to `<workspace>/` (resolution ladder: CLI `--workspace` → `WORKSPACE` env → `paths.workspace` from config → `~/.openclaw/workspace`).

### 1.1 Shared inputs (both profiles)

| File pattern | Owner | Target purified output |
|---|---|---|
| `MEMORY.md` | OpenClaw native dreaming | `LTMEMORY.md` |
| `RTMEMORY.md` | `reflections-hybrid` | `LTMEMORY.md` |
| `PROCEDURES.md` | `reflections-hybrid` | `PLAYBOOKS.md` |
| `episodes/*.md` | `reflections-hybrid` | `EPISODES.md` (digest) |

### 1.2 Personal-only inputs

Only read when `profile: "personal"` and `personal_surfaces.enabled: true`.

| File pattern | Owner | Target purified output |
|---|---|---|
| `CHRONICLES.md` | operator-authored | `HISTORY.md` + `WISHES.md` |
| `DREAMS.md` | operator-authored | `WISHES.md` |

### 1.3 Absence handling

- **Shared input missing:** continue; record absence in manifest `warnings[]`. A run with zero inputs terminates as `skipped` (not `error`).
- **Personal input missing on personal run:** continue; the run behaves as if the missing surface had no content. Non-blocking.
- **Personal input present on business run:** ignored silently — does not upgrade the run to personal.

---

## 2. Explicit non-inputs (MUST NOT READ)

The purifier must never ingest the following, even if paths are passed via CLI:

### 2.1 Raw daily logs (consolidator territory)

- `memory/*.md`
- `memory/**/*.md`
- `memory/YYYY-MM-DD.md` (daily shard pattern)
- `memory/.reflections-log.md`
- `memory/.reflections-archive.md`
- `memory/.dreams/`

Reason: these are pre-consolidation. Reflections-hybrid and native dreaming own the lift from daily logs into `RTMEMORY.md` / `MEMORY.md`. The purifier is post-consolidation.

### 2.2 Authority documents

- `CONSTITUTION.md`
- `KNOWLEDGE.md`
- `AGENTS.md`
- `SOUL.md`
- `HARNESS.md`
- Any operator-curated authority doc at `<workspace>` root not listed in §1.

Reason: authority docs carry explicit operator rules that outrank memory. Canonicalization must not touch them.

### 2.3 Already-purified outputs

- `LTMEMORY.md`
- `PLAYBOOKS.md`
- `EPISODES.md`
- `HISTORY.md`
- `WISHES.md`

Reason: these are the purifier's own outputs. Ingesting them as if they were lower substrate would create a self-feedback loop.

**Exception:** reconciliation-mode may *inspect* prior purified artifacts (via `runtime/purified-claims.jsonl`, not the markdown views) for repair, supersession detection, and contradiction re-scoring. This is done through `prior_claims_context` in the Pass 2 input — not through re-ingesting the markdown.

### 2.4 Reflections ops / runtime surfaces

- `TRENDS.md` — operational surface, not consolidated fact
- `runtime/reflections-metadata.json` — index, not content
- `runtime/reflections-deferred.jsonl` — noise ledger
- `runtime/memory-state.json` — shared runtime state, not memory content

Reason: operational surfaces carry metadata, counters, and suppression ledgers, not memory units.

### 2.5 Other packages' runtime state

- `runtime/memory-reconciler-metadata.json` (and anything else under other-package namespaces)
- `runtime/pending-actions/` (shared harness surfaces)

Reason: package boundaries.

---

## 3. Discovery algorithm (what `discover_sources.py` does)

```
1. Resolve <workspace> via resolution ladder.
2. Resolve profile via resolution ladder (CLI → config → reflections.json → default "personal").
3. Build the allow-list from §1 based on profile.
4. For each allowed entry:
   a. Check existence.
   b. If glob (episodes/*.md): enumerate matches, sort alphabetically.
   c. Capture file size, mtime, and content hash for cursor comparison.
5. Build the deny-list from §2 (explicit patterns) for defensive logging.
6. If any file under a deny pattern is explicitly passed via CLI, ABORT with a structured error JSON.
7. Return an inventory JSON object with:
   - workspace (resolved path)
   - profile (resolved)
   - found[] (allowed files actually present, with size/mtime/hash)
   - missing[] (allowed files not present — logged as warnings, not errors)
   - denied_attempts[] (any CLI-passed path that hit the deny list — caused abort)
```

Output JSON shape (one object to stdout):

```json
{
  "workspace": "<abs path>",
  "profile": "business | personal",
  "found": [
    {
      "path": "<relative to workspace>",
      "bytes": 12345,
      "mtime_utc": "<iso>",
      "content_hash": "sha256:<hex>"
    }
  ],
  "missing": [{"path": "<relative>", "severity": "warn"}],
  "denied_attempts": [],
  "timestamp": "<iso-local>",
  "timestamp_utc": "<iso-utc>",
  "timezone": "Asia/Manila"
}
```

---

## 4. Idempotency & cursor semantics

- The `content_hash` field lets `select_scope.py` diff inputs against the last successful run's cursor (stored in `memory-purifier.json` `lastRun.cursor` and manifest `lastSuccessfulCursor`).
- In `incremental` mode, unchanged files (matching hash) drop out of the processing scope. Only new-or-changed files feed Pass 1.
- In `reconciliation` mode, the entire inventory is reprocessed against a widened horizon — content_hash is still captured but not used as a skip filter.

---

## 5. Anti-patterns — do NOT

- Do NOT read files under `memory/` — ever.
- Do NOT recurse into subdirectories beyond `episodes/*.md`.
- Do NOT ingest authority docs via CLI override — always reject with `denied_attempts[]`.
- Do NOT write to any discovered source file. Discovery is read-only.
- Do NOT trust file extensions alone — if a non-markdown file matches an allow pattern (e.g., someone names a binary `MEMORY.md`), still check it is UTF-8 text before returning it. Fail soft with a warning if not.
