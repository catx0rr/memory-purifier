#!/usr/bin/env python3
"""Pass 1 — promotion scoring.

Loads prompts/promotion-pass.md as the system prompt, sends a candidate batch
(from extract_candidates.py) to the configured model backend, validates the
returned verdicts against the Pass 1 output schema, and persists rejected /
deferred candidates to JSONL. Emits one JSON summary object to stdout.

Backends:
- claude-code   (default) — shells out to `claude -p`
- anthropic-sdk           — uses the anthropic Python SDK (requires ANTHROPIC_API_KEY)
- file                    — reads a canned response; used for smoke tests
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_BACKEND = "claude-code"
VALID_VERDICTS = {"reject", "defer", "compress", "merge", "promote"}
SCORE_KEYS = [
    "durability",
    "future_judgment_value",
    "action_value",
    "identity_relationship_weight",
    "cross_time_persistence",
    "noise_risk",
]


def timestamp_triple(tz_name: str = "Asia/Manila") -> dict:
    now_local = datetime.now().astimezone()
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def compute_strength(scores: dict) -> float:
    return (
        scores.get("durability", 0.0)
        + scores.get("future_judgment_value", 0.0)
        + scores.get("action_value", 0.0)
        + scores.get("identity_relationship_weight", 0.0)
        + scores.get("cross_time_persistence", 0.0)
        - scores.get("noise_risk", 0.0)
    )


def validate_verdicts(verdicts_obj, input_candidates: list, run_id: str) -> tuple:
    """Cross-check verdicts against Pass 1 output schema.

    The strength-formula self-check (re-derived vs model-emitted) is the key
    defense against hallucinated scoring.
    """
    errors: list = []

    if not isinstance(verdicts_obj, dict):
        return False, ["output is not a JSON object"]

    if verdicts_obj.get("run_id") != run_id:
        errors.append(f"run_id mismatch: expected {run_id!r}, got {verdicts_obj.get('run_id')!r}")

    verdicts = verdicts_obj.get("verdicts")
    if not isinstance(verdicts, list):
        return False, errors + ["verdicts field missing or not a list"]

    input_ids = {c["candidate_id"] for c in input_candidates}
    seen_ids: set = set()

    for i, v in enumerate(verdicts):
        if not isinstance(v, dict):
            errors.append(f"verdicts[{i}] is not an object")
            continue

        cid = v.get("candidate_id")
        if not cid:
            errors.append(f"verdicts[{i}] missing candidate_id")
            continue
        if cid in seen_ids:
            errors.append(f"verdicts[{i}] duplicate candidate_id: {cid}")
        seen_ids.add(cid)
        if cid not in input_ids:
            errors.append(f"verdicts[{i}] unknown candidate_id: {cid}")

        scores = v.get("scores", {})
        if not isinstance(scores, dict):
            errors.append(f"verdicts[{i}] scores is not an object")
            scores = {}
        for k in SCORE_KEYS:
            val = scores.get(k)
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                errors.append(f"verdicts[{i}] scores.{k} missing or not numeric")
                continue
            if not (0.0 <= float(val) <= 1.0):
                errors.append(f"verdicts[{i}] scores.{k}={val} out of [0,1]")

        strength = v.get("strength")
        if isinstance(strength, (int, float)) and not isinstance(strength, bool):
            expected = compute_strength(scores)
            if abs(float(strength) - expected) > 0.01:
                errors.append(
                    f"verdicts[{i}] strength={strength} != formula={expected:.3f} (|diff|>0.01)"
                )
        else:
            errors.append(f"verdicts[{i}] strength missing or not numeric")

        verdict = v.get("verdict")
        if verdict not in VALID_VERDICTS:
            errors.append(f"verdicts[{i}] verdict={verdict!r} not in {sorted(VALID_VERDICTS)}")

        merge_ids = v.get("merge_candidate_ids", [])
        if verdict == "merge":
            if not isinstance(merge_ids, list) or not merge_ids:
                errors.append(f"verdicts[{i}] verdict=merge but merge_candidate_ids is empty")
        else:
            if merge_ids:
                errors.append(f"verdicts[{i}] verdict={verdict} but merge_candidate_ids is populated")

        compress_target = v.get("compress_target")
        if verdict == "compress":
            if not compress_target or not isinstance(compress_target, str):
                errors.append(f"verdicts[{i}] verdict=compress but compress_target missing or not string")
        else:
            if compress_target is not None:
                errors.append(f"verdicts[{i}] verdict={verdict} but compress_target is populated")

    missing = input_ids - seen_ids
    if missing:
        sample = sorted(missing)[:5]
        errors.append(f"{len(missing)} input candidate_id(s) missing from verdicts; sample: {sample}")

    return len(errors) == 0, errors


def _fixture_lookup(fixture_dir: Path, fixture_file: Path, input_payload: dict) -> Path:
    if fixture_file and fixture_file.is_file():
        return fixture_file
    key = hashlib.sha256(
        json.dumps(input_payload, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:16]
    specific = fixture_dir / f"promotion-{key}.json"
    if specific.is_file():
        return specific
    default = fixture_dir / "promotion-default.json"
    if default.is_file():
        return default
    raise FileNotFoundError(
        f"no fixture at {specific} or {default} (hash-key: {key})"
    )


def _approximate_tokens(text: str) -> int:
    """Conservative char-to-token heuristic for when provider usage metadata is unavailable.

    ~4 chars per token is a rough English-plus-JSON average. Tokenizer-based
    estimates would be more accurate but require the anthropic package; this
    heuristic is acceptable per the audit's 'approximate' rule and avoids
    asking the model to self-report its own usage.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


def _usage_unavailable() -> dict:
    return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "source": "unavailable"}


def _usage_approximate(prompt_text: str, completion_text: str) -> dict:
    prompt = _approximate_tokens(prompt_text)
    completion = _approximate_tokens(completion_text)
    if prompt == 0 and completion == 0:
        return _usage_unavailable()
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
        "source": "approximate",
    }


def _usage_exact(input_tokens: int, output_tokens: int) -> dict:
    return {
        "prompt_tokens": int(input_tokens),
        "completion_tokens": int(output_tokens),
        "total_tokens": int(input_tokens) + int(output_tokens),
        "source": "exact",
    }


def invoke_backend(
    backend: str,
    prompt_file: Path,
    input_payload: dict,
    fixture_dir: Path = None,
    fixture_file: Path = None,
    model: str = None,
    max_tokens: int = None,
    timeout: int = 300,
) -> dict:
    """Invoke the configured model backend.

    Returns {"raw": <response text>, "usage": <token_usage block>}. Token usage
    is 'exact' when the provider returns usage metadata, 'approximate' when
    computed from prompt/completion char counts, 'unavailable' otherwise
    (e.g. file-backed fixtures that never hit a real model).
    """
    if backend == "file":
        if not (fixture_dir or fixture_file):
            raise ValueError("backend=file requires --fixture-dir or --fixture-file")
        path = _fixture_lookup(
            Path(fixture_dir) if fixture_dir else Path(),
            Path(fixture_file) if fixture_file else None,
            input_payload,
        )
        return {"raw": path.read_text(), "usage": _usage_unavailable()}

    if backend == "claude-code":
        cmd = ["claude", "-p"]
        if model:
            cmd += ["--model", model]
        system_text = prompt_file.read_text()
        user_text = json.dumps(input_payload, indent=2, ensure_ascii=False)
        combined = (
            f"{system_text}\n\n---\n\nInput payload:\n\n```json\n{user_text}\n```\n\n"
            "Respond with the JSON envelope only."
        )
        proc = subprocess.run(
            cmd, input=combined, text=True, capture_output=True, timeout=timeout
        )
        if proc.returncode != 0:
            raise RuntimeError(f"claude-code backend failed (rc={proc.returncode}): {proc.stderr.strip()}")
        return {"raw": proc.stdout, "usage": _usage_approximate(combined, proc.stdout)}

    if backend == "anthropic-sdk":
        try:
            import anthropic
        except ImportError as e:
            raise RuntimeError("anthropic package not installed; pip install anthropic") from e
        client = anthropic.Anthropic()
        system_text = prompt_file.read_text()
        user_text = json.dumps(input_payload, indent=2, ensure_ascii=False)
        resp = client.messages.create(
            model=model or "claude-opus-4-7",
            max_tokens=max_tokens or 8192,
            system=system_text,
            messages=[{"role": "user", "content": user_text}],
        )
        raw = resp.content[0].text
        u = getattr(resp, "usage", None)
        if u is not None:
            usage = _usage_exact(
                getattr(u, "input_tokens", 0) or 0,
                getattr(u, "output_tokens", 0) or 0,
            )
        else:
            usage = _usage_approximate(system_text + user_text, raw)
        return {"raw": raw, "usage": usage}

    raise ValueError(f"unknown backend: {backend}")


def _merge_usage(a: dict, b: dict) -> dict:
    """Aggregate two token_usage blocks. Source degrades to the weakest of the two."""
    src_rank = {"exact": 0, "approximate": 1, "unavailable": 2}
    a_src = a.get("source", "unavailable")
    b_src = b.get("source", "unavailable")
    merged_src = a_src if src_rank[a_src] >= src_rank[b_src] else b_src
    return {
        "prompt_tokens": int(a.get("prompt_tokens") or 0) + int(b.get("prompt_tokens") or 0),
        "completion_tokens": int(a.get("completion_tokens") or 0) + int(b.get("completion_tokens") or 0),
        "total_tokens": int(a.get("total_tokens") or 0) + int(b.get("total_tokens") or 0),
        "source": merged_src,
    }


def extract_json(raw: str):
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end > start:
            return json.loads(raw[start:end + 1])
        raise


def _append_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Pass 1 — promotion scoring.")
    ap.add_argument("--candidates", required=True, help="Candidates JSON path (from extract_candidates.py) or '-' for stdin")
    ap.add_argument("--prompt", help="Path to prompts/promotion-pass.md (default: resolved from script location)")
    ap.add_argument("--workspace", help="Workspace root override")
    ap.add_argument("--runtime-dir", help="Runtime dir override (default: <workspace>/runtime)")
    ap.add_argument("--backend", default=None, help="Model backend: claude-code | anthropic-sdk | file")
    ap.add_argument("--model", help="Model override")
    ap.add_argument("--max-tokens", type=int, help="Max output tokens")
    ap.add_argument("--fixture-dir", help="Fixture directory (backend=file)")
    ap.add_argument("--fixture-file", help="Explicit fixture file path (backend=file)")
    ap.add_argument("--retry", type=int, default=1, help="Retries on validation failure (default: 1)")
    ap.add_argument("--timeout", type=int, default=300, help="Backend call timeout in seconds")
    ap.add_argument("--timezone", help="IANA timezone name (default: from candidates or Asia/Manila)")
    ap.add_argument("--dry-run", action="store_true", help="Validate only; do not persist JSONL or write failure files")

    args = ap.parse_args()

    if args.candidates == "-":
        cand_obj = json.load(sys.stdin)
    else:
        cand_obj = json.loads(Path(args.candidates).expanduser().read_text())

    tz_name = args.timezone or cand_obj.get("timezone") or "Asia/Manila"

    if cand_obj.get("status") != "ok":
        out = {
            "status": "skipped",
            "reason": f"candidates status is {cand_obj.get('status')!r}",
            "pass": "promotion",
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    run_id = cand_obj["run_id"]
    candidates = cand_obj.get("candidates", [])

    if not candidates:
        out = {
            "status": "skipped",
            "reason": "no candidates to score",
            "run_id": run_id,
            "pass": "promotion",
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    script_dir = Path(__file__).resolve().parent
    prompt_path = Path(args.prompt) if args.prompt else (script_dir.parent / "prompts" / "promotion-pass.md")
    if not prompt_path.is_file():
        out = {
            "status": "error",
            "error": f"prompt file not found: {prompt_path}",
            "pass": "promotion",
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    backend = args.backend or os.environ.get("MEMORY_PURIFIER_BACKEND") or DEFAULT_BACKEND

    workspace_hint = args.workspace or cand_obj.get("workspace") or os.environ.get("WORKSPACE")
    workspace = Path(workspace_hint).expanduser() if workspace_hint else (Path.home() / ".openclaw" / "workspace")
    runtime_dir = Path(args.runtime_dir).expanduser() if args.runtime_dir else (workspace / "runtime")
    locks_dir = runtime_dir / "locks"

    input_payload = {
        "run_id": run_id,
        "mode": cand_obj.get("mode", "incremental"),
        "profile_scope": cand_obj.get("profile_scope", "business"),
        "candidates": candidates,
    }

    last_errors: list = []
    raw_response = None
    verdicts_obj = None
    attempts = 0
    total_usage = _usage_unavailable()

    for _ in range(args.retry + 1):
        attempts += 1
        try:
            resp = invoke_backend(
                backend=backend,
                prompt_file=prompt_path,
                input_payload=input_payload,
                fixture_dir=args.fixture_dir,
                fixture_file=args.fixture_file,
                model=args.model,
                max_tokens=args.max_tokens,
                timeout=args.timeout,
            )
            raw_response = resp["raw"]
            total_usage = _merge_usage(total_usage, resp.get("usage") or _usage_unavailable())
        except Exception as e:
            last_errors = [f"backend invocation failed: {type(e).__name__}: {e}"]
            continue

        try:
            parsed = extract_json(raw_response)
        except Exception as e:
            last_errors = [f"JSON parse failed: {type(e).__name__}: {e}"]
            continue

        ok, errors = validate_verdicts(parsed, candidates, run_id)
        if ok:
            verdicts_obj = parsed
            last_errors = []
            break
        last_errors = errors

    if verdicts_obj is None:
        fail_path = locks_dir / f"purifier-failed-promotion-{run_id}.json"
        fail_payload = {
            "run_id": run_id,
            "pass": "promotion",
            "attempts": attempts,
            "errors": last_errors,
            "raw_response": raw_response,
            "input_payload": input_payload,
            **timestamp_triple(tz_name),
        }
        if not args.dry_run:
            locks_dir.mkdir(parents=True, exist_ok=True)
            fail_path.write_text(json.dumps(fail_payload, indent=2, ensure_ascii=False))

        out = {
            "status": "partial_failure",
            "run_id": run_id,
            "pass": "promotion",
            "backend": backend,
            "attempts": attempts,
            "errors": last_errors,
            "failed_record_path": str(fail_path) if not args.dry_run else None,
            "token_usage": total_usage,
            "dry_run": args.dry_run,
            **timestamp_triple(tz_name),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    verdict_stats = {v: 0 for v in VALID_VERDICTS}
    survivors: list = []
    deferred: list = []
    rejected: list = []

    cand_by_id = {c["candidate_id"]: c for c in candidates}
    ts = timestamp_triple(tz_name)

    for v in verdicts_obj["verdicts"]:
        vname = v["verdict"]
        verdict_stats[vname] += 1
        cid = v["candidate_id"]
        cand = cand_by_id.get(cid, {})
        enriched = {
            "candidate_id": cid,
            "run_id": run_id,
            "text": cand.get("text"),
            "type_hint": cand.get("type_hint"),
            "source_refs": cand.get("source_refs"),
            "verdict": vname,
            "strength": v.get("strength"),
            "scores": v.get("scores"),
            "rationale": v.get("rationale"),
            "merge_candidate_ids": v.get("merge_candidate_ids", []),
            "compress_target": v.get("compress_target"),
            **ts,
        }
        if vname == "reject":
            rejected.append(enriched)
        elif vname == "defer":
            deferred.append(enriched)
        else:
            survivors.append(enriched)

    if not args.dry_run:
        runtime_dir.mkdir(parents=True, exist_ok=True)
        if rejected:
            _append_jsonl(runtime_dir / "rejected-candidates.jsonl", rejected)
        if deferred:
            _append_jsonl(runtime_dir / "deferred-candidates.jsonl", deferred)

    out = {
        "status": "ok",
        "run_id": run_id,
        "pass": "promotion",
        "backend": backend,
        "attempts": attempts,
        "mode": input_payload["mode"],
        "profile_scope": input_payload["profile_scope"],
        "workspace": str(workspace),
        "candidate_count": len(candidates),
        "verdict_stats": verdict_stats,
        "survivor_count": len(survivors),
        "rejected_written": 0 if args.dry_run else len(rejected),
        "deferred_written": 0 if args.dry_run else len(deferred),
        "survivors": survivors,
        "token_usage": total_usage,
        "dry_run": args.dry_run,
        **ts,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
