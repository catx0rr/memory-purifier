"""Test helpers — disposable workspace setup + pipeline invocation.

These helpers build a temp workspace with seeded inputs and Pass 1 / Pass 2
fixtures that match whatever candidate IDs the extractor produces. Tests
exercise real subprocess invocations of `scripts/run_purifier.py` with the
`file` backend, so the full pipeline runs end-to-end without hitting a real LLM.
"""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACKAGE_ROOT / "scripts"
INSTALL_SH = PACKAGE_ROOT / "install.sh"


def make_sandbox() -> dict:
    """Create a fresh workspace with config, telemetry, and fixtures dirs.

    Returns a dict of absolute paths for everything a test needs.
    """
    root = Path(tempfile.mkdtemp(prefix="memory-purifier-test-"))
    ws = root / "workspace"
    ws.mkdir()
    (ws / "episodes").mkdir()
    (root / "config").mkdir()
    (root / "telemetry" / "memory-purifier").mkdir(parents=True)
    (root / "fixtures").mkdir()
    return {
        "root": root,
        "workspace": ws,
        "config_root": root / "config",
        "config_file": root / "config" / "memory-purifier" / "memory-purifier.json",
        "telemetry_root": root / "telemetry" / "memory-purifier",
        "global_log_root": root / "telemetry",
        "fixtures_dir": root / "fixtures",
        # Flat runtime: purifier files live directly under <workspace>/runtime/.
        "runtime_dir": ws / "runtime",
    }


def cleanup_sandbox(sandbox: dict) -> None:
    shutil.rmtree(sandbox["root"], ignore_errors=True)


def write_source(ws: Path, name: str, content: str) -> None:
    (ws / name).write_text(content)


def run_installer(sandbox: dict, profile: str = "business") -> None:
    """Invoke install.sh with the sandbox as the root. Uses --local so the
    installer works offline without git. Seeds config + runtime.
    """
    env = os.environ.copy()
    env.update({
        "CONFIG_ROOT": str(sandbox["config_root"]),
        "WORKSPACE": str(sandbox["workspace"]),
        # SKILLS_PATH is set to an isolated dir so --local syncs the package in.
        "SKILLS_PATH": str(sandbox["root"] / "skills"),
        "TELEMETRY_ROOT": str(sandbox["telemetry_root"]),
    })
    subprocess.run(
        ["bash", str(INSTALL_SH), "--local", "--agent-profile", profile, "--skip-cron", "--non-interactive"],
        env=env,
        check=True,
        capture_output=True,
    )


def build_fixtures(sandbox: dict, profile: str, run_id: str, verdict_hook=None, claim_hook=None) -> None:
    """Run discover → scope → extract → cluster against the workspace, then
    synthesize Pass 1 + Pass 2 fixtures matching the produced IDs.

    `verdict_hook(candidate) -> verdict_dict | None` lets a test override the
    default (everything promote). `claim_hook(cluster) -> claim_dict | None`
    does the same for Pass 2.
    """
    workspace = sandbox["workspace"]
    config_file = sandbox["config_file"]
    fixtures_dir = sandbox["fixtures_dir"]

    # Discover → scope → extract
    inv = _run(["discover_sources.py", "--workspace", str(workspace), "--profile", profile, "--config", str(config_file)])
    (sandbox["root"] / "_inv.json").write_text(json.dumps(inv))
    scope = _run(["select_scope.py", "--inventory", str(sandbox["root"] / "_inv.json"), "--mode", "incremental"])
    (sandbox["root"] / "_scope.json").write_text(json.dumps(scope))
    cands = _run([
        "extract_candidates.py",
        "--scope", str(sandbox["root"] / "_scope.json"),
        "--run-id", run_id,
    ])
    (sandbox["root"] / "_cands.json").write_text(json.dumps(cands))

    # Default verdicts = promote everything
    verdicts = []
    for cand in cands["candidates"]:
        v = verdict_hook(cand) if verdict_hook else None
        if v is None:
            scores = {
                "durability": 0.8, "future_judgment_value": 0.7, "action_value": 0.5,
                "identity_relationship_weight": 0.2, "cross_time_persistence": 0.5, "noise_risk": 0.05,
            }
            strength = round(
                sum(scores[k] for k in [
                    "durability", "future_judgment_value", "action_value",
                    "identity_relationship_weight", "cross_time_persistence",
                ]) - scores["noise_risk"],
                3,
            )
            v = {
                "candidate_id": cand["candidate_id"],
                "scores": scores,
                "strength": strength,
                "verdict": "promote",
                "rationale": "test promote",
                "merge_candidate_ids": [],
                "compress_target": None,
            }
        verdicts.append(v)
    (fixtures_dir / "promotion-default.json").write_text(
        json.dumps({"run_id": cands["run_id"], "verdicts": verdicts}, indent=2)
    )

    # Drive pass1 + cluster to generate Pass 2 fixtures
    pass1 = _run([
        "score_promotion.py",
        "--candidates", str(sandbox["root"] / "_cands.json"),
        "--workspace", str(workspace),
        "--runtime-dir", str(sandbox["root"] / "_throwaway"),
        "--backend", "file",
        "--fixture-dir", str(fixtures_dir),
        "--dry-run",
    ])
    (sandbox["root"] / "_pass1.json").write_text(json.dumps(pass1))
    clusters = _run(["cluster_survivors.py", "--pass1", str(sandbox["root"] / "_pass1.json")])

    claims = []
    for cluster in clusters.get("clusters", []):
        c = claim_hook(cluster) if claim_hook else None
        if c is None:
            cand = cluster["candidates"][0]
            prov = [
                {"source": r["source"], "line_span": r["line_span"], "type": "direct", "captured_at": r["captured_at"]}
                for r in cand["source_refs"]
            ]
            c = {
                "claim_id": "<new>",
                "source_cluster_id": cluster["cluster_id"],
                "scores": {
                    "semantic_cluster_confidence": 0.9, "canonical_clarity": 0.9,
                    "provenance_strength": 0.8, "contradiction_pressure": 0.0,
                    "freshness": 0.9, "confidence": 0.9,
                    "route_fitness": 0.9, "supersession_confidence": 0.9,
                },
                "canonical": {
                    "type": "fact", "status": "resolved",
                    "text": cand["text"][:80],
                    "subject": (cand.get("source_refs", [{}])[0].get("source") or "x").replace(".md", ""),
                    "predicate": "is stated in",
                    "object": None,
                    "primary_home": "LTMEMORY.md",
                    "secondary_tags": [],
                },
                "provenance": prov,
                "contradictions": [],
                "supersedes": [],
                "superseded_by": [],
                "freshness_posture": "fresh",
                "confidence_posture": "high",
                "rationale": "test",
                "route_rationale": "test",
            }
        claims.append(c)
    (fixtures_dir / "purifier-default.json").write_text(
        json.dumps({"run_id": clusters["run_id"], "canonical_claims": claims}, indent=2)
    )
    # Clean intermediate scratch
    for fn in ("_inv.json", "_scope.json", "_cands.json", "_pass1.json"):
        p = sandbox["root"] / fn
        if p.exists():
            p.unlink()
    throwaway = sandbox["root"] / "_throwaway"
    if throwaway.exists():
        shutil.rmtree(throwaway, ignore_errors=True)


def run_pipeline(sandbox: dict, profile: str = "business", mode: str = "incremental", run_id: str = None, extra_args=None) -> dict:
    """Invoke run_purifier.py and return the parsed final JSON."""
    argv = [
        "python3", str(SCRIPTS_DIR / "run_purifier.py"),
        "--mode", mode,
        "--workspace", str(sandbox["workspace"]),
        "--profile", profile,
        "--config", str(sandbox["config_file"]),
        "--telemetry-root", str(sandbox["telemetry_root"]),
        "--backend", "file",
        "--fixture-dir", str(sandbox["fixtures_dir"]),
    ]
    if run_id:
        argv.extend(["--run-id", run_id])
    if extra_args:
        argv.extend(extra_args)
    argv.append("--force")  # bypass reconciliation-window guard in tests
    proc = subprocess.run(argv, capture_output=True, text=True)
    assert proc.returncode == 0, f"runner exit {proc.returncode}: {proc.stderr[:500]}"
    return json.loads(proc.stdout)


def load_claims(sandbox: dict) -> list:
    """Read purified-claims.jsonl into a list of claim dicts."""
    path = sandbox["runtime_dir"] / "purified-claims.jsonl"
    if not path.is_file():
        return []
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _run(argv_tail) -> dict:
    """Invoke a script under scripts/ and parse its stdout JSON."""
    argv = ["python3", str(SCRIPTS_DIR / argv_tail[0])] + argv_tail[1:]
    proc = subprocess.run(argv, capture_output=True, text=True)
    assert proc.returncode == 0, f"{argv_tail[0]} exit {proc.returncode}: {proc.stderr[:500]}"
    return json.loads(proc.stdout)
