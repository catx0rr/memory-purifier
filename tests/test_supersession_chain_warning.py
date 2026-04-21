"""H: script-level supersession chain sanity check (v1.4.0).

Seed `purified-claims.jsonl` with a prior claim already marked `superseded`,
then emit a new claim that supersedes that already-superseded prior. Assert
that `manifest.warnings[]` picks up the chain-warning entry.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import build_fixtures, cleanup_sandbox, make_sandbox, run_installer, run_pipeline, write_source  # noqa: E402


class TestSupersessionChainWarning(unittest.TestCase):
    def test_supersedes_already_superseded_emits_warning(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator originally based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n")
            run_installer(sandbox, profile="business")

            # Pre-seed the claims JSONL with two claims: cl-A is already
            # superseded by cl-B (cl-B resolved). A new run will emit cl-C
            # whose supersedes=[cl-A] — which should trigger the warning.
            claims_path = sandbox["runtime_dir"] / "purified-claims.jsonl"
            claims_path.parent.mkdir(parents=True, exist_ok=True)
            seed_claim_a = {
                "id": "cl-A", "type": "fact", "status": "superseded",
                "text": "Operator is based in Asia/Manila.",
                "subject": "operator", "predicate": "is based in", "object": "Asia/Manila",
                "primaryHome": "LTMEMORY.md", "secondaryTags": [],
                "profileScope": "business",
                "provenance": [{"source": "MEMORY.md", "lineSpan": [1, 1], "type": "direct", "capturedAt": "2026-01-01T00:00:00+08:00"}],
                "supersedes": [], "supersededBy": ["cl-B"],
                "updatedAt": "2026-01-01T00:00:00+08:00",
                "updatedAt_utc": "2026-01-01T00:00:00Z",
                "timezone": "Asia/Manila",
            }
            seed_claim_b = {
                "id": "cl-B", "type": "fact", "status": "resolved",
                "text": "Operator is based in Asia/Singapore.",
                "subject": "operator", "predicate": "is based in", "object": "Asia/Singapore",
                "primaryHome": "LTMEMORY.md", "secondaryTags": [],
                "profileScope": "business",
                "provenance": [{"source": "MEMORY.md", "lineSpan": [1, 1], "type": "direct", "capturedAt": "2026-03-01T00:00:00+08:00"}],
                "supersedes": ["cl-A"], "supersededBy": [],
                "updatedAt": "2026-03-01T00:00:00+08:00",
                "updatedAt_utc": "2026-03-01T00:00:00Z",
                "timezone": "Asia/Manila",
            }
            with claims_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps(seed_claim_a) + "\n")
                f.write(json.dumps(seed_claim_b) + "\n")

            # Emit a new claim cl-C whose supersedes = ["cl-A"] — chain warning should fire.
            def claim_hook(cluster):
                cand = cluster["candidates"][0]
                prov = [
                    {"source": r["source"], "line_span": r["line_span"], "type": "direct", "captured_at": r["captured_at"]}
                    for r in cand["source_refs"]
                ]
                # Use <new> so Pass 2 validator accepts the claim_id. Use a
                # distinct (subject, predicate) triple so _semantic_reuse_match
                # does NOT match prior cl-B — otherwise the new claim would
                # reuse cl-B's id and the test would no longer exercise the
                # supersession-chain path cleanly.
                return {
                    "claim_id": "<new>",
                    "source_cluster_id": cluster["cluster_id"],
                    "scores": {
                        "semantic_cluster_confidence": 0.9, "canonical_clarity": 0.9,
                        "provenance_strength": 0.9, "contradiction_pressure": 0.0,
                        "freshness": 0.9, "confidence": 0.9,
                        "route_fitness": 0.9, "supersession_confidence": 0.9,
                    },
                    "canonical": {
                        "type": "fact", "status": "resolved",
                        "text": "Operator relocation recorded: Asia/Tokyo as of April 2026.",
                        "subject": "operator relocation", "predicate": "records", "object": "Asia/Tokyo",
                        "primary_home": "LTMEMORY.md", "secondary_tags": [],
                    },
                    "provenance": prov,
                    "contradictions": [],
                    "supersedes": ["cl-A"],  # targeting an already-superseded prior
                    "superseded_by": [],
                    "freshness_posture": "fresh",
                    "confidence_posture": "high",
                    "rationale": "chain-warning test",
                    "route_rationale": "fact -> LTMEMORY",
                }

            # Run reconciliation so the seeded prior claims stay intact and
            # extraction re-reads from scratch.
            build_fixtures(sandbox, profile="business", run_id="chain-warn-run", claim_hook=claim_hook)
            result = run_pipeline(sandbox, mode="reconciliation", run_id="chain-warn-run")

            # Warning must not halt the pipeline.
            self.assertEqual(result["status"], "ok", f"chain-warning must not halt: {result}")

            # Check manifest.warnings for the chain-warning entry.
            manifest_path = sandbox["runtime_dir"] / "purified-manifest.json"
            manifest = json.loads(manifest_path.read_text())
            warnings = manifest.get("warnings") or []
            match = [w for w in warnings if isinstance(w, str) and "supersession_chain_warning" in w and "cl-A" in w and "already superseded" in w]
            self.assertTrue(
                match,
                f"expected supersession_chain_warning mentioning cl-A in manifest.warnings; got: {warnings}",
            )

            # Last-run summary should reflect at least one warning.
            summary_path = sandbox["runtime_dir"] / "purifier-last-run-summary.json"
            summary = json.loads(summary_path.read_text())
            self.assertGreaterEqual(
                summary.get("warningCount", 0), 1,
                f"summary.warningCount should be >= 1; got {summary.get('warningCount')}",
            )
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
