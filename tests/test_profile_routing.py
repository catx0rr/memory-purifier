"""C: business vs personal routing — personal-only surfaces gated to personal profile only."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import build_fixtures, cleanup_sandbox, make_sandbox, run_installer, run_pipeline, write_source  # noqa: E402


class TestProfileRouting(unittest.TestCase):
    def test_business_does_not_emit_personal_views(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator is based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nGeneric lesson.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nLogs.\n")
            # Personal-only inputs present but profile=business — purifier must ignore them.
            write_source(ws, "CHRONICLES.md", "# CHRONICLES\nBusiness run must ignore this.\n")
            write_source(ws, "DREAMS.md", "# DREAMS\nSame.\n")
            run_installer(sandbox, profile="business")
            build_fixtures(sandbox, profile="business", run_id="prof-biz")

            result = run_pipeline(sandbox, profile="business", run_id="prof-biz")
            self.assertEqual(result["status"], "ok")

            # No personal-only views emitted.
            self.assertFalse((ws / "HISTORY.md").exists(), "HISTORY.md must NOT exist on business profile")
            self.assertFalse((ws / "WISHES.md").exists(), "WISHES.md must NOT exist on business profile")
            self.assertTrue((ws / "LTMEMORY.md").exists(), "LTMEMORY.md expected on business profile")

            # Final report flags the skips.
            skipped_paths = [v.get("path") for v in (result.get("trigger", {}) or {}).values() if isinstance(v, dict)]
            # The skipped list lives inside render.views_skipped — check via manifest.
            manifest = sandbox["runtime_dir"] / "purified-manifest.json"
            self.assertTrue(manifest.is_file(), "manifest should exist after ok run")

        finally:
            cleanup_sandbox(sandbox)

    def test_personal_emits_history_and_wishes(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator is based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nLesson.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nLogs.\n")
            write_source(ws, "CHRONICLES.md", "# CHRONICLES\nOperator shipped memory-purifier V1 in 2026.\n")
            write_source(ws, "DREAMS.md", "# DREAMS\nOperator wants a fully-automated memory stack.\n")
            run_installer(sandbox, profile="personal")

            def claim_hook(cluster):
                cand = cluster["candidates"][0]
                prov = [{"source": r["source"], "line_span": r["line_span"], "type": "direct", "captured_at": r["captured_at"]} for r in cand["source_refs"]]
                src = prov[0]["source"] if prov else ""
                if src == "CHRONICLES.md":
                    return {
                        "claim_id": "<new>",
                        "source_cluster_id": cluster["cluster_id"],
                        "scores": {k: 0.9 for k in [
                            "semantic_cluster_confidence", "canonical_clarity",
                            "provenance_strength", "contradiction_pressure",
                            "freshness", "confidence", "route_fitness", "supersession_confidence",
                        ]},
                        "canonical": {
                            "type": "milestone", "status": "resolved",
                            "text": "Operator shipped memory-purifier V1 in 2026.",
                            "subject": "memory-purifier-v1", "predicate": "shipped", "object": "2026",
                            "primary_home": "HISTORY.md", "secondary_tags": [],
                        },
                        "provenance": prov,
                        "contradictions": [], "supersedes": [], "superseded_by": [],
                        "freshness_posture": "fresh", "confidence_posture": "high",
                        "rationale": "test", "route_rationale": "milestone",
                    }
                if src == "DREAMS.md":
                    return {
                        "claim_id": "<new>",
                        "source_cluster_id": cluster["cluster_id"],
                        "scores": {k: 0.9 for k in [
                            "semantic_cluster_confidence", "canonical_clarity",
                            "provenance_strength", "contradiction_pressure",
                            "freshness", "confidence", "route_fitness", "supersession_confidence",
                        ]},
                        "canonical": {
                            "type": "aspiration", "status": "resolved",
                            "text": "Operator wants a fully-automated memory stack.",
                            "subject": "automated memory stack", "predicate": "aspires to", "object": "full automation",
                            "primary_home": "WISHES.md", "secondary_tags": [],
                        },
                        "provenance": prov,
                        "contradictions": [], "supersedes": [], "superseded_by": [],
                        "freshness_posture": "fresh", "confidence_posture": "high",
                        "rationale": "test", "route_rationale": "aspiration",
                    }
                return None

            build_fixtures(sandbox, profile="personal", run_id="prof-pers", claim_hook=claim_hook)
            result = run_pipeline(sandbox, profile="personal", run_id="prof-pers")
            self.assertEqual(result["status"], "ok")
            self.assertTrue((ws / "HISTORY.md").exists())
            self.assertTrue((ws / "WISHES.md").exists())
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
