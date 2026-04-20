"""B: supersession chains — prior claim replaced by later claim; supersedes/supersededBy preserved."""

import json
import unittest

from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source


class TestSupersession(unittest.TestCase):
    def test_later_claim_supersedes_prior(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator is based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nOperator works remotely.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nInspect logs.\n")
            run_installer(sandbox, profile="business")
            build_fixtures(sandbox, profile="business", run_id="super-run-1")

            first = run_pipeline(sandbox, run_id="super-run-1")
            self.assertEqual(first["status"], "ok")
            claims_after_first = load_claims(sandbox)
            # Pick the first claim to supersede on the next run.
            prior_id = claims_after_first[0]["id"]

            # Mutate workspace so scope is non-empty + build a pass2 fixture
            # whose first claim supersedes prior_id.
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator relocated to Asia/Singapore in April 2026.\nOperator is based in Asia/Manila.\n")

            def claim_hook(cluster):
                cand = cluster["candidates"][0]
                prov = [{"source": r["source"], "line_span": r["line_span"], "type": "direct", "captured_at": r["captured_at"]} for r in cand["source_refs"]]
                if "Singapore" in cand["text"]:
                    return {
                        "claim_id": "<new>",
                        "source_cluster_id": cluster["cluster_id"],
                        "scores": {
                            "semantic_cluster_confidence": 0.95, "canonical_clarity": 0.95,
                            "provenance_strength": 0.9, "contradiction_pressure": 0.0,
                            "freshness": 0.95, "confidence": 0.95,
                            "route_fitness": 0.95, "supersession_confidence": 0.95,
                        },
                        "canonical": {
                            "type": "fact", "status": "resolved",
                            "text": "Operator relocated from Asia/Manila to Asia/Singapore in April 2026.",
                            "subject": "operator", "predicate": "has timezone", "object": "Asia/Singapore",
                            "primary_home": "LTMEMORY.md", "secondary_tags": [],
                        },
                        "provenance": prov,
                        "contradictions": [],
                        "supersedes": [prior_id],
                        "superseded_by": [],
                        "freshness_posture": "fresh",
                        "confidence_posture": "high",
                        "rationale": "supersession test",
                        "route_rationale": "identity fact → LTMEMORY",
                    }
                return None

            # Rebuild fixtures for the full current workspace state. Using
            # reconciliation mode ensures extraction re-reads ALL sources so the
            # fixture's candidate_ids align with the pipeline's extraction.
            build_fixtures(sandbox, profile="business", run_id="super-run-2", claim_hook=claim_hook)
            second = run_pipeline(sandbox, mode="reconciliation", run_id="super-run-2")
            self.assertEqual(second["status"], "ok", f"second run: {second}")

            claims = {c["id"]: c for c in load_claims(sandbox)}
            self.assertIn(prior_id, claims, "prior claim must still be present in JSONL (not deleted)")
            self.assertEqual(claims[prior_id]["status"], "superseded")
            self.assertIn(
                claims[prior_id].get("supersededBy", [])[0] if claims[prior_id].get("supersededBy") else None,
                {c["id"] for c in claims.values() if c.get("supersedes")},
                "supersededBy must point at the superseding claim",
            )

            # Routes file must exclude the superseded claim.
            routes_path = sandbox["runtime_dir"] / "purified-routes.json"
            routes = json.loads(routes_path.read_text())
            all_routed_ids = [cid for lst in routes.values() for cid in lst]
            self.assertNotIn(prior_id, all_routed_ids, "routes must exclude superseded claim")
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
