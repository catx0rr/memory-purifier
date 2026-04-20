"""K: smart-skip enrichment fields land on skip paths (v1.4.0 lock).

After a wet run populates claims, a subsequent identical run skips on
scope=skipped. The final JSON and purifier-last-run-summary.json must
both carry `claimsTotal`, `nextSchedule`, and `recallSurface` fields.
"""

import json
import unittest

from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source


class TestSkipEnrichment(unittest.TestCase):
    def test_skipped_run_carries_claims_total_and_next_schedule(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator prefers terse responses.\nOperator uses Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nOperator works remotely.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nInspect logs first.\n")
            run_installer(sandbox, profile="business")

            build_fixtures(sandbox, profile="business", run_id="skip-run")
            r1 = run_pipeline(sandbox, run_id="skip-run")
            self.assertEqual(r1["status"], "ok", f"run1: {r1}")
            claims_after_r1 = load_claims(sandbox)
            expected_total = len(claims_after_r1)
            self.assertGreater(expected_total, 0, "run1 should produce at least one claim")

            # Run 2 with identical sources → scope=skipped (cursor unchanged).
            build_fixtures(sandbox, profile="business", run_id="skip-run2")
            r2 = run_pipeline(sandbox, run_id="skip-run2")
            self.assertEqual(r2["status"], "skipped", f"run2 should skip: {r2}")

            # Final JSON carries all three enrichment fields.
            self.assertIn("claimsTotal", r2)
            self.assertEqual(r2["claimsTotal"], expected_total,
                             f"claimsTotal on skip should equal lines in purified-claims.jsonl: "
                             f"got {r2['claimsTotal']}, expected {expected_total}")
            self.assertIn("nextSchedule", r2, "skip JSON must include nextSchedule field (may be null)")
            self.assertIn("recallSurface", r2, "skip JSON must include recallSurface field (may be null)")
            # In test sandbox, openclaw absent → nextSchedule is None
            self.assertIsNone(r2["nextSchedule"], f"nextSchedule should be null without openclaw in sandbox; got {r2['nextSchedule']}")
            # No contested/unresolved/retire_candidate claims seeded by default → recallSurface is None
            self.assertIsNone(r2["recallSurface"], f"recallSurface should be null with only 'resolved' claims; got {r2['recallSurface']}")

            # purifier-last-run-summary.json mirrors the final JSON.
            summary_path = sandbox["runtime_dir"] / "purifier-last-run-summary.json"
            summary = json.loads(summary_path.read_text())
            self.assertEqual(summary.get("status"), "skipped")
            self.assertEqual(summary.get("claimsTotal"), expected_total)
            self.assertIn("nextSchedule", summary)
            self.assertIn("recallSurface", summary)
        finally:
            cleanup_sandbox(sandbox)

    def test_skipped_run_with_unresolved_claim_populates_recall(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator prefers terse responses.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n")
            run_installer(sandbox, profile="business")

            build_fixtures(sandbox, profile="business", run_id="skip-recall-run")
            r1 = run_pipeline(sandbox, run_id="skip-recall-run")
            self.assertEqual(r1["status"], "ok")

            # Inject an unresolved claim directly so the recall surface populates on the next skip.
            claims_path = sandbox["runtime_dir"] / "purified-claims.jsonl"
            injected = {
                "id": "cl-unresolved-injected",
                "type": "open_question", "status": "unresolved",
                "text": "What is the operator's preferred escalation path for compliance issues?",
                "subject": "escalation path", "predicate": "is undefined", "object": None,
                "primaryHome": "LTMEMORY.md", "secondaryTags": [],
                "profileScope": "business",
                "provenance": [{"source": "MEMORY.md", "lineSpan": [1, 1], "type": "direct", "capturedAt": "2026-01-01T00:00:00+08:00"}],
                "supersedes": [], "supersededBy": [],
                "updatedAt": "2026-01-01T00:00:00+08:00",
                "updatedAt_utc": "2026-01-01T00:00:00Z",
                "timezone": "Asia/Manila",
            }
            with claims_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(injected) + "\n")

            # Now trigger skip.
            build_fixtures(sandbox, profile="business", run_id="skip-recall-run2")
            r2 = run_pipeline(sandbox, run_id="skip-recall-run2")
            self.assertEqual(r2["status"], "skipped", f"run2 should skip: {r2}")
            recall = r2.get("recallSurface")
            self.assertIsNotNone(recall, "recallSurface should populate when an unresolved claim exists")
            self.assertEqual(recall["claimId"], "cl-unresolved-injected")
            self.assertEqual(recall["status"], "unresolved")
            self.assertIn("recallScore", recall, "v1.4.0 weighted surface must expose recallScore")
            self.assertGreater(recall["recallScore"], 0)
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
