"""F: downstream metric correctness — claimCount must use real claim count, not source count."""

import json
import unittest

from _helpers import build_fixtures, cleanup_sandbox, make_sandbox, run_installer, run_pipeline, write_source


class TestDownstreamMetric(unittest.TestCase):
    def test_claim_count_uses_real_claims(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            # 3 source files. Blank-line separation creates multiple paragraphs per
            # file so the candidate count (and hence claim count) exceeds the source count.
            write_source(ws, "MEMORY.md", "# MEMORY\n\nFact A is durable.\n\nFact B is durable.\n\nFact C is durable.\n\nFact D is durable.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\nFact E is durable.\n\nFact F is durable.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n## Step\nMethod G is useful.\n")
            run_installer(sandbox, profile="business")
            build_fixtures(sandbox, profile="business", run_id="metric-run")

            result = run_pipeline(sandbox, run_id="metric-run")
            self.assertEqual(result["status"], "ok")

            signal_path = sandbox["runtime_dir"] / "purifier-downstream-signal.json"
            self.assertTrue(signal_path.is_file(), "purifier-downstream-signal.json should exist after ok run")
            signal = json.loads(signal_path.read_text())

            # sourceCount = 3 (MEMORY, RTMEMORY, PROCEDURES)
            self.assertEqual(signal["sourceCount"], 3, f"sourceCount should reflect inventory: {signal}")
            # claimCount = actual claims in the JSONL
            claims_path = sandbox["runtime_dir"] / "purified-claims.jsonl"
            with claims_path.open() as f:
                actual_claims = sum(1 for line in f if line.strip())
            self.assertEqual(signal["claimCount"], actual_claims,
                             f"claimCount must match lines in purified-claims.jsonl (got {signal['claimCount']}, expected {actual_claims})")
            # Regression guard: the two counts should differ on this fixture (4 paragraphs in MEMORY produced ≥4 claims).
            self.assertNotEqual(signal["claimCount"], signal["sourceCount"],
                                "sanity check: claim count and source count should differ on this fixture")
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
