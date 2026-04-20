"""A: rerun idempotency — same input twice must not multiply claims or churn markdown."""

import unittest

from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source


class TestIdempotency(unittest.TestCase):
    def test_rerun_same_input_no_duplication(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator is based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nOperator prefers terse replies.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nInspect logs.\n")
            run_installer(sandbox, profile="business")
            run_id = "idem-run"
            build_fixtures(sandbox, profile="business", run_id=run_id)

            first = run_pipeline(sandbox, run_id=run_id)
            self.assertEqual(first["status"], "ok", f"first run not ok: {first}")
            claims_1 = load_claims(sandbox)
            ltmemory_1 = (ws / "LTMEMORY.md").read_text()

            # Second run — no workspace mutation, so scope will skip (cursor unchanged).
            # Fixture run_id doesn't matter because pass1 isn't invoked.
            second = run_pipeline(sandbox, run_id=run_id + "-2")
            # Second run's scope is empty → skipped. That's the idempotent outcome.
            self.assertIn(second["status"], {"ok", "skipped"})
            claims_2 = load_claims(sandbox)

            # No duplication.
            self.assertEqual(len(claims_1), len(claims_2), "rerun must not duplicate claims")
            # Same IDs.
            self.assertEqual(
                sorted(c["id"] for c in claims_1),
                sorted(c["id"] for c in claims_2),
            )

            # Markdown unchanged (byte-identical) — the render skipped because scope skipped.
            # If the second run did re-render, only the regeneration timestamp line could differ.
            ltmemory_2 = (ws / "LTMEMORY.md").read_text()
            self.assertEqual(
                ltmemory_1.replace("Regenerated", "_R_"),  # strip the timestamp line variance
                ltmemory_2.replace("Regenerated", "_R_"),
            )
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
