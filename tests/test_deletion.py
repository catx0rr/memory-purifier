"""D: deletion — source removed, claim that depends only on it must be marked retire_candidate."""

import unittest

from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source


class TestDeletion(unittest.TestCase):
    def test_removed_source_triggers_retire_candidate(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            # Isolate the removable surface: RTMEMORY is the only source of one claim.
            write_source(ws, "MEMORY.md", "# MEMORY\nOperator is based in Asia/Manila.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\nDecided to split the purifier from the reconciler.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n## Debug\nInspect logs.\n")
            run_installer(sandbox, profile="business")
            build_fixtures(sandbox, profile="business", run_id="del-1")

            first = run_pipeline(sandbox, run_id="del-1")
            self.assertEqual(first["status"], "ok")

            claims_before = load_claims(sandbox)
            rtmem_claims = [c for c in claims_before if any(p.get("source") == "RTMEMORY.md" for p in (c.get("provenance") or []))]
            self.assertTrue(rtmem_claims, "expected at least one claim sourced from RTMEMORY.md")

            # Remove RTMEMORY.md and rerun.
            (ws / "RTMEMORY.md").unlink()
            # No fixture rebuild needed — scope will be empty except for removed_sources.
            result = run_pipeline(sandbox, run_id="del-2")
            self.assertEqual(result["status"], "ok", f"stale sweep should finish ok: {result}")

            claims_after = load_claims(sandbox)
            # Every claim that was sourced ONLY from RTMEMORY.md must now be retire_candidate.
            by_id = {c["id"]: c for c in claims_after}
            for prior in rtmem_claims:
                sources = {p.get("source") for p in (prior.get("provenance") or [])}
                after = by_id[prior["id"]]
                if sources == {"RTMEMORY.md"}:
                    self.assertEqual(
                        after["status"], "retire_candidate",
                        f"claim {prior['id']} should be retire_candidate (only RTMEMORY source, now removed)",
                    )
                    self.assertTrue(after.get("retirementReasons"), "must record retirementReasons trace")
                else:
                    # Multi-source claims keep their current status even when one source drops.
                    self.assertNotEqual(after["status"], "retire_candidate")
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
