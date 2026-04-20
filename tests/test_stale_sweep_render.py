"""I: stale-only retire sweep re-renders markdown views (locks v1.3.0 bugfix).

When scope=skipped but removed_sources is non-empty, the pipeline runs
assemble_artifacts.py to mark orphaned claims as retire_candidate AND
then runs render_views.py to refresh the markdown views on disk.

Before v1.3.0 the stale-only path skipped render — views on disk still
referenced retired claims until the next non-skip run. This test locks
the fix going forward.
"""

import time
import unittest

from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source


class TestStaleSweepRender(unittest.TestCase):
    def test_stale_sweep_rerenders_views(self):
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", "# MEMORY\n\nOperator prefers terse responses.\n\nOperator uses Asia/Manila timezone.\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\nOperator works remotely.\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n## Debug\nInspect logs before escalation.\n")
            run_installer(sandbox, profile="business")

            build_fixtures(sandbox, profile="business", run_id="stale-run")
            r1 = run_pipeline(sandbox, run_id="stale-run")
            self.assertEqual(r1["status"], "ok", f"run1: {r1}")

            ltmemory = ws / "LTMEMORY.md"
            playbooks = ws / "PLAYBOOKS.md"
            self.assertTrue(ltmemory.is_file(), "LTMEMORY.md should exist after run1")
            mtime_before = ltmemory.stat().st_mtime
            content_before = ltmemory.read_text()

            # Sleep long enough that mtime delta is observable on low-precision filesystems.
            time.sleep(1.1)

            # Delete PROCEDURES.md so its claims orphan → stale-only path.
            (ws / "PROCEDURES.md").unlink()

            # Second run: scope=skipped (no new content) but removed_sources non-empty.
            # build_fixtures still runs to refresh the fixture files — uses reconciliation
            # so extraction sees current workspace state.
            build_fixtures(sandbox, profile="business", run_id="stale-run2")
            r2 = run_pipeline(sandbox, mode="incremental", run_id="stale-run2")

            # Final JSON assertions on the stale-only path shape.
            self.assertEqual(r2["status"], "ok", f"stale sweep must complete ok: {r2}")
            self.assertIn("stale sweep", str(r2.get("haltReason", "")),
                          f"haltReason should indicate stale sweep: {r2.get('haltReason')}")
            steps = r2.get("steps") or {}
            render_step = steps.get("render") or {}
            self.assertTrue(
                render_step.get("stale_sweep") is True,
                f"render step should carry stale_sweep=True on this path; got {render_step}",
            )
            self.assertEqual(
                render_step.get("status"), "ok",
                f"render step should succeed on stale-sweep path; got {render_step}",
            )

            # LTMEMORY.md mtime must advance (views were re-rendered).
            mtime_after = ltmemory.stat().st_mtime
            self.assertGreater(
                mtime_after, mtime_before,
                "LTMEMORY.md mtime should advance — stale sweep re-rendered views",
            )

            # At least one claim should be retire_candidate (the PROCEDURES-sourced one).
            claims_after = load_claims(sandbox)
            retire_count = sum(1 for c in claims_after if c.get("status") == "retire_candidate")
            self.assertGreaterEqual(
                retire_count, 1,
                f"expected >=1 retire_candidate claim after source removal; got {retire_count}",
            )

            # Routes must exclude retired claims.
            import json as _json
            routes_path = sandbox["runtime_dir"] / "purified-routes.json"
            routes = _json.loads(routes_path.read_text())
            all_routed_ids = {cid for lst in routes.values() for cid in lst}
            retired_ids = {c["id"] for c in claims_after if c.get("status") == "retire_candidate"}
            self.assertFalse(
                retired_ids & all_routed_ids,
                f"retired claims must not appear in routes; overlap: {retired_ids & all_routed_ids}",
            )
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
