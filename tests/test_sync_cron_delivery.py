"""M: sync_cron_delivery.py edge cases (v1.4.0).

Covers:
- No memory-state.json => skipped_no_desired_state
- openclaw unavailable on PATH => skipped_no_openclaw
- reporting.enabled flip without channel/to in state or config =>
  desired_channel/to fields on the result JSON remain None; the helper
  can still run (it just can't sync announce without the target).
"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
HELPER = PACKAGE_ROOT / "scripts" / "sync_cron_delivery.py"


def _run_sync(workspace: Path, config: Path = None, dry_run: bool = True, extra_env: dict = None) -> dict:
    """Invoke sync_cron_delivery.py and return parsed stdout JSON."""
    argv = ["python3", str(HELPER), "--workspace", str(workspace)]
    if config is not None:
        argv += ["--config", str(config)]
    if dry_run:
        argv += ["--dry-run"]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(argv, capture_output=True, text=True, env=env)
    assert proc.returncode == 0, f"sync helper exit {proc.returncode}: {proc.stderr[:500]}"
    return json.loads(proc.stdout)


class TestSyncCronDelivery(unittest.TestCase):

    def test_help_renders(self):
        """--help must render cleanly — CLI regression guard."""
        proc = subprocess.run(
            ["python3", str(HELPER), "--help"],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, f"--help failed: {proc.stderr}")
        self.assertIn("--workspace", proc.stdout)
        self.assertIn("--dry-run", proc.stdout)

    def test_no_memory_state_returns_skipped_no_desired_state(self):
        """No memory-state.json => helper is a no-op with that status."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-sync-test-"))
        try:
            # tmp has no runtime/ directory at all
            result = _run_sync(workspace=tmp, dry_run=True)
            self.assertEqual(result["status"], "skipped_no_desired_state", result)
            self.assertIsNone(result.get("desired_reporting_enabled"))
            self.assertIsNone(result.get("desired_channel"))
            self.assertIsNone(result.get("desired_to"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_no_openclaw_on_path_returns_skipped_no_openclaw(self):
        """PATH without openclaw => helper records skipped_no_openclaw."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-sync-test-"))
        try:
            ws = tmp / "workspace"
            (ws / "runtime").mkdir(parents=True)
            # Valid memory-state.json with announce=true
            state = {
                "memoryPurifier": {
                    "reporting": {
                        "enabled": True,
                        "mode": "summary",
                        "delivery": {"channel": "whatsapp", "to": "+6312345"},
                    }
                }
            }
            (ws / "runtime" / "memory-state.json").write_text(json.dumps(state))

            # Strip openclaw from PATH by using a minimal PATH.
            result = _run_sync(
                workspace=ws, dry_run=True,
                extra_env={"PATH": "/usr/bin:/bin"},  # no openclaw on this PATH
            )
            self.assertEqual(result["status"], "skipped_no_openclaw", result)
            # Desired state was read correctly even though we couldn't act on it.
            self.assertTrue(result.get("desired_reporting_enabled"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_announce_flip_without_channel_records_desired_state_as_none(self):
        """reporting.enabled=true but delivery.channel/to empty + no config fallback
        => desired_channel/to on the result stay None. (The refusal path is
        exercised only at mutation time when openclaw is present; without it we
        land in skipped_no_openclaw with None targets.)"""
        tmp = Path(tempfile.mkdtemp(prefix="mp-sync-test-"))
        try:
            ws = tmp / "workspace"
            (ws / "runtime").mkdir(parents=True)
            state = {
                "memoryPurifier": {
                    "reporting": {
                        "enabled": True,
                        "mode": "summary",
                        "delivery": {"channel": None, "to": None},
                    }
                }
            }
            (ws / "runtime" / "memory-state.json").write_text(json.dumps(state))

            result = _run_sync(
                workspace=ws, dry_run=True,
                extra_env={"PATH": "/usr/bin:/bin"},
            )
            # openclaw absent path wins first, but we still want desired fields
            # to reflect what was read (both None in this case).
            self.assertTrue(result.get("desired_reporting_enabled"))
            self.assertIsNone(result.get("desired_channel"),
                              f"desired_channel should be None; got {result.get('desired_channel')}")
            self.assertIsNone(result.get("desired_to"),
                              f"desired_to should be None; got {result.get('desired_to')}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_desired_channel_resolves_from_config_fallback(self):
        """When memory-state has no channel/to, helper falls back to cron.announce_channel/to in config."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-sync-test-"))
        try:
            ws = tmp / "workspace"
            (ws / "runtime").mkdir(parents=True)
            state = {
                "memoryPurifier": {
                    "reporting": {
                        "enabled": True,
                        "mode": "summary",
                        "delivery": {"channel": None, "to": None},
                    }
                }
            }
            (ws / "runtime" / "memory-state.json").write_text(json.dumps(state))

            config_path = tmp / "config" / "memory-purifier" / "memory-purifier.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps({
                "version": "1.4.0", "profile": "business", "timezone": "Asia/Manila",
                "cadence": {"incremental": [], "reconciliation": []},
                "cron": {
                    "tz": "Asia/Manila", "timeout_seconds": 1200,
                    "announce": True,
                    "announce_channel": "telegram",
                    "announce_to": "@ops-channel",
                },
            }))

            result = _run_sync(
                workspace=ws, config=config_path, dry_run=True,
                extra_env={"PATH": "/usr/bin:/bin"},
            )
            self.assertEqual(result.get("desired_channel"), "telegram",
                             f"config fallback should resolve channel; got {result}")
            self.assertEqual(result.get("desired_to"), "@ops-channel",
                             f"config fallback should resolve to; got {result}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
