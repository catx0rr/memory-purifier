"""J: install.sh announce flag validation matrix (v1.4.0 lock).

Verifies that install.sh:
- rejects `--cron-announce true` without `--cron-announce-channel`
- rejects `--cron-announce true --cron-announce-channel X` without `--cron-announce-to`
- accepts the full announce tuple and seeds config + memory-state correctly
"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
INSTALL_SH = PACKAGE_ROOT / "install.sh"


def _sandbox_env():
    """Create an isolated sandbox and return (root, env_dict) for install.sh."""
    root = Path(tempfile.mkdtemp(prefix="mp-announce-test-"))
    env = os.environ.copy()
    env.update({
        "CONFIG_ROOT": str(root / "config"),
        "WORKSPACE": str(root / "workspace"),
        "SKILLS_PATH": str(root / "skills"),
        "TELEMETRY_ROOT": str(root / "telemetry" / "memory-purifier"),
    })
    return root, env


def _run_installer(args: list, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(INSTALL_SH)] + args,
        env=env,
        capture_output=True,
        text=True,
    )


class TestAnnounceValidation(unittest.TestCase):

    def test_announce_true_without_channel_rejected(self):
        root, env = _sandbox_env()
        try:
            proc = _run_installer(
                ["--local", "--non-interactive", "--skip-cron",
                 "--agent-profile", "business", "--cron-announce", "true"],
                env=env,
            )
            self.assertNotEqual(proc.returncode, 0, f"install should reject: {proc.stdout}\n{proc.stderr}")
            combined = proc.stdout + proc.stderr
            self.assertIn("cron-announce-channel is required", combined,
                          f"expected clear error message; got: {combined[:500]}")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_announce_true_channel_without_to_rejected(self):
        root, env = _sandbox_env()
        try:
            proc = _run_installer(
                ["--local", "--non-interactive", "--skip-cron",
                 "--agent-profile", "business", "--cron-announce", "true",
                 "--cron-announce-channel", "whatsapp"],
                env=env,
            )
            self.assertNotEqual(proc.returncode, 0, f"install should reject: {proc.stdout}\n{proc.stderr}")
            combined = proc.stdout + proc.stderr
            self.assertIn("cron-announce-to is required", combined,
                          f"expected clear error message; got: {combined[:500]}")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_announce_true_full_tuple_accepted(self):
        root, env = _sandbox_env()
        try:
            proc = _run_installer(
                ["--local", "--non-interactive", "--skip-cron",
                 "--agent-profile", "personal", "--cron-announce", "true",
                 "--cron-announce-channel", "whatsapp",
                 "--cron-announce-to", "+6312345"],
                env=env,
            )
            self.assertEqual(proc.returncode, 0,
                             f"install should accept full tuple: rc={proc.returncode}\n"
                             f"stdout={proc.stdout[-400:]}\nstderr={proc.stderr[-400:]}")

            # Assert config seeded correctly
            cfg_path = Path(env["CONFIG_ROOT"]) / "memory-purifier" / "memory-purifier.json"
            self.assertTrue(cfg_path.is_file(), f"config file missing at {cfg_path}")
            cfg = json.loads(cfg_path.read_text())
            self.assertTrue(cfg["cron"]["announce"], f"cron.announce should be True; got {cfg['cron']}")
            self.assertEqual(cfg["cron"]["announce_channel"], "whatsapp", f"{cfg['cron']}")
            self.assertEqual(cfg["cron"]["announce_to"], "+6312345", f"{cfg['cron']}")

            # Assert memory-state seeded correctly
            ms_path = Path(env["WORKSPACE"]) / "runtime" / "memory-state.json"
            self.assertTrue(ms_path.is_file(), f"memory-state missing at {ms_path}")
            ms = json.loads(ms_path.read_text())
            rp = ms["memoryPurifier"]["reporting"]
            self.assertTrue(rp["enabled"], f"reporting.enabled should be True; got {rp}")
            self.assertEqual(rp["delivery"]["channel"], "whatsapp", f"{rp}")
            self.assertEqual(rp["delivery"]["to"], "+6312345", f"{rp}")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_announce_false_ignores_channel_to(self):
        """announce=false path: channel/to (even if passed) become null in config + state."""
        root, env = _sandbox_env()
        try:
            proc = _run_installer(
                ["--local", "--non-interactive", "--skip-cron",
                 "--agent-profile", "business", "--cron-announce", "false",
                 "--cron-announce-channel", "whatsapp",
                 "--cron-announce-to", "+6312345"],
                env=env,
            )
            self.assertEqual(proc.returncode, 0, f"install should accept: {proc.stdout}\n{proc.stderr}")
            cfg = json.loads((Path(env["CONFIG_ROOT"]) / "memory-purifier" / "memory-purifier.json").read_text())
            self.assertFalse(cfg["cron"]["announce"])
            self.assertIsNone(cfg["cron"]["announce_channel"],
                              f"announce_channel should be null when announce=false; got {cfg['cron']}")
            self.assertIsNone(cfg["cron"]["announce_to"])
            ms = json.loads((Path(env["WORKSPACE"]) / "runtime" / "memory-state.json").read_text())
            rp = ms["memoryPurifier"]["reporting"]
            self.assertFalse(rp["enabled"])
            self.assertEqual(rp["delivery"]["channel"], "last")
            self.assertIsNone(rp["delivery"]["to"])
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
