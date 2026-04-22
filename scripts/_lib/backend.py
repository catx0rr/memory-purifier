"""Scoring backend taxonomy + preflight (v1.6.0).

v1.6.0 emergency patch: memory-purifier lives inside the OpenClaw stack.
The seeded default was ``claude-code``, which assumed the Claude CLI
binary on PATH — wrong for this deployment target and causing Pass 1 /
Pass 2 to fail before purifier semantics even ran. This module fixes it:

  - ``DEFAULT_BACKEND`` is now ``"openclaw"``.
  - ``BACKEND_CHOICES`` lists every supported backend exactly once so
    both scoring scripts stay byte-identical in their argparse choices.
  - ``preflight_backend()`` runs BEFORE any subprocess invocation so a
    missing binary or bad config fails loudly and early, not as an
    opaque ``FileNotFoundError`` buried inside retry logic.

Supported backends:

  - ``openclaw`` — default. Invokes the documented headless-inference
    CLI: ``openclaw infer model run --prompt <body> --json`` with an
    optional ``--model <provider/model>`` override. See
    ``score_promotion.invoke_backend`` for the full contract. The env
    var ``MEMORY_PURIFIER_OPENCLAW_CMD`` is a **base-command override
    only** — it replaces the ``openclaw`` executable prefix (e.g. a
    wrapper script path), while the script ALWAYS appends the required
    ``infer model run --prompt ... --json`` tail so the override cannot
    silently drop the scoring payload.
  - ``claude-code`` — legacy. Invokes ``claude -p``. Still supported for
    operators who have the Claude CLI installed.
  - ``anthropic-sdk`` — Python SDK. Requires ``anthropic`` package.
  - ``file`` — deterministic fixture backend for tests; no network.

Backend preflight policy (Contract: fail loud, fail early):

  - If the selected backend's binary/SDK prerequisite is missing,
    ``preflight_backend`` raises ``BackendUnavailableError`` with an
    operator-readable message that names the backend, the missing
    prerequisite, and the config path to edit.
  - The ``file`` backend is always available when a fixture path is
    provided; the preflight just validates the path resolves.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional


# Default backend for fresh v1.6.0 installs. This is the authoritative
# value; ``install.sh`` and ``references/config-template.md`` should
# match it.
DEFAULT_BACKEND: str = "openclaw"

# Every supported backend exactly once. Both scoring scripts pull their
# argparse ``--backend`` choices from this tuple.
BACKEND_CHOICES: tuple = (
    "openclaw",
    "claude-code",
    "anthropic-sdk",
    "file",
)

BACKEND_CHOICE_SET: frozenset = frozenset(BACKEND_CHOICES)


# Deprecated default (pre-v1.6.0). Orchestrator checks for this when
# applying the auto-substitution / warning policy on upgraded installs.
DEPRECATED_DEFAULT_BACKEND: str = "claude-code"


class BackendUnavailableError(RuntimeError):
    """Raised by ``preflight_backend`` when the selected backend's
    prerequisite is missing. Carries an operator-readable message."""


def preflight_backend(
    backend: str,
    *,
    fixture_dir: Optional[Path] = None,
    fixture_file: Optional[Path] = None,
) -> None:
    """Validate that the selected backend can actually run.

    Fails early with a clear message rather than letting the scoring
    subprocess crash with an opaque ``FileNotFoundError`` mid-retry.

    - ``openclaw``      → confirms ``openclaw`` binary on PATH
    - ``claude-code``   → confirms ``claude`` binary on PATH
    - ``anthropic-sdk`` → confirms ``anthropic`` package importable
    - ``file``          → confirms fixture path is provided + resolvable
    - unknown backend   → raises with the supported list

    Override knob: ``MEMORY_PURIFIER_OPENCLAW_CMD`` lets operators
    point at an alternate openclaw invocation (e.g. a wrapper script).
    When set, only the FIRST token of that override is probed by the
    preflight.
    """
    if backend not in BACKEND_CHOICE_SET:
        raise BackendUnavailableError(
            f"unsupported backend={backend!r}; supported: {sorted(BACKEND_CHOICE_SET)}. "
            f"Edit prompts.backend in memory-purifier.json."
        )

    if backend == "openclaw":
        override = os.environ.get("MEMORY_PURIFIER_OPENCLAW_CMD")
        probe = override.split()[0] if override else "openclaw"
        if shutil.which(probe) is None:
            raise BackendUnavailableError(
                f"openclaw backend selected but {probe!r} is not on PATH. "
                f"Install OpenClaw or set prompts.backend to one of "
                f"{sorted(BACKEND_CHOICE_SET - {backend})} in memory-purifier.json."
            )
        return

    if backend == "claude-code":
        if shutil.which("claude") is None:
            raise BackendUnavailableError(
                "claude-code backend selected but 'claude' binary is not on PATH. "
                "Install the Claude CLI or switch prompts.backend to 'openclaw' "
                "(v1.6.0+ default) in memory-purifier.json."
            )
        return

    if backend == "anthropic-sdk":
        try:
            import anthropic  # noqa: F401  (import-probe only)
        except ImportError as e:
            raise BackendUnavailableError(
                "anthropic-sdk backend selected but 'anthropic' package is not "
                "installed. Run `pip install anthropic` or switch prompts.backend "
                "to 'openclaw' in memory-purifier.json."
            ) from e
        return

    if backend == "file":
        if not (fixture_dir or fixture_file):
            raise BackendUnavailableError(
                "file backend selected but neither --fixture-dir nor --fixture-file "
                "was provided. This backend is for deterministic testing only."
            )
        if fixture_file and not Path(fixture_file).is_file():
            raise BackendUnavailableError(
                f"file backend: --fixture-file={fixture_file!r} does not exist."
            )
        if fixture_dir and not Path(fixture_dir).is_dir():
            raise BackendUnavailableError(
                f"file backend: --fixture-dir={fixture_dir!r} does not exist."
            )
        return


__all__ = [
    "DEFAULT_BACKEND",
    "BACKEND_CHOICES",
    "BACKEND_CHOICE_SET",
    "DEPRECATED_DEFAULT_BACKEND",
    "BackendUnavailableError",
    "preflight_backend",
]
