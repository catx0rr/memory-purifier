"""JSON envelope parsing for Pass 1 / Pass 2 model output (v1.7.0).

Hardened against common model-output drift the prompt rules can't
always prevent:

  - prose before the JSON (``"Here is the result:\\n{...}"``)
  - prose after the JSON (``"{...}\\n\\nLet me know if..."``)
  - fenced JSON blocks (``"```json\\n{...}\\n```"``)
  - missing load-bearing top-level keys (no ``run_id``, no ``verdicts``, etc.)

Always preserves the raw input on failure so the operator-facing
failure record (``purifier-failed-<pass>-<run_id>.json``) carries
exactly what the model returned — not just an error class name.

The parser fails fast when the required top-level envelope keys are
absent, so operators see ``"top-level `verdicts` missing"`` instead of
a deeper validator error chain.
"""

from __future__ import annotations

import json
from typing import Any, Iterable


class PassOutputParseError(RuntimeError):
    """Raised when a pass's model output cannot be turned into usable JSON.

    Attributes:
      - ``reason``: one-line human-readable cause
      - ``raw``: the original model output (unmodified), for the failure record
      - ``recovery_attempts``: ordered list of parse strategies tried
        (``direct`` / ``fence_strip`` / ``brace_scan``) plus their
        outcomes. Useful for debugging silently-drifting model output.
    """

    def __init__(self, reason: str, raw: str, recovery_attempts: list | None = None):
        super().__init__(reason)
        self.reason = reason
        self.raw = raw
        self.recovery_attempts = list(recovery_attempts or [])


def parse_pass_output(
    raw: str,
    *,
    required_top_level: Iterable[str] = (),
) -> dict:
    """Parse a Pass 1 or Pass 2 model-output envelope.

    Recovery ladder (each attempt is logged in ``recovery_attempts``):

      1. Direct ``json.loads(raw.strip())``
      2. Strip leading/trailing ```...``` markdown fence, retry
      3. Scan for outermost ``{`` … ``}`` and parse that substring

    After a successful parse, fast-fail if any key in
    ``required_top_level`` is missing from the top-level dict. The
    caller-side validator would catch this eventually, but failing here
    gives a crisper operator message and keeps the raw output attached.

    Raises ``PassOutputParseError`` on unrecoverable failure. The caller
    is expected to catch it, write the ``raw`` into a failure record,
    and move on (retry / partial_failure).
    """
    if not isinstance(raw, str):
        raw = str(raw or "")

    recovery_attempts: list = []
    cleaned = raw.strip()
    if not cleaned:
        raise PassOutputParseError(
            "model returned empty output",
            raw=raw,
            recovery_attempts=recovery_attempts,
        )

    parsed: Any = None

    # Attempt 1 — direct parse.
    try:
        parsed = json.loads(cleaned)
        recovery_attempts.append("direct")
    except json.JSONDecodeError as e:
        recovery_attempts.append(f"direct_failed: {e.msg}")

    # Attempt 2 — strip markdown fence wrapper if present.
    if parsed is None and cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        defenced = "\n".join(lines).strip()
        try:
            parsed = json.loads(defenced)
            recovery_attempts.append("fence_strip")
        except json.JSONDecodeError as e:
            recovery_attempts.append(f"fence_strip_failed: {e.msg}")

    # Attempt 3 — scan for outermost { ... } span (prose before/after).
    if parsed is None:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end > start:
            try:
                parsed = json.loads(cleaned[start:end + 1])
                recovery_attempts.append("brace_scan")
            except json.JSONDecodeError as e:
                recovery_attempts.append(f"brace_scan_failed: {e.msg}")

    if parsed is None:
        raise PassOutputParseError(
            "could not parse model output as JSON; "
            "all recovery strategies failed (direct → fence_strip → brace_scan)",
            raw=raw,
            recovery_attempts=recovery_attempts,
        )

    if not isinstance(parsed, dict):
        raise PassOutputParseError(
            f"parsed JSON is not a top-level object (got {type(parsed).__name__})",
            raw=raw,
            recovery_attempts=recovery_attempts + ["not_a_dict"],
        )

    # Fast-fail on missing load-bearing top-level envelope keys.
    missing = [k for k in required_top_level if k not in parsed]
    if missing:
        raise PassOutputParseError(
            f"top-level required field(s) missing: {missing}",
            raw=raw,
            recovery_attempts=recovery_attempts + [f"missing_top_level:{missing}"],
        )

    return parsed


__all__ = ["PassOutputParseError", "parse_pass_output"]
