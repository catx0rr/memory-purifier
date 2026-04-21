"""Locked final-status taxonomy — Contract 1 for v1.5.0.

Single-sourced so ``run_purifier.CANONICAL_STATUSES``,
``write_manifest.py --status`` argparse choices, and
``validate_outputs.check_manifest`` all agree byte-for-byte. Adding or
removing a status must happen here and only here; divergence is a
contract break.

Deprecated top-level statuses (``error``, ``validation_failed``,
``transactional_commit_failed``, ``skipped_no_work``) are deliberately
absent. Internal child-script output may still use its own status
strings — see ``run_purifier._run_script`` for the internal-to-top-level
fold — but no runtime contract surface (manifest, last-run summary,
orchestrator final state, validator) may carry them.
"""

from __future__ import annotations


# Runtime-visible top-level statuses. Order here is intentional: the
# `write_manifest.py` argparse choice list uses this same order.
CANONICAL_TOP_LEVEL_STATUSES: tuple = (
    "ok",
    "skipped",
    "skipped_superseded",
    "partial_failure",
    "failed",
    "upgrade_required",
)

# Frozen set form for membership tests.
CANONICAL_TOP_LEVEL_STATUS_SET: frozenset = frozenset(CANONICAL_TOP_LEVEL_STATUSES)

# Deprecated top-level values that must never appear on a runtime surface.
# Retained here as a sentinel set so regression tests can grep/assert on it.
DEPRECATED_TOP_LEVEL_STATUSES: frozenset = frozenset({
    "error",
    "validation_failed",
    "transactional_commit_failed",
    "skipped_no_work",
})


__all__ = [
    "CANONICAL_TOP_LEVEL_STATUSES",
    "CANONICAL_TOP_LEVEL_STATUS_SET",
    "DEPRECATED_TOP_LEVEL_STATUSES",
]
