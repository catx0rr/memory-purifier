"""Version constants for the purifier package.

Four distinct version identifiers, each tracking a different concern:

``PURIFIER_PACKAGE_VERSION``
    Release string. Surfaces in docs, CHANGELOG, and the installer's
    config seed. Bumped every release regardless of logic changes.

``PURIFIER_LOGIC_VERSION``
    Code logic fingerprint. Bumped when semantic behavior (retrieval,
    reuse, routing, validation) changes in ways that would invalidate
    prior artifact state. A mismatch between installed ``PURIFIER_LOGIC_VERSION``
    and the ``logicVersion`` stored in ``purified-manifest.json`` triggers
    the refuse-and-lock upgrade flow (see ``run_purifier.py``).

``PURIFIER_MANIFEST_SCHEMA``
    Manifest JSON shape version. Bumped on breaking shape change to the
    manifest itself. v1.5.0 ships as ``"1"``.

``PURIFIER_ARTIFACT_SCHEMA``
    Shape version for ``purified-claims.jsonl`` and ``purified-contradictions.jsonl``.
    Bumped only on breaking (not additive) changes. v1.5.0 adds optional
    fields (``duplicateDisposition``, ``probable_duplicate_of``, etc.) so
    this stays at ``"1"``.

A release that changes any of the four version identifiers may trigger a
forced reconciliation on next run — see ``references/prompt-contracts.md``
for the upgrade state machine.
"""

from __future__ import annotations


PURIFIER_PACKAGE_VERSION: str = "1.6.0"
PURIFIER_LOGIC_VERSION: str = "1.6.0"
PURIFIER_MANIFEST_SCHEMA: str = "1"
PURIFIER_ARTIFACT_SCHEMA: str = "1"


def version_tuple(ver: str) -> tuple:
    """Parse a dotted version string into a tuple of ints.

    Non-numeric segments (e.g., ``"1.5.0-rc1"``) degrade to 0, so
    ``version_tuple("1.5.0-rc1") == (1, 5, 0)``. Good enough for the
    monotonic comparisons the upgrade state machine does; not a full
    PEP 440 parser.
    """
    out = []
    for part in str(ver).split("."):
        num = ""
        for ch in part:
            if ch.isdigit():
                num += ch
            else:
                break
        out.append(int(num) if num else 0)
    return tuple(out)


__all__ = [
    "PURIFIER_PACKAGE_VERSION",
    "PURIFIER_LOGIC_VERSION",
    "PURIFIER_MANIFEST_SCHEMA",
    "PURIFIER_ARTIFACT_SCHEMA",
    "version_tuple",
]
