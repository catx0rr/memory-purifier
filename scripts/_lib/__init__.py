"""Shared helpers for memory-purifier scripts.

Consolidates correctness-sensitive helpers (timezone, atomic I/O, version
constants) that were previously duplicated across multiple scripts. Keep
this package narrow: new helpers land here only when duplication becomes
a correctness risk, not for cosmetic refactoring.

Modules:
- time_utils : timezone-aware timestamp triple + timezone resolver
- fs         : atomic JSON/JSONL writes + safe JSON loader
- version    : package + logic + schema version constants

All scripts import via ``sys.path`` insertion of ``Path(__file__).parent`` —
see any core script for the pattern.
"""
