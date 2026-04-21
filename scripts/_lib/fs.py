"""Atomic filesystem helpers for JSON/JSONL artifact writes.

The purifier's publish contract requires that any consumer reading an
artifact sees either the prior-run state or a complete new-run state —
never a half-written file. These helpers enforce that by writing to a
temp path in the same directory and calling ``os.replace`` (which is
atomic on POSIX).

``load_json_safe`` is a defensive reader that returns a default when the
file is missing, unreadable, or malformed. Used for optional config
blocks and runtime-metadata probes where absence is legal.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional


def atomic_write_json(path: Path, payload: Any, *, indent: int = 2) -> None:
    """Atomically write a JSON-serializable payload to ``path``.

    Writes a sibling tempfile, flushes+fsyncs, then ``os.replace`` swaps
    it in. Leaves the original file intact on any mid-write failure.
    Creates parent directories as needed.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{path.name}.tmp.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=indent, ensure_ascii=False)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_jsonl(path: Path, records: Iterable[dict]) -> None:
    """Atomically write a sequence of JSON records as JSONL to ``path``.

    Same atomicity story as ``atomic_write_json``. Each record becomes one
    line; no trailing blank line. Empty iterables produce an empty file
    (valid JSONL).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{path.name}.tmp.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False))
                fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_json_safe(path: Path, default: Optional[Any] = None) -> Any:
    """Load ``path`` as JSON, returning ``default`` on any failure.

    Replaces the ad-hoc ``_load_json_safely`` that lived in multiple
    scripts. Treats missing file, permission error, and malformed JSON
    identically — callers that need to distinguish them should check
    ``path.is_file()`` first.
    """
    if default is None:
        default = {}
    try:
        with Path(path).open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


__all__ = ["atomic_write_json", "atomic_write_jsonl", "load_json_safe"]
