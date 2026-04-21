"""Timezone-aware timestamp helpers.

Single source of truth for every timestamp the purifier emits. Before v1.5.0
each core script carried its own ``timestamp_triple`` helper and a few sites
bypassed it with ``datetime.now().astimezone()`` — which uses system-local
rather than the configured timezone. That gap let emitted timestamps drift
from their labels in any environment where host TZ != configured TZ.

This module fixes that by resolving the timezone explicitly via
``zoneinfo.ZoneInfo(tz_name)`` on every call. The local offset always
matches the label.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Asia/Manila"


def timestamp_triple(tz_name: str = DEFAULT_TIMEZONE) -> dict:
    """Emit the canonical timestamp triple.

    Returns::

        {
          "timestamp":     "<local ISO 8601 with offset>",
          "timestamp_utc": "<UTC ISO 8601 with Z suffix>",
          "timezone":      "<IANA name>",
        }

    The local offset is always derived from ``ZoneInfo(tz_name)``, never
    from the host's system-local TZ. An invalid ``tz_name`` falls back to
    ``DEFAULT_TIMEZONE`` rather than raising — tests cover the fallback.
    """
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz_name = DEFAULT_TIMEZONE
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    now_local = datetime.now(tz)
    now_utc = now_local.astimezone(timezone.utc)
    return {
        "timestamp": now_local.isoformat(),
        "timestamp_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "timezone": tz_name,
    }


def resolve_timezone(
    cli_arg: Optional[str],
    config_path: Optional[Path] = None,
    default: str = DEFAULT_TIMEZONE,
) -> str:
    """Resolve the effective timezone via the standard ladder.

    Priority (first non-empty wins):

    1. Explicit CLI arg
    2. ``memory-purifier.json`` ``timezone`` field (if config_path is a file)
    3. ``default`` (``Asia/Manila`` unless overridden)

    The resolved name is not validated here — callers pass it into
    ``timestamp_triple`` which handles invalid names gracefully.
    """
    if cli_arg:
        return cli_arg
    if config_path is not None and config_path.is_file():
        try:
            cfg = json.loads(config_path.read_text())
        except (OSError, ValueError):
            cfg = {}
        tz = cfg.get("timezone")
        if isinstance(tz, str) and tz:
            return tz
    return default


def local_date_str(tz_name: str = DEFAULT_TIMEZONE) -> str:
    """Return today's date in ``YYYY-MM-DD`` form under ``tz_name``.

    Used for date-shard filenames (e.g. ``memory-log-YYYY-MM-DD.jsonl``).
    Centralized so telemetry shard boundaries are consistent with the rest
    of the emitted timestamps — all anchored to the configured timezone,
    never system-local.
    """
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    return datetime.now(tz).strftime("%Y-%m-%d")


def local_report_timestamp(tz_name: str = DEFAULT_TIMEZONE, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """Return a human-readable local timestamp for operator-facing reports.

    Used for ``last-run.md`` regeneration lines and similar prose surfaces.
    Anchored to the configured timezone so what an operator reads matches
    the tz label emitted elsewhere in the run.
    """
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    return datetime.now(tz).strftime(fmt)


__all__ = [
    "DEFAULT_TIMEZONE",
    "timestamp_triple",
    "resolve_timezone",
    "local_date_str",
    "local_report_timestamp",
]
