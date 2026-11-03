"""Place-name lookup for the public validator, matching the harvest runner's
`HarvesterDBInterface.get_geo_from_string` without a database.

The harvester resolves a name with `display_name ILIKE '%<name>%'` and
`.scalar()`, so a name resolves only when exactly one locations row matches;
more than one raises and resolves nothing. This module applies the same rule to
a bundled snapshot of the table's display names (one entry per row, so names
shared by several rows still count as several matches). It answers whether a
name resolves, not with what geometry: the validator only reports warnings.

Regenerate the snapshot with `scripts/refresh_location_names.py`.
"""

import json
import re
from bisect import bisect_right
from functools import lru_cache
from pathlib import Path

LOCATION_NAMES_PATH = (
    Path(__file__).resolve().parent / "reference_data" / "location_names.json"
)

_LIKE_SPECIAL = ("%", "_", "\\")


class _Snapshot:
    def __init__(self, names: list[str]):
        self.names = names
        self.lowered = [name.lower() for name in names]
        # One newline-delimited string makes a plain substring search a single
        # C-level str.find instead of a Python loop over every name.
        self.haystack = "\n".join(self.lowered)
        self.starts = []
        offset = 0
        for name in self.lowered:
            self.starts.append(offset)
            offset += len(name) + 1


@lru_cache(maxsize=1)
def _snapshot() -> _Snapshot:
    with open(LOCATION_NAMES_PATH) as f:
        return _Snapshot(json.load(f)["values"])


def load_location_names() -> None:
    """Load the snapshot now, so a missing or broken file fails at startup."""
    _snapshot()


def _like_to_regex(pattern: str) -> re.Pattern:
    """Translate a LIKE pattern to a regex for fullmatch. Callers pass the
    whole `%name%` pattern, as the harvester builds it, so a name ending in
    the escape character escapes the closing `%` just as it does there."""
    parts = []
    chars = iter(pattern)
    for char in chars:
        if char == "\\":
            escaped = next(chars, None)
            parts.append(re.escape(escaped) if escaped is not None else "")
        elif char == "%":
            parts.append(".*")
        elif char == "_":
            parts.append(".")
        else:
            parts.append(re.escape(char))
    return re.compile("".join(parts), re.IGNORECASE | re.DOTALL)


def _single_substring_match(snapshot: _Snapshot, needle: str) -> int | None:
    if "\n" in needle:
        return None
    position = snapshot.haystack.find(needle)
    if position < 0:
        return None
    row = bisect_right(snapshot.starts, position) - 1
    next_row_start = (
        snapshot.starts[row + 1] if row + 1 < len(snapshot.starts) else None
    )
    if (
        next_row_start is not None
        and snapshot.haystack.find(needle, next_row_start) >= 0
    ):
        return None
    return row


def _single_pattern_match(snapshot: _Snapshot, pattern: re.Pattern) -> int | None:
    match_row = None
    for row, name in enumerate(snapshot.names):
        if pattern.fullmatch(name):
            if match_row is not None:
                return None
            match_row = row
    return match_row


@lru_cache(maxsize=4096)
def snapshot_place_lookup(name: str) -> str | None:
    """Return the matching display name when exactly one locations row matches
    `name` the way the harvester's lookup would, else None."""
    snapshot = _snapshot()
    if any(char in name for char in _LIKE_SPECIAL):
        row = _single_pattern_match(snapshot, _like_to_regex(f"%{name}%"))
    else:
        row = _single_substring_match(snapshot, name.lower())
    return None if row is None else snapshot.names[row]
