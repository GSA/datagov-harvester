"""Place-name lookup for the public validator, matching the harvest runner's
`HarvesterDBInterface.get_geo_from_string` without a database.

The harvester resolves a name with `display_name ILIKE '%<name>%'` and
`.scalar()`, so a name resolves only when exactly one locations row matches;
more than one raises and resolves nothing. This module applies the same rule to
`reference_data/location_names.csv`: every non-postal-code row (counties,
states, countries) with its row count, and a flag for names that also appear
inside a postal-code name. Postal-code rows themselves are left out because
each has at least two rows, so they only ever make a match ambiguous.

Known gap: a query that matches one kept name but is also part of a postal-code
name without being that whole name (e.g. "Abbeville", which is both
"Abbeville County, South Carolina" and "Abbeville, AL (36310)") resolves here
but not at harvest.

It answers whether a name resolves, not with what geometry: the validator only
reports warnings. Regenerate the CSV with `scripts/refresh_location_names.py`.
"""

import csv
from bisect import bisect_right
from functools import lru_cache
from pathlib import Path

LOCATION_NAMES_PATH = (
    Path(__file__).resolve().parent / "reference_data" / "location_names.csv"
)

_LIKE_SPECIAL = ("%", "_", "\\")


class _Snapshot:
    def __init__(self, rows: list[dict]):
        self.names = [row["display_name"] for row in rows]
        self.row_counts = [int(row["row_count"]) for row in rows]
        self.in_postal_code_name = [row["in_postal_code_name"] == "1" for row in rows]
        self.lowered = lowered = [name.lower() for name in self.names]
        self.longest_name = max(map(len, lowered), default=0)
        # One newline-delimited string makes a plain substring search a single
        # C-level str.find instead of a Python loop over every name.
        self.haystack = "\n".join(lowered)
        self.starts = []
        offset = 0
        for name in lowered:
            self.starts.append(offset)
            offset += len(name) + 1


@lru_cache(maxsize=1)
def _snapshot() -> _Snapshot:
    with open(LOCATION_NAMES_PATH, newline="") as f:
        return _Snapshot(list(csv.DictReader(f)))


def load_location_names() -> None:
    """Load the CSV now, so a missing or broken file fails at startup."""
    _snapshot()


_ANY_CHAR = None


def _like_segments(pattern: str, max_chars: int) -> list[list] | None:
    """Split a LIKE pattern on its unescaped `%` into segments of characters,
    with `_` as _ANY_CHAR, or None when it needs more than max_chars characters
    to match. Callers pass the whole `%name%` pattern, as the harvester builds
    it, so a name ending in the escape character escapes the closing `%` just
    as it does there."""
    segments = [[]]
    chars_needed = 0
    chars = iter(pattern.lower())
    for char in chars:
        if char == "%":
            if len(segments) == 1 or segments[-1]:
                segments.append([])
            continue
        if char == "\\":
            char = next(chars, None)
            if char is None:
                break
        elif char == "_":
            char = _ANY_CHAR
        segments[-1].append(char)
        chars_needed += 1
        if chars_needed > max_chars:
            return None
    return segments


def _segment_at(name: str, segment: list, position: int) -> bool:
    if position < 0 or position + len(segment) > len(name):
        return False
    return all(
        char is _ANY_CHAR or name[position + i] == char
        for i, char in enumerate(segment)
    )


def _like_match(name: str, segments: list[list]) -> bool:
    """LIKE match without a backtracking regex: with `%` matching any run,
    taking each middle segment at its leftmost fit is always safe, so this is
    linear in the name for each segment and can't be made to blow up."""
    if len(segments) == 1:
        return len(name) == len(segments[0]) and _segment_at(name, segments[0], 0)
    first, *middle, last = segments
    if not _segment_at(name, first, 0):
        return False
    position = len(first)
    for segment in middle:
        while not _segment_at(name, segment, position):
            position += 1
            if position + len(segment) > len(name):
                return False
        position += len(segment)
    return len(name) - len(last) >= position and _segment_at(
        name, last, len(name) - len(last)
    )


def _substring_matches(snapshot: _Snapshot, needle: str):
    """Yield each row whose name contains needle, once per row."""
    if "\n" in needle:
        return
    position = snapshot.haystack.find(needle)
    while position >= 0:
        row = bisect_right(snapshot.starts, position) - 1
        yield row
        if row + 1 >= len(snapshot.starts):
            return
        position = snapshot.haystack.find(needle, snapshot.starts[row + 1])


def _pattern_matches(snapshot: _Snapshot, pattern: str):
    segments = _like_segments(pattern, snapshot.longest_name)
    if segments is None:
        return
    for row, name in enumerate(snapshot.lowered):
        if _like_match(name, segments):
            yield row


# Longer names are looked up uncached so submitted text can't pile up in a
# cache that lives as long as the worker.
_MAX_CACHED_NAME_CHARS = 256


def snapshot_place_lookup(name: str) -> str | None:
    """Return the matching display name when exactly one locations row matches
    `name` the way the harvester's lookup would, else None."""
    if len(name) > _MAX_CACHED_NAME_CHARS:
        return _lookup(name)
    return _cached_lookup(name)


def _lookup(name: str) -> str | None:
    snapshot = _snapshot()
    if any(char in name for char in _LIKE_SPECIAL):
        matches = _pattern_matches(snapshot, f"%{name}%")
    else:
        matches = _substring_matches(snapshot, name.lower())

    matched_row = None
    total_rows = 0
    for row in matches:
        total_rows += snapshot.row_counts[row]
        if total_rows > 1:
            return None
        matched_row = row
    if matched_row is None or snapshot.in_postal_code_name[matched_row]:
        return None
    return snapshot.names[matched_row]


_cached_lookup = lru_cache(maxsize=4096)(_lookup)
