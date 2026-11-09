"""Place-name lookup for the public validator, matching the harvest runner's
`HarvesterDBInterface.get_geo_from_string` without a database.

The harvester resolves a name with `display_name ILIKE '%<name>%'` and
`.scalar()`, so a name resolves only when exactly one locations row matches;
more than one raises and resolves nothing. This module applies the same rule to
`reference_data/location_names.csv`: every non-postal-code row (counties,
states, countries) with its row count, and a flag for names that also appear
inside a postal-code name. Postal-code rows themselves are left out because
each has at least two rows, so they only ever make a match ambiguous.

Known gaps:
- A query that matches one kept name but is also part of a postal-code name
  without being that whole name (e.g. "Abbeville", which is both
  "Abbeville County, South Carolina" and "Abbeville, AL (36310)") resolves here
  but not at harvest.
- `%`, `_` and `\\` are matched as plain characters, not as ILIKE wildcards. A
  wildcard match scans every name instead of one substring search, and a
  submitter can make each name unique to dodge the cache, so honoring them let
  one document run for many minutes. Real place names don't contain them.

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


class _Snapshot:
    def __init__(self, rows: list[dict]):
        self.names = [row["display_name"] for row in rows]
        self.row_counts = [int(row["row_count"]) for row in rows]
        self.in_postal_code_name = [row["in_postal_code_name"] == "1" for row in rows]
        lowered = [name.lower() for name in self.names]
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
