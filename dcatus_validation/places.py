"""Place-name lookup for the public validator, matching the harvest runner's
`HarvesterDBInterface.get_geo_from_string` without the harvest database.

The harvester resolves a name with `display_name ILIKE '%<name>%'` and
`.scalar()`, so a name resolves only when exactly one locations row matches and
that row has a geometry. `reference_data/location_names.sqlite` holds every
locations row (display name and whether it has a geometry, not the geometry
itself) behind a trigram index, and this module applies the same rule.

Known gap: `%`, `_` and `\\` are matched as plain characters, not as ILIKE
wildcards. Honoring them means scanning every row per lookup, and a submitter
can make each name unique to dodge the cache, so one document could run for
minutes. Real place names don't contain them.

Regenerate the file with `scripts/refresh_location_names.py`.
"""

import sqlite3
import threading
from functools import lru_cache
from pathlib import Path

LOCATION_NAMES_PATH = (
    Path(__file__).resolve().parent / "reference_data" / "location_names.sqlite"
)

# Matches scripts/refresh_location_names.py; shorter needles use a lookup table.
_TRIGRAM = 3

_local = threading.local()


def _connection() -> sqlite3.Connection:
    # One read-only connection per thread: gunicorn runs gthread workers.
    if getattr(_local, "path", None) != LOCATION_NAMES_PATH:
        uri = f"{LOCATION_NAMES_PATH.as_uri()}?mode=ro&immutable=1"
        _local.conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        _local.path = LOCATION_NAMES_PATH
    return _local.conn


@lru_cache(maxsize=1)
def _max_name_chars(path: Path) -> int:
    return _connection().execute("SELECT max_name_chars FROM meta").fetchone()[0]


def load_location_names() -> None:
    """Open the file now, so a missing or broken one fails at startup."""
    _max_name_chars(LOCATION_NAMES_PATH)
    _connection().execute("SELECT 1 FROM locations LIMIT 1").fetchall()


def snapshot_place_lookup(name: str) -> str | None:
    """Return the matching display name when the harvester's lookup would
    resolve `name` to a geometry, else None."""
    # Longer than every name, so it can't match; checked before the cache so
    # submitted text can't pile up there.
    if len(name) > _max_name_chars(LOCATION_NAMES_PATH):
        return None
    return _cached_lookup(name)


@lru_cache(maxsize=4096)
def _cached_lookup(name: str) -> str | None:
    needle = name.lower()
    if not needle:
        return None
    if len(needle) < _TRIGRAM:
        row = (
            _connection()
            .execute(
                "SELECT match_count, display_name, has_geom FROM short_substrings"
                " WHERE sub = ?",
                (needle,),
            )
            .fetchone()
        )
        if row is None or row[0] != 1 or not row[2]:
            return None
        return row[1]

    phrase = '"' + needle.replace('"', '""') + '"'
    rows = (
        _connection()
        .execute(
            "SELECT display_name, has_geom FROM locations WHERE locations MATCH ?"
            " LIMIT 2",
            (phrase,),
        )
        .fetchall()
    )
    if len(rows) != 1 or not rows[0][1]:
        return None
    return rows[0][0]
