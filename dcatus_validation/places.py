"""Place-name lookup for the public validator, matching the harvest runner's
`HarvesterDBInterface.get_geo_from_string` without the harvest database.

The harvester resolves a name with `display_name ILIKE '%<name>%'` and
`.scalar()`, so a name resolves only when exactly one locations row matches and
that row has a geometry. `reference_data/location_names.sqlite` holds every
locations row (display name and whether it has a geometry, not the geometry
itself), and this module runs the same LIKE against it.

SQLite's LIKE only folds ASCII case, so both sides are lowercased in Python to
match ILIKE. Regenerate the file with `scripts/refresh_location_names.py`.
"""

import sqlite3
import threading
from functools import lru_cache
from pathlib import Path

LOCATION_NAMES_PATH = (
    Path(__file__).resolve().parent / "reference_data" / "location_names.sqlite"
)

_local = threading.local()


def _connection() -> sqlite3.Connection:
    # One read-only connection per thread: gunicorn runs gthread workers.
    conn = getattr(_local, "conn", None)
    if conn is None or getattr(_local, "path", None) != LOCATION_NAMES_PATH:
        uri = f"{LOCATION_NAMES_PATH.as_uri()}?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        _local.conn, _local.path = conn, LOCATION_NAMES_PATH
    return conn


def load_location_names() -> None:
    """Open the file now, so a missing or broken one fails at startup."""
    _connection().execute("SELECT 1 FROM locations LIMIT 1").fetchall()


@lru_cache(maxsize=4096)
def snapshot_place_lookup(name: str) -> str | None:
    """Return the matching display name when the harvester's lookup would
    resolve `name` to a geometry, else None."""
    # Postgres LIKE escapes with backslash by default; SQLite has no default.
    rows = (
        _connection()
        .execute(
            "SELECT display_name, has_geom FROM locations"
            " WHERE name_lower LIKE ? ESCAPE '\\' LIMIT 2",
            (f"%{name}%".lower(),),
        )
        .fetchall()
    )
    if len(rows) != 1 or not rows[0][1]:
        return None
    return rows[0][0]
