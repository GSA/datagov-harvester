"""Regenerate dcatus_validation/reference_data/location_names.sqlite, the
place-name data the public validator uses instead of the harvester's locations
table (GSA/data.gov#6380).

Reads every locations row from a harvest database. To pull from cloud.gov, see
database/export_validator_locations.sh.

Usage:
    poetry run python scripts/refresh_location_names.py postgresql+psycopg://user:pass@host:port/db
"""

import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from sqlalchemy import Connection, create_engine, text

OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "dcatus_validation"
    / "reference_data"
    / "location_names.sqlite"
)

# The trigram index can't answer needles shorter than this.
TRIGRAM = 3


def fetch_rows(conn: Connection) -> list[tuple[str, bool]]:
    rows = conn.execute(
        text(
            "SELECT display_name, the_geom IS NOT NULL FROM locations"
            " WHERE display_name IS NOT NULL ORDER BY display_name"
        )
    ).all()
    return [(name, has_geom) for name, has_geom in rows]


def _short_substrings(rows: list[tuple[str, bool]]) -> list[tuple]:
    """Every substring under TRIGRAM characters, with how many rows contain it
    (capped at 2) and, when exactly one does, that row."""
    found = defaultdict(list)
    for index, (name, _) in enumerate(rows):
        lowered = name.lower()
        subs = {
            lowered[i : i + size]
            for size in range(1, TRIGRAM)
            for i in range(len(lowered) - size + 1)
        }
        for sub in subs:
            if len(found[sub]) < 2:
                found[sub].append(index)
    return [
        (sub, len(hits), *(rows[hits[0]] if len(hits) == 1 else (None, None)))
        for sub, hits in sorted(found.items())
    ]


def write_location_names(rows: list[tuple[str, bool]], path: Path) -> None:
    if not rows:
        raise SystemExit("locations table is empty; refusing to write")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    conn = sqlite3.connect(tmp)
    conn.execute(
        "CREATE VIRTUAL TABLE locations USING fts5("
        "name_lower, display_name UNINDEXED, has_geom UNINDEXED,"
        " tokenize='trigram')"
    )
    conn.executemany(
        "INSERT INTO locations VALUES (?, ?, ?)",
        [(name.lower(), name, int(has_geom)) for name, has_geom in rows],
    )
    conn.execute(
        "CREATE TABLE short_substrings (sub TEXT PRIMARY KEY, match_count INTEGER,"
        " display_name TEXT, has_geom INTEGER) WITHOUT ROWID"
    )
    conn.executemany(
        "INSERT INTO short_substrings VALUES (?, ?, ?, ?)", _short_substrings(rows)
    )
    conn.execute("CREATE TABLE meta (max_name_chars INTEGER)")
    conn.execute(
        "INSERT INTO meta VALUES (?)", (max(len(name.lower()) for name, _ in rows),)
    )
    conn.execute("INSERT INTO locations(locations) VALUES ('optimize')")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    tmp.replace(path)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    engine = create_engine(sys.argv[1])
    with engine.connect() as conn:
        rows = fetch_rows(conn)
    write_location_names(rows, OUTPUT_PATH)
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
