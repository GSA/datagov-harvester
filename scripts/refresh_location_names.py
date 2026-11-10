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
from pathlib import Path

from sqlalchemy import Connection, create_engine, text

OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "dcatus_validation"
    / "reference_data"
    / "location_names.sqlite"
)


def fetch_rows(conn: Connection) -> list[tuple[str, bool]]:
    rows = conn.execute(
        text(
            "SELECT display_name, the_geom IS NOT NULL FROM locations"
            " WHERE display_name IS NOT NULL ORDER BY display_name"
        )
    ).all()
    return [(name, has_geom) for name, has_geom in rows]


def write_location_names(rows: list[tuple[str, bool]], path: Path) -> None:
    if not rows:
        raise SystemExit("locations table is empty; refusing to write")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    conn = sqlite3.connect(tmp)
    conn.execute(
        "CREATE TABLE locations (display_name TEXT NOT NULL,"
        " name_lower TEXT NOT NULL, has_geom INTEGER NOT NULL)"
    )
    conn.executemany(
        "INSERT INTO locations VALUES (?, ?, ?)",
        [(name, name.lower(), int(has_geom)) for name, has_geom in rows],
    )
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
