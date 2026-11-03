"""Regenerate dcatus_validation/reference_data/location_names.json, the
place-name snapshot the public validator uses instead of the harvester's
locations table (GSA/data.gov#6380).

Input is the same locations.csv that database/load_locations.sh loads into the
locations table (see database/README.md for where it lives). Only display
names are kept, one entry per row, so a name shared by several rows still
counts as several matches, exactly as the harvester's lookup sees it.

Usage:
    poetry run python scripts/refresh_location_names.py path/to/locations.csv
"""

import csv
import json
import sys
from pathlib import Path

OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "dcatus_validation"
    / "reference_data"
    / "location_names.json"
)


def build_snapshot(csv_path: Path) -> dict:
    csv.field_size_limit(sys.maxsize)
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows or "display_name" not in rows[0]:
        raise SystemExit(f"{csv_path} has no display_name column")
    # A matched row with no geometry resolves to nothing in the harvester; the
    # snapshot can't express that, so refuse rather than drift from it.
    missing = [row["display_name"] for row in rows if not row.get("the_geom")]
    if missing:
        raise SystemExit(f"{len(missing)} rows have no geometry, e.g. {missing[0]!r}")
    return {
        "source": "locations table display_name, one entry per row",
        "values": sorted(row["display_name"] for row in rows),
    }


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    snapshot = build_snapshot(Path(sys.argv[1]))
    with open(OUTPUT_PATH, "w") as f:
        json.dump(snapshot, f, indent=0, ensure_ascii=False)
        f.write("\n")
    print(f"Wrote {len(snapshot['values'])} names to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
