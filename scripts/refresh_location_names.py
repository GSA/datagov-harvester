"""Regenerate dcatus_validation/reference_data/location_names.csv, the place-name
data the public validator uses instead of the harvester's locations table
(GSA/data.gov#6380).

Input is the same locations.csv that database/load_locations.sh loads into the
locations table (see database/README.md for where it lives).

The harvester resolves a name only when exactly one locations row matches
`display_name ILIKE '%<name>%'`. US postal-code rows are 95% of the table, but
every postal-code name has at least two rows, so a postal code never resolves;
it only makes other names ambiguous. The output therefore keeps every other row
(counties, states, countries) with its row count, plus a flag for names that
also appear inside a postal-code name, which the validator treats as ambiguous.

Usage:
    poetry run python scripts/refresh_location_names.py path/to/locations.csv
"""

import csv
import sys
from collections import Counter
from pathlib import Path

OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "dcatus_validation"
    / "reference_data"
    / "location_names.csv"
)
POSTAL_CODE_TYPE = "us_postalcode"


def build_rows(csv_path: Path) -> list[list]:
    csv.field_size_limit(sys.maxsize)
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows or not {"display_name", "type"} <= rows[0].keys():
        raise SystemExit(f"{csv_path} needs display_name and type columns")
    # A matched row with no geometry resolves to nothing in the harvester; this
    # file can't express that, so refuse rather than drift from it.
    missing = [row["display_name"] for row in rows if not row.get("the_geom")]
    if missing:
        raise SystemExit(f"{len(missing)} rows have no geometry, e.g. {missing[0]!r}")

    postal_counts = Counter(
        row["display_name"] for row in rows if row["type"] == POSTAL_CODE_TYPE
    )
    single = [name for name, count in postal_counts.items() if count < 2]
    if single:
        # Dropping postal codes is only exact while none can resolve alone.
        raise SystemExit(f"{len(single)} postal-code names have one row: {single[0]!r}")
    postal_names = "\n".join(name.lower() for name in postal_counts)

    kept = Counter(
        row["display_name"] for row in rows if row["type"] != POSTAL_CODE_TYPE
    )
    return [
        [name, count, int(name.lower() in postal_names)]
        for name, count in sorted(kept.items())
    ]


def write_location_names(rows: list[list], path: Path) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["display_name", "row_count", "in_postal_code_name"])
        writer.writerows(rows)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    rows = build_rows(Path(sys.argv[1]))
    write_location_names(rows, OUTPUT_PATH)
    print(f"Wrote {len(rows)} names to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
