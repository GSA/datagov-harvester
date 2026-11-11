"""Validate geometry in a locations CSV without changing its contents."""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

from shapely import wkb

csv.field_size_limit(sys.maxsize if sys.maxsize <= 2147483647 else 2147483647)

REQUIRED_COLUMNS = {
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
}


def validate(csv_path):
    total = 0
    valid = 0
    invalid = 0
    geometry_types = Counter()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        for row in reader:
            total += 1

            try:
                geom_hex = (row["the_geom"] or "").strip()

                if geom_hex.startswith("\\x") or geom_hex.startswith("0x"):
                    geom_hex = geom_hex[2:]

                geometry = wkb.loads(bytes.fromhex(geom_hex))

                if geometry.is_empty or not geometry.is_valid:
                    raise ValueError("Empty or invalid geometry")

                geometry_types[geometry.geom_type] += 1
                valid += 1

            except (ValueError, TypeError):
                invalid += 1

    print(f"Total records: {total}")
    print(f"Valid geometries: {valid}")
    print(f"Invalid geometries: {invalid}")
    print("Geometry types:")

    for geom_type, count in sorted(geometry_types.items()):
        print(f"  {geom_type}: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    args = parser.parse_args()

    validate(args.csv_file)
