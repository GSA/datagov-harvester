"""Prepare 2026 Census county polygons for Data.gov issue #5111."""

import argparse
import os
import csv
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

import shapefile
from pyproj import Transformer
from shapely import wkb, transform
from shapely.geometry import shape

COLUMNS = [
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
]

csv.field_size_limit(2147483647)


def normalize(value):
    return " ".join(value.strip().casefold().split())


def extract_shapefile(zip_path, directory):
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            if Path(member).suffix.lower() in {
                ".shp", ".shx", ".dbf", ".prj"
            }:
                if Path(member).name != member:
                    raise ValueError("Unexpected ZIP structure")
                archive.extract(member, directory)

    files = list(directory.glob("*.shp"))
    if len(files) != 1:
        raise ValueError("Expected exactly one shapefile")

    return files[0]


def load_states(state_zip):
    with tempfile.TemporaryDirectory() as tmp:
        shp = extract_shapefile(state_zip, Path(tmp))
        states = {}

        with shapefile.Reader(str(shp)) as reader:
            for record in reader.iterRecords():
                data = record.as_dict()
                states[normalize(data["NAME"])] = data["STATEFP"]
                states[normalize(data["STUSPS"])] = data["STATEFP"]

        return states


def prepare(existing_csv, state_zip, county_zip, output_csv):
    states = load_states(state_zip)

    with existing_csv.open(
        encoding="utf-8-sig", newline=""
    ) as file:
        existing = [
            row for row in csv.DictReader(file)
            if row["type"] == "us_county"
        ]

    lookup = {}
    unresolved_states = 0

    for row in existing:
        county, state = row["name"].rsplit(",", 1)
        statefp = states.get(normalize(state))

        if statefp is None:
            unresolved_states += 1
            continue

        key = (statefp, normalize(county))
        lookup.setdefault(key, []).append(row)

    transformer = Transformer.from_crs(
        "EPSG:4269", "EPSG:4326", always_xy=True
    )

    matches = {}
    duplicates = set()
    seen = set()
    geometry_types = Counter()

    with tempfile.TemporaryDirectory() as tmp:
        shp = extract_shapefile(county_zip, Path(tmp))

        with shapefile.Reader(str(shp)) as reader:
            for record in reader.iterShapeRecords():
                data = record.record.as_dict()

                key = (
                    data["STATEFP"],
                    normalize(data["NAMELSAD"]),
                )

                if key not in lookup:
                    continue

                if key in seen:
                    duplicates.add(key)
                    continue
                seen.add(key)

                if len(lookup[key]) != 1:
                    duplicates.add(key)
                    continue

                geom = shape(record.shape.__geo_interface__)
                geom = transform(geom, transformer.transform, interleaved=False)

                if (
                    geom.is_empty
                    or not geom.is_valid
                    or geom.geom_type not in ("Polygon", "MultiPolygon")
                ):
                    continue

                row = lookup[key][0].copy()
                if (len(matches) + 1) % 100 == 0:
                    print(
                        f"Converted {len(matches) + 1} counties",
                        flush=True,
                    )

                row["the_geom"] = wkb.dumps(
                    geom, hex=True, srid=4326
                )

                matches[row["id"]] = row
                geometry_types[geom.geom_type] += 1

                limit = int(os.environ.get("MAX_COUNTIES", "0"))
                if limit and len(matches) >= limit:
                    break

    print("Existing counties:", len(existing))
    print("Matched counties:", len(matches))
    print("Unmatched counties:", len(existing) - len(matches))
    print("Unresolved states:", unresolved_states)
    print("Ambiguous/duplicate keys:", len(duplicates))
    print("Geometry types:", dict(geometry_types))

    if duplicates:
        raise ValueError("Ambiguous county matches detected")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()

        for row in existing:
            if row["id"] in matches:
                writer.writerow(matches[row["id"]])

    print("Output records:", len(matches))
    print("No database changes made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("existing_csv", type=Path)
    parser.add_argument("state_zip", type=Path)
    parser.add_argument("county_zip", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()

    prepare(
        args.existing_csv,
        args.state_zip,
        args.county_zip,
        args.output_csv,
    )
