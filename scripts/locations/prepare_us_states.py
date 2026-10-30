"""Prepare updated Census state polygons for Data.gov issue #5111."""

import argparse
import csv
import tempfile
import zipfile
from pathlib import Path

import shapefile
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform
from shapely import wkb

COLUMNS = [
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
]


def normalize(value):
    return " ".join(value.casefold().split())


def load_states(csv_path):
    states = []

    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not set(COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("Existing CSV is missing required columns")

        for row in reader:
            if row["type"] == "us_state":
                states.append(row)

    return states


def load_census(zip_path):
    census = {}
    transformer = Transformer.from_crs(
        "EPSG:4269", "EPSG:4326", always_xy=True
    )

    with tempfile.TemporaryDirectory() as directory:
        with zipfile.ZipFile(zip_path) as archive:
            members = [
                m for m in archive.namelist()
                if Path(m).suffix.lower()
                in {".shp", ".shx", ".dbf", ".prj"}
            ]

            for member in members:
                if Path(member).name != member:
                    raise ValueError("Unexpected ZIP directory structure")

            archive.extractall(directory, members=members)

        shp_files = list(Path(directory).glob("*.shp"))

        if len(shp_files) != 1:
            raise ValueError("Expected one shapefile")

        with shapefile.Reader(str(shp_files[0])) as reader:
            for record in reader.iterShapeRecords():
                values = record.record.as_dict()
                name = values["NAME"]

                geometry = shape(record.shape.__geo_interface__)
                geometry = transform(transformer.transform, geometry)

                if geometry.is_empty or not geometry.is_valid:
                    raise ValueError("Invalid Census geometry")

                if geometry.geom_type not in ("Polygon", "MultiPolygon"):
                    raise ValueError("Unexpected geometry type")

                census[normalize(name)] = geometry

    return census


def prepare(existing_path, zip_path, output_path):
    existing = load_states(existing_path)
    census = load_census(zip_path)

    matches = []
    unmatched = 0

    for row in existing:
        key = normalize(row["name"])

        if key not in census:
            unmatched += 1
            continue

        geometry = census[key]

        updated = row.copy()
        updated["the_geom"] = wkb.dumps(
            geometry, hex=True, srid=4326
        )

        matches.append(updated)

    print("Existing state records:", len(existing))
    print("Matched state records:", len(matches))
    print("Unmatched state records:", unmatched)

    if unmatched or len(existing) != 51:
        print("No output written: review matching first.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(matches)

    print("Output records:", len(matches))
    print("Geometry format: hexadecimal EWKB (SRID 4326)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("existing_csv", type=Path)
    parser.add_argument("census_zip", type=Path)
    parser.add_argument("output_csv", type=Path)

    args = parser.parse_args()

    prepare(
        args.existing_csv,
        args.census_zip,
        args.output_csv,
    )
