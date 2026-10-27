"""Prepare Natural Earth country polygons for Data.gov #5111."""

import argparse
import csv
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely import wkb, transform
from shapely.geometry import shape

COLUMNS = [
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
]
csv.field_size_limit(2147483647)


def normalize(value):
    return " ".join(str(value or "").casefold().split())


def prepare(existing_csv, country_zip, output_csv):
    with existing_csv.open(
        encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.DictReader(file)
        if not set(COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("Missing required CSV columns")
        existing = [
            row for row in reader if row["type"] == "country"
        ]

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)

        with zipfile.ZipFile(country_zip) as archive:
            for extension in (".shp", ".shx", ".dbf", ".prj"):
                members = [
                    m for m in archive.namelist()
                    if m.lower().endswith(extension)
                ]
                if len(members) != 1:
                    raise ValueError(f"Expected one {extension}")
                member = members[0]
                if Path(member).name != member:
                    raise ValueError("Unexpected archive structure")
                archive.extract(member, folder)

        prj = next(folder.glob("*.prj"))
        source_crs = CRS.from_wkt(
            prj.read_text(encoding="utf-8")
        )
        target_crs = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(
            source_crs, target_crs, always_xy=True
        )

        shp = next(folder.glob("*.shp"))
        records = []
        aliases = {}

        with shapefile.Reader(str(shp)) as reader:
            for item in reader.iterShapeRecords():
                data = item.record.as_dict()
                records.append(item)

                for field in ("ADMIN", "NAME", "NAME_EN"):
                    alias = normalize(data.get(field))
                    if alias:
                        aliases.setdefault(alias, set()).add(
                            len(records) - 1
                        )

        matched = {}
        ambiguous = 0
        used_sources = set()
        geometry_types = Counter()

        for row in existing:
            candidates = aliases.get(normalize(row["name"]), set())

            if len(candidates) != 1:
                if len(candidates) > 1:
                    ambiguous += 1
                continue

            index = next(iter(candidates))

            if index in used_sources:
                ambiguous += 1
                continue

            geom = shape(records[index].shape.__geo_interface__)
            geom = transform(
                geom, transformer.transform, interleaved=False
            )

            if (
                geom.is_empty
                or not geom.is_valid
                or geom.geom_type not in ("Polygon", "MultiPolygon")
            ):
                continue

            updated = row.copy()
            updated["the_geom"] = wkb.dumps(
                geom, hex=True, srid=4326
            )

            matched[row["id"]] = updated
            used_sources.add(index)
            geometry_types[geom.geom_type] += 1

"""Prepare Natural Earth country polygons for Data.gov #5111."""

import argparse
import csv
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely import wkb, transform
from shapely.geometry import shape

COLUMNS = [
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
]
csv.field_size_limit(2147483647)


def normalize(value):
    return " ".join(str(value or "").casefold().split())


def prepare(existing_csv, country_zip, output_csv):
    with existing_csv.open(
        encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.DictReader(file)
        if not set(COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("Missing required CSV columns")
        existing = [
            row for row in reader if row["type"] == "country"
        ]

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)

        with zipfile.ZipFile(country_zip) as archive:
            for extension in (".shp", ".shx", ".dbf", ".prj"):
                members = [
                    m for m in archive.namelist()
                    if m.lower().endswith(extension)
                ]
                if len(members) != 1:
                    raise ValueError(f"Expected one {extension}")
                member = members[0]
                if Path(member).name != member:
                    raise ValueError("Unexpected archive structure")
                archive.extract(member, folder)

        prj = next(folder.glob("*.prj"))
        source_crs = CRS.from_wkt(
            prj.read_text(encoding="utf-8")
        )
        target_crs = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(
            source_crs, target_crs, always_xy=True
        )

        shp = next(folder.glob("*.shp"))
        records = []
        aliases = {}

        with shapefile.Reader(str(shp)) as reader:
            for item in reader.iterShapeRecords():
                data = item.record.as_dict()
                records.append(item)

                for field in ("ADMIN", "NAME", "NAME_EN"):
                    alias = normalize(data.get(field))
                    if alias:
                        aliases.setdefault(alias, set()).add(
                            len(records) - 1
                        )

        matched = {}
        ambiguous = 0
        used_sources = set()
        geometry_types = Counter()

        for row in existing:
            candidates = aliases.get(normalize(row["name"]), set())

            if len(candidates) != 1:
                if len(candidates) > 1:
                    ambiguous += 1
                continue

            index = next(iter(candidates))

            if index in used_sources:
                ambiguous += 1
                continue

            geom = shape(records[index].shape.__geo_interface__)
            geom = transform(
                geom, transformer.transform, interleaved=False
            )

            if (
                geom.is_empty
                or not geom.is_valid
                or geom.geom_type not in ("Polygon", "MultiPolygon")
            ):
                continue

            updated = row.copy()
            updated["the_geom"] = wkb.dumps(
                geom, hex=True, srid=4326
            )

            matched[row["id"]] = updated
            used_sources.add(index)
            geometry_types[geom.geom_type] += 1

    print("Existing countries:", len(existing))
    print("Matched countries:", len(matched))
    print("Unmatched countries:", len(existing) - len(matched))
    print("Ambiguous matches:", ambiguous)
    print("Geometry types:", dict(geometry_types))

    # Do not silently accept ambiguous country assignments.
    if ambiguous:
        print("Review ambiguous matches before using this output.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(
            matched[row["id"]]
            for row in existing if row["id"] in matched
        )

    print("Output records:", len(matched))
    print("No database changes made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("existing_csv", type=Path)
    parser.add_argument("country_zip", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()

    prepare(args.existing_csv, args.country_zip, args.output_csv)
