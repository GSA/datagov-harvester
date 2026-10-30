"""Prepare Census 2025 ZCTA polygons for GSA #5111."""

import argparse
import csv
import re
import tempfile
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from shapely import transform, wkb
from shapely.geometry import shape

FIELDS = ["name", "type", "display_name", "the_geom", "type_order", "id"]
csv.field_size_limit(2147483647)


def prepare(existing_csv, zcta_zip, output_csv):
    # Keep only metadata here. Original geometries can be very large.
    by_zip = defaultdict(list)

    with existing_csv.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row["type"] != "us_postalcode":
                continue

            name = row["name"].strip()
            if re.fullmatch(r"\d{5}", name):
                code = name
            else:
                match = re.search(
                    r"\((\d{5})\)\s*$",
                    row["display_name"].strip()
                )
                if not match:
                    raise ValueError("Unexpected postal-code format")
                code = match.group(1)

            metadata = {field: row[field] for field in FIELDS}
            metadata["the_geom"] = ""
            by_zip[code].append(metadata)

    if any(len(rows) != 2 for rows in by_zip.values()):
        raise ValueError("Expected exactly two records per ZIP")

    print("Existing ZIP codes:", len(by_zip), flush=True)
    print(
        "Existing postal records:",
        sum(len(rows) for rows in by_zip.values()),
        flush=True,
    )

    matched_codes = set()
    geometry_types = Counter()
    invalid_geometries = 0
    duplicate_zctas = set()
    converted_records = 0

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)

        with zipfile.ZipFile(zcta_zip) as archive:
            for extension in (".shp", ".shx", ".dbf", ".prj"):
                members = [
                    n for n in archive.namelist()
                    if n.lower().endswith(extension)
                ]
                if len(members) != 1:
                    raise ValueError(f"Expected one {extension} file")
                member = members[0]
                if Path(member).name != member:
                    raise ValueError("Unexpected archive structure")
                archive.extract(member, folder)

        shp = next(folder.glob("*.shp"))
        prj = next(folder.glob("*.prj"))

        source_crs = CRS.from_wkt(prj.read_text(encoding="utf-8"))
        transformer = Transformer.from_crs(
            source_crs, "EPSG:4326", always_xy=True
        )

        with shapefile.Reader(str(shp)) as reader:
            with output_csv.open(
                "w", encoding="utf-8", newline=""
            ) as output:
                field_names = [f[0] for f in reader.fields[1:]]
                if "ZCTA5CE20" not in field_names:
                    raise ValueError(
                        f"ZCTA5CE20 field missing: {field_names}"
                    )

                writer = csv.DictWriter(output, fieldnames=FIELDS)
                writer.writeheader()

                for item in reader.iterShapeRecords():
                    data = item.record.as_dict()
                    code = str(data["ZCTA5CE20"]).zfill(5)

                    if code not in by_zip:
                        continue

                    if code in matched_codes:
                        duplicate_zctas.add(code)
                        continue

                    geom = shape(item.shape.__geo_interface__)
                    geom = transform(
                        geom, transformer.transform, interleaved=False
                    )

                    if (
                        geom.is_empty
                        or not geom.is_valid
                        or geom.geom_type not in ("Polygon", "MultiPolygon")
                    ):
                        invalid_geometries += 1
                        continue

                    geometry_hex = wkb.dumps(
                        geom, hex=True, srid=4326
                    )

                    for original in by_zip[code]:
                        updated = original.copy()
                        updated["the_geom"] = geometry_hex
                        writer.writerow(updated)
                        converted_records += 1

                    matched_codes.add(code)
                    geometry_types[geom.geom_type] += 1

                    if len(matched_codes) % 1000 == 0:
                        print(
                            f"Converted {len(matched_codes)} ZIP codes",
                            flush=True,
                        )

    print("Matched ZIP codes:", len(matched_codes))
    print("Unmatched ZIP codes:", len(by_zip) - len(matched_codes))
    print("Output records:", converted_records)
    print("Invalid candidate geometries:", invalid_geometries)
    print("Duplicate Census ZCTA codes:", len(duplicate_zctas))
    print("Geometry types:", dict(geometry_types))
    print("No database changes made.")

    if duplicate_zctas:
        raise ValueError("Duplicate ZCTA codes require review")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("existing_csv", type=Path)
    parser.add_argument("zcta_zip", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()

    prepare(args.existing_csv, args.zcta_zip, args.output_csv)
