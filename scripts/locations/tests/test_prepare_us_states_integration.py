"""Integration test using synthetic Census-style shapefile data."""

import csv
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import shapefile
from shapely import wkb, get_srid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prepare_us_states import prepare


class TestStateConverterIntegration(unittest.TestCase):

    def test_prepare_preserves_metadata_and_geometry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_csv = root / "locations.csv"
            source_zip = root / "states.zip"
            output_csv = root / "output.csv"

            columns = [
                "name", "type", "display_name",
                "the_geom", "type_order", "id",
            ]

            # Create 51 synthetic state records to satisfy
            # the converter's existing record-count check.
            with source_csv.open(
                "w", encoding="utf-8", newline=""
            ) as file:
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()

                for index in range(51):
                    writer.writerow({
                        "name": f"Test State {index}",
                        "type": "us_state",
                        "display_name": f"Test State {index}",
                        "the_geom": "",
                        "type_order": "2",
                        "id": str(index + 1),
                    })

            shapefile_base = root / "states"

            with shapefile.Writer(str(shapefile_base)) as writer:
                writer.field("NAME", "C")

                for index in range(51):
                    x = -120 + index

                    writer.poly([[
                        [x, 30],
                        [x + 0.5, 30],
                        [x + 0.5, 30.5],
                        [x, 30.5],
                        [x, 30],
                    ]])

                    writer.record(f"Test State {index}")

            (root / "states.prj").write_text(
                'GEOGCS["GCS_North_American_1983",'
                'DATUM["D_North_American_1983",'
                'SPHEROID["GRS_1980",6378137,298.257222101]],'
                'PRIMEM["Greenwich",0],'
                'UNIT["Degree",0.017453292519943295]]',
                encoding="utf-8",
            )

            with zipfile.ZipFile(source_zip, "w") as archive:
                for extension in (".shp", ".shx", ".dbf", ".prj"):
                    path = root / f"states{extension}"
                    archive.write(path, arcname=path.name)

            prepare(source_csv, source_zip, output_csv)

            self.assertTrue(output_csv.exists())

            with output_csv.open(
                encoding="utf-8", newline=""
            ) as file:
                rows = list(csv.DictReader(file))

            self.assertEqual(len(rows), 51)

            for index, row in enumerate(rows):
                self.assertEqual(row["id"], str(index + 1))
                self.assertEqual(row["type"], "us_state")
                self.assertEqual(row["type_order"], "2")
                self.assertEqual(
                    row["display_name"], f"Test State {index}"
                )

                geometry = wkb.loads(
                    bytes.fromhex(row["the_geom"])
                )

                self.assertTrue(geometry.is_valid)
                self.assertFalse(geometry.is_empty)
                self.assertEqual(geometry.geom_type, "Polygon")
                self.assertEqual(get_srid(geometry), 4326)


if __name__ == "__main__":
    unittest.main()
