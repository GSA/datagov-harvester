"""Tests for the Census state boundary conversion."""

import sys
import unittest
from pathlib import Path

from shapely import wkb
from shapely.geometry import Polygon

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prepare_us_states import normalize, load_states


class TestPrepareUSStates(unittest.TestCase):

    def test_normalize_name(self):
        self.assertEqual(normalize("  New   York "), "new york")

    def test_ewkb_round_trip(self):
        geometry = Polygon([
            (-100, 30),
            (-99, 30),
            (-99, 31),
            (-100, 31),
        ])

        encoded = wkb.dumps(geometry, hex=True, srid=4326)
        decoded = wkb.loads(bytes.fromhex(encoded))

        self.assertTrue(decoded.equals(geometry))
        self.assertTrue(decoded.is_valid)

    def test_load_states_filters_location_types(self):
        import csv
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "locations.csv"

            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "name", "type", "display_name",
                        "the_geom", "type_order", "id",
                    ],
                )
                writer.writeheader()
                writer.writerow({
                    "name": "Texas",
                    "type": "us_state",
                    "display_name": "Texas",
                    "the_geom": "sample",
                    "type_order": "1",
                    "id": "1",
                })
                writer.writerow({
                    "name": "United States",
                    "type": "country",
                    "display_name": "United States",
                    "the_geom": "sample",
                    "type_order": "0",
                    "id": "2",
                })

            states = load_states(csv_path)

            self.assertEqual(len(states), 1)
            self.assertEqual(states[0]["name"], "Texas")


if __name__ == "__main__":
    unittest.main()
