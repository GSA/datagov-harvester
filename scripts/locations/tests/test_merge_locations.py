import csv
import tempfile
import unittest
from pathlib import Path

from merge_locations import merge, FIELDS


class TestMergeLocations(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)

    def write_csv(self, filename, rows):
        path = self.folder / filename
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_merge_preserves_metadata_and_unmatched_records(self):
        original_rows = [
            {
                "name": "Place A", "type": "country",
                "display_name": "Place A", "the_geom": "OLD_A",
                "type_order": "1", "id": "10",
            },
            {
                "name": "Place B", "type": "country",
                "display_name": "Place B", "the_geom": "OLD_B",
                "type_order": "1", "id": "11",
            },
        ]

        updated_rows = [
            {**original_rows[0], "the_geom": "NEW_A"}
        ]

        source = self.write_csv("source.csv", original_rows)
        updates = self.write_csv("updates.csv", updated_rows)
        output = self.folder / "merged.csv"

        merge(source, [updates], output)

        with output.open(newline="", encoding="utf-8") as f:
            result = list(csv.DictReader(f))

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["the_geom"], "NEW_A")
        self.assertEqual(result[1], original_rows[1])

    def test_merge_rejects_changed_metadata(self):
        original = {
            "name": "Place A", "type": "country",
            "display_name": "Place A", "the_geom": "OLD_A",
            "type_order": "1", "id": "10",
        }

        source = self.write_csv("source.csv", [original])
        update = self.write_csv(
            "updates.csv",
            [{**original, "name": "Different Name"}],
        )

        with self.assertRaises(ValueError):
            merge(source, [update], self.folder / "merged.csv")

    def test_merge_rejects_duplicate_ids(self):
        original = {
            "name": "Place A", "type": "country",
            "display_name": "Place A", "the_geom": "OLD_A",
            "type_order": "1", "id": "10",
        }

        source = self.write_csv("source.csv", [original])
        update = self.write_csv(
            "updates.csv", [original, original]
        )

        with self.assertRaises(ValueError):
            merge(source, [update], self.folder / "merged.csv")


if __name__ == "__main__":
    unittest.main()
