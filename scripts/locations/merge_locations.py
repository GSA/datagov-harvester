"""Merge validated location geometry updates without changing metadata."""

import argparse
import csv
import os
import sqlite3
import tempfile
from pathlib import Path

FIELDS = [
    "name", "type", "display_name",
    "the_geom", "type_order", "id",
]

METADATA = [field for field in FIELDS if field != "the_geom"]
csv.field_size_limit(2147483647)


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != FIELDS:
            raise ValueError(f"Unexpected columns: {path}")
        yield from reader


def merge(source, updates, output):
    output.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="locations_merge_",
        dir=output.parent,
    ) as tmp:
        folder = Path(tmp)
        database = folder / "updates.sqlite"
        staged_output = folder / "combined.csv"

        conn = sqlite3.connect(database)

        try:
            conn.execute("""
                CREATE TABLE updates (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    type TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    type_order TEXT NOT NULL,
                    the_geom TEXT NOT NULL
                )
            """)

            expected = 0

            for path in updates:
                count = 0

                for row in rows(path):
                    if not row["id"] or not row["the_geom"]:
                        raise ValueError(f"Missing ID/geometry in {path}")

                    try:
                        conn.execute(
                            """
                            INSERT INTO updates
                            (id, name, type, display_name,
                             type_order, the_geom)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            tuple(row[key] for key in (
                                "id", "name", "type",
                                "display_name", "type_order",
                                "the_geom",
                            )),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise ValueError(
                            f"Duplicate replacement ID in {path}"
                        ) from exc

                    count += 1

                expected += count
                print(f"Loaded {path.name}: {count}", flush=True)

            conn.commit()

            original_count = 0
            replaced_count = 0
            preserved_count = 0
            seen_ids = set()

            with staged_output.open(
                "w", encoding="utf-8", newline=""
            ) as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()

                for row in rows(source):
                    original_count += 1
                    record_id = row["id"]

                    if record_id in seen_ids:
                        raise ValueError(
                            f"Duplicate original ID: {record_id}"
                        )
                    seen_ids.add(record_id)

                    updated = conn.execute(
                        """
                        SELECT name, type, display_name,
                               type_order, the_geom
                        FROM updates WHERE id = ?
                        """,
                        (record_id,),
                    ).fetchone()

                    if updated is None:
                        preserved_count += 1
                    else:
                        original_metadata = (
                            row["name"],
                            row["type"],
                            row["display_name"],
                            row["type_order"],
                        )

                        if original_metadata != updated[:4]:
                            raise ValueError(
                                f"Metadata differs for ID: {record_id}"
                            )

                        row["the_geom"] = updated[4]
                        replaced_count += 1

                    writer.writerow(row)

            if replaced_count != expected:
                raise ValueError(
                    f"Replacement records missing from original: "
                    f"{expected - replaced_count}"
                )

            if original_count != replaced_count + preserved_count:
                raise ValueError("Final record count mismatch")

            os.replace(staged_output, output)

            print("\nMERGE SUMMARY")
            print("Original records:", original_count)
            print("Updated geometries:", replaced_count)
            print("Preserved original geometries:", preserved_count)
            print("Final records:", original_count)
            print("Output:", output)
            print("No database changes made.")

        finally:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("updates", nargs="+", type=Path)
    args = parser.parse_args()

    merge(args.source, args.updates, args.output)
