"""Transactional PostGIS locations geometry importer for GSA #5111."""

import argparse
import csv
import os
from pathlib import Path

csv.field_size_limit(2147483647)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    parser.add_argument("--table", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.csv_file.is_file():
        parser.error("Input CSV does not exist")

    parts = args.table.split(".")
    if len(parts) != 2 or not all(
        part.replace("_", "").isalnum()
        and not part[0].isdigit()
        for part in parts
    ):
        parser.error("Use --table schema.table")

    url = os.environ.get("LOCATIONS_DATABASE_URL")
    if not url:
        parser.error("Set LOCATIONS_DATABASE_URL first")

    import psycopg
    from psycopg import sql

    table = sql.Identifier(*parts)

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT PostGIS_Version()")
            print("PostGIS:", cur.fetchone()[0])

            cur.execute(
                """
                CREATE TEMP TABLE locations_stage (
                    id TEXT PRIMARY KEY,
                    the_geom_hex TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )

            with args.csv_file.open(
                encoding="utf-8-sig", newline=""
            ) as file:
                reader = csv.DictReader(file)

                with cur.copy(
                    "COPY locations_stage (id, the_geom_hex) FROM STDIN"
                ) as copy:
                    count = 0
                    for row in reader:
                        copy.write_row(
                            (row["id"], row["the_geom"])
                        )
                        count += 1

            print("Staged records:", count)

            if count != 63730:
                raise ValueError("Unexpected number of staged records")

            # Lock the table for a consistent count and update.
            cur.execute(
                sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE")
                .format(table)
            )

            cur.execute(
                sql.SQL("""
                    SELECT count(*)
                    FROM locations_stage s
                    JOIN {} t ON t.id::text = s.id
                """).format(table)
            )
            matched = cur.fetchone()[0]
            print("Matching database IDs:", matched)

            if matched != count:
                raise ValueError(
                    "Destination does not contain every staged ID"
                )

            cur.execute("""
                SELECT count(*)
                FROM locations_stage
                WHERE NOT (
                    the_geom_hex ~ '^[0-9A-Fa-f]+$'
                    AND length(the_geom_hex) % 2 = 0
                )
            """)
            if cur.fetchone()[0]:
                raise ValueError("Invalid hexadecimal geometry")

            cur.execute("""
                SELECT count(*)
                FROM locations_stage
                WHERE ST_SRID(
                    ST_GeomFromEWKB(decode(the_geom_hex, 'hex'))
                ) NOT IN (0, 4326)
            """)
            if cur.fetchone()[0]:
                raise ValueError("Unexpected geometry SRID")

            if args.apply:
                cur.execute(
                    sql.SQL("""
                        UPDATE {} AS t
                        SET the_geom = ST_GeomFromEWKB(
                            decode(s.the_geom_hex, 'hex')
                        )
                        FROM locations_stage s
                        WHERE t.id::text = s.id
                          AND t.the_geom IS DISTINCT FROM
                              ST_GeomFromEWKB(
                                  decode(s.the_geom_hex, 'hex')
                              )
                    """).format(table)
                )
                print("Updated database rows:", cur.rowcount)
                conn.commit()
                print("Transaction committed")
            else:
                conn.rollback()
                print("DRY RUN PASSED — transaction rolled back")


if __name__ == "__main__":
    main()
