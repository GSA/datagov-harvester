"""Bulk update dataset slugs for GSA/data.gov#5968."""

import argparse
import csv
import re
import sys
from pathlib import Path

# Allow the script to be executed from the scripts directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database.models import Dataset, db
from harvester import HarvesterDBInterface

REQUIRED_COLUMNS = {"dataset_id", "old_slug", "new_slug"}


def load_changes(csv_path):
    """Read and validate the input CSV."""

    changes = []
    seen_ids = set()
    seen_slugs = set()

    with open(csv_path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames or not REQUIRED_COLUMNS.issubset(reader.fieldnames):
            raise ValueError("CSV must contain dataset_id, old_slug, new_slug")

        for line_number, row in enumerate(reader, start=2):
            dataset_id = (row["dataset_id"] or "").strip()
            old_slug = (row["old_slug"] or "").strip()
            new_slug = (row["new_slug"] or "").strip()

            if not all([dataset_id, old_slug, new_slug]):
                raise ValueError(f"Line {line_number}: Missing required value")

            if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", new_slug):
                raise ValueError(f"Line {line_number}: Invalid slug: {new_slug}")

            if dataset_id in seen_ids:
                raise ValueError(f"Line {line_number}: Duplicate dataset ID")

            if new_slug in seen_slugs:
                raise ValueError(f"Line {line_number}: Duplicate target slug")

            seen_ids.add(dataset_id)
            seen_slugs.add(new_slug)

            changes.append(
                {
                    "dataset_id": dataset_id,
                    "old_slug": old_slug,
                    "new_slug": new_slug,
                }
            )

    return changes


def bulk_update(csv_path, apply=False):
    """Validate and optionally apply dataset slug changes."""

    changes = load_changes(csv_path)

    if not changes:
        print("No dataset changes found.")
        return 0

    interface = HarvesterDBInterface(session=db.session)

    errors = []
    ready = []

    print(f"\nDatasets requested: {len(changes)}")
    print("Mode:", "APPLY" if apply else "DRY RUN")
    print("-" * 60)

    # Validate every row before making any changes.
    for change in changes:
        dataset_id = change["dataset_id"]
        old_slug = change["old_slug"]
        new_slug = change["new_slug"]

        dataset = db.session.get(Dataset, dataset_id)

        if dataset is None:
            errors.append(f"{dataset_id}: Dataset not found")
            continue

        if dataset.slug != old_slug:
            errors.append(
                f"{dataset_id}: Expected '{old_slug}', " f"found '{dataset.slug}'"
            )
            continue

        if old_slug == new_slug:
            print(f"SKIP: {old_slug} is already correct")
            continue

        existing = interface.get_dataset_by_slug(new_slug)

        if existing is not None:
            errors.append(f"{new_slug}: Slug already exists")
            continue

        ready.append(change)

        print(f"READY: {dataset_id}\n" f"       {old_slug} -> {new_slug}")

    if errors:
        print("\nValidation failed. No changes applied.")

        for error in errors:
            print(f"ERROR: {error}")

        return 1

    print(f"\nReady to update: {len(ready)}")

    if not apply:
        print("Dry run complete. No changes applied.")
        return 0

    successful = 0
    failed = 0
    total = len(ready)

    for index, change in enumerate(ready, start=1):
        dataset_id = change["dataset_id"]
        new_slug = change["new_slug"]

        print(f"[{index}/{total}] Updating {dataset_id} -> {new_slug}")

        dataset, os_synced, error = interface.update_dataset_slug(
            dataset_id,
            new_slug,
        )

        if dataset is None:
            failed += 1
            print(f"FAILED: {dataset_id}: {error}")

        elif not os_synced:
            failed += 1
            print(
                f"PARTIAL SUCCESS: {dataset_id}\n"
                f"Database updated to: {new_slug}\n"
                f"OpenSearch error: {error}"
            )

        else:
            successful += 1
            print(f"SUCCESS: {dataset_id} -> {new_slug}")

    print("\nBulk update summary")
    print("-" * 60)
    print(f"Successful: {successful}")
    print(f"Failed or partially synced: {failed}")

    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description="Bulk update Data.gov dataset slugs")

    parser.add_argument(
        "--file",
        required=True,
        help="Path to CSV containing slug changes",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes; otherwise perform a dry run",
    )

    args = parser.parse_args()

    # Use the existing Harvester application configuration.
    from app import create_app

    app = create_app()

    with app.app_context():
        try:
            return bulk_update(args.file, args.apply)
        except ValueError as error:
            print(f"ERROR: {error}")
            return 1
        finally:
            db.session.remove()


if __name__ == "__main__":
    sys.exit(main())
