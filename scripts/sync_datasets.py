import json
import logging
import os
import sys
from typing import List

import click
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from database.models import Dataset, HarvestRecord, db
from harvester import HarvesterDBInterface
from harvester.utils.ckan_utils import add_uuid_to_package_name, munge_title_to_name
from harvester.utils.general_utils import get_datetime

logger = logging.getLogger("sync_datasets")
BATCH_SIZE = 1000


def _load_metadata(record: HarvestRecord):
    if record.source_transform:
        return record.source_transform
    if record.source_raw:
        try:
            return json.loads(record.source_raw)
        except json.JSONDecodeError as exc:
            logger.warning("Record %s has invalid JSON: %s", record.id, exc)
    return None


def _insert_dataset_for_record(interface: HarvesterDBInterface, record: HarvestRecord):
    metadata = _load_metadata(record)
    if not metadata:
        raise click.ClickException(
            f"Record {record.id} missing metadata to build dataset payload"
        )

    slug = munge_title_to_name(metadata.get("title") or record.identifier)
    payload = {
        "slug": slug,
        "dcat": metadata,
        "organization_id": record.harvest_source.organization_id,
        "harvest_source_id": record.harvest_source_id,
        "harvest_record_id": record.id,
        "last_harvested_date": record.date_finished or get_datetime(),
    }

    while True:
        try:
            interface.insert_dataset(payload)
            return payload["slug"]
        except IntegrityError:
            payload["slug"] = add_uuid_to_package_name(payload["slug"])
        except Exception as exc:  # pragma: no cover - defensive
            raise click.ClickException(str(exc))


def _records_missing_datasets(session) -> List[HarvestRecord]:
    return (
        session.query(HarvestRecord)
        .outerjoin(Dataset, Dataset.harvest_record_id == HarvestRecord.id)
        .filter(
            HarvestRecord.status == "success",
            HarvestRecord.action.in_(["create", "update"]),
            Dataset.id.is_(None),
        )
        .all()
    )


def _datasets_missing_records(session) -> List[Dataset]:
    return (
        session.query(Dataset)
        .outerjoin(HarvestRecord, Dataset.harvest_record_id == HarvestRecord.id)
        .filter(HarvestRecord.id.is_(None))
        .all()
    )


def _datasets_with_unexpected_records(session) -> List[Dataset]:
    return (
        session.query(Dataset)
        .join(HarvestRecord, Dataset.harvest_record_id == HarvestRecord.id)
        .filter(
            or_(
                HarvestRecord.status != "success",
                HarvestRecord.action.notin_(["create", "update"]),
            )
        )
        .all()
    )


def _report(records_missing, datasets_missing, datasets_bad):
    click.echo("Dataset Sync Report\n====================")
    click.echo(f"Records needing datasets: {len(records_missing)}")
    click.echo(f"Datasets pointing to missing harvest records: {len(datasets_missing)}")
    click.echo(
        f"Datasets tied to non-success/non-create records: {len(datasets_bad)}"
    )


def _sync_impl(apply_changes: bool):
    interface = HarvesterDBInterface(session=db.session)

    try:
        records_missing = _records_missing_datasets(db.session)
        datasets_missing = _datasets_missing_records(db.session)
        datasets_bad = _datasets_with_unexpected_records(db.session)

        _report(records_missing, datasets_missing, datasets_bad)

        if apply_changes:
            synced = 0
            total = len(records_missing)
            for start in range(0, total, BATCH_SIZE):
                batch = records_missing[start : start + BATCH_SIZE]
                click.echo(
                    f"Processing batch {start // BATCH_SIZE + 1}"
                    f" ({len(batch)} records)..."
                )
                for record in batch:
                    try:
                        slug = _insert_dataset_for_record(interface, record)
                        synced += 1
                        click.echo(
                            f"Created dataset for record {record.id} (slug: {slug})"
                        )
                    except click.ClickException as exc:
                        click.echo(f"Failed to sync record {record.id}: {exc}")
            click.echo(f"Datasets created: {synced}")
    finally:
        db.session.remove()


def register_cli(app):
    @app.cli.group("dataset")
    def dataset_group():
        """Dataset consistency commands."""

    @dataset_group.command("check")
    @click.option(
        "--apply",
        "apply_changes",
        is_flag=True,
        help="Create datasets for missing harvest records",
    )
    def dataset_check(apply_changes):
        """Report (and optionally repair) dataset mismatches."""
        _sync_impl(apply_changes)


if __name__ == "__main__":
    from app import create_app

    app = create_app()
    with app.app_context():
        _sync_impl(False)
