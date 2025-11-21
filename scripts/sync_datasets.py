import json
import logging
import os
import sys
from typing import Iterable, Optional

import click
from sqlalchemy import String, cast, or_
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from database.models import Dataset, HarvestRecord, HarvestSource, db
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

    harvest_source = getattr(record, "harvest_source", None)
    if harvest_source is None:
        harvest_source = interface.db.get(HarvestSource, record.harvest_source_id)
    if harvest_source is None:
        raise click.ClickException(
            f"Record {record.id} is missing an associated harvest source"
        )

    slug = munge_title_to_name(metadata.get("title") or record.identifier)
    payload = {
        "slug": slug,
        "dcat": metadata,
        "organization_id": harvest_source.organization_id,
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


def _latest_successful_records(session):
    subquery = (
        session.query(HarvestRecord)
        .filter(HarvestRecord.status == "success")
        .order_by(
            HarvestRecord.identifier,
            HarvestRecord.harvest_source_id,
            HarvestRecord.date_created.desc(),
        )
        .distinct(HarvestRecord.identifier, HarvestRecord.harvest_source_id)
        .subquery()
    )
    return aliased(HarvestRecord, subquery)


def _records_missing_datasets(
    session, exclude_ids: Optional[Iterable[int]] = None
):
    LatestRecord = _latest_successful_records(session)
    harvest_source_alias = aliased(HarvestSource)

    query = (
        session.query(LatestRecord)
        .join(
            harvest_source_alias,
            LatestRecord.harvest_source_id == harvest_source_alias.id,
        )
        .outerjoin(Dataset, Dataset.harvest_record_id == LatestRecord.id)
        .filter(
            LatestRecord.action.in_(["create", "update"]),
            Dataset.id.is_(None),
            or_(
                cast(harvest_source_alias.schema_type, String).like("dcatus1.1:%"),
                LatestRecord.source_transform.isnot(None),
            ),
        )
    )

    if exclude_ids:
        query = query.filter(~LatestRecord.id.in_(tuple(exclude_ids)))

    return query


def _datasets_with_unexpected_records(
    session, exclude_ids: Optional[Iterable[int]] = None
):
    query = (
        session.query(Dataset)
        .join(HarvestRecord, Dataset.harvest_record_id == HarvestRecord.id)
        .filter(
            or_(
                HarvestRecord.status != "success",
                HarvestRecord.action.notin_(["create", "update"]),
            )
        )
    )

    if exclude_ids:
        query = query.filter(~Dataset.id.in_(tuple(exclude_ids)))

    return query


def _report(
    total_records: int,
    total_datasets: int,
    records_missing_count: int,
    datasets_bad_count: int,
):
    click.echo("Dataset Sync Report\n====================")
    click.echo(f"Total harvest records: {total_records}")
    click.echo(f"Total datasets: {total_datasets}")
    click.echo(f"Records needing datasets: {records_missing_count}")
    click.echo(
        "Datasets tied to non-success/non-create records: "
        f"{datasets_bad_count}"
    )
    record_batches = (records_missing_count + BATCH_SIZE - 1) // BATCH_SIZE
    dataset_batches = (datasets_bad_count + BATCH_SIZE - 1) // BATCH_SIZE
    click.echo(
        f"Estimated record batches (size {BATCH_SIZE}): {record_batches}"
    )
    click.echo(
        f"Estimated dataset batches (size {BATCH_SIZE}): {dataset_batches}"
    )


def _sync_impl(apply_changes: bool):
    interface = HarvesterDBInterface(session=db.session)

    try:
        total_records = db.session.query(HarvestRecord.id).count()
        total_datasets = db.session.query(Dataset.id).count()
        records_missing_count = _records_missing_datasets(db.session).count()
        datasets_bad_count = _datasets_with_unexpected_records(db.session).count()

        _report(
            total_records,
            total_datasets,
            records_missing_count,
            datasets_bad_count,
        )

        if apply_changes:
            synced = 0
            current_batch = 0
            failed_record_ids = set()
            while True:
                batch_records = (
                    _records_missing_datasets(db.session, failed_record_ids)
                    .limit(BATCH_SIZE)
                    .all()
                )
                if not batch_records:
                    break
                current_batch += 1
                click.echo(
                    f"Processing batch {current_batch} "
                    f"({len(batch_records)} records)..."
                )
                for record_in_batch in batch_records:
                    try:
                        slug = _insert_dataset_for_record(interface, record_in_batch)
                        synced += 1
                        click.echo(
                            f"Created dataset for record {record_in_batch.id} "
                            f"(slug: {slug})"
                        )
                    except click.ClickException as exc:
                        failed_record_ids.add(record_in_batch.id)
                        click.echo(
                            f"Failed to sync record {record_in_batch.id}: {exc}"
                        )
            click.echo(f"Datasets created: {synced}")

            deleted = 0
            if datasets_bad_count:
                click.echo(
                    f"Deleting {datasets_bad_count} dataset(s) tied "
                    "to invalid harvest records..."
                )
                failed_dataset_ids = set()
                current_batch = 0
                while True:
                    batch = (
                        _datasets_with_unexpected_records(
                            db.session, failed_dataset_ids
                        )
                        .limit(BATCH_SIZE)
                        .all()
                    )
                    if not batch:
                        break
                    current_batch += 1
                    click.echo(
                        f"Deleting batch {current_batch} "
                        f"({len(batch)} datasets)..."
                    )
                    for dataset in batch:
                        try:
                            interface.db.delete(dataset)
                            interface.db.commit()
                            deleted += 1
                            click.echo(
                                f"Deleted dataset {dataset.slug} "
                                f"(harvest_record_id={dataset.harvest_record_id})"
                            )
                        except Exception as exc:  # pragma: no cover - defensive
                            failed_dataset_ids.add(dataset.id)
                            interface.db.rollback()
                            click.echo(
                                f"Failed to delete dataset {dataset.slug}: {exc}"
                            )
                click.echo(f"Datasets deleted: {deleted}")
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
        help=(
            "Create datasets for missing harvest records and delete datasets "
            "tied to invalid harvest records"
        ),
    )
    def dataset_check(apply_changes):
        """Report (and optionally repair) dataset mismatches."""
        _sync_impl(apply_changes)


if __name__ == "__main__":
    from app import create_app

    app = create_app()
    with app.app_context():
        _sync_impl(False)
