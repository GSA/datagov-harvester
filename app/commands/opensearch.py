from datetime import datetime, timezone

import click
from flask import Blueprint
from opensearchpy.helpers import scan

from database.interface import HarvesterDBInterface
from database.models import Dataset
from harvester.opensearch import OpenSearchInterface

search = Blueprint("search", __name__)


@search.cli.command("compare")
@click.option(
    "--sample-size",
    default=10,
    show_default=True,
    help="How many example IDs to print for each discrepancy type.",
)
@click.option(
    "--update",
    is_flag=True,
    help=(
        "Automatically index missing/updated datasets and delete extra docs "
        "from OpenSearch."
    ),
)
@click.option(
    "--force-update",
    is_flag=True,
    help="Re-index all datasets from DB regardless of last_harvested_date.",
)
def compare_opensearch(sample_size: int, update: bool, force_update: bool):
    """Report (and optionally update) dataset ID discrepancies between
    DB and OpenSearch."""

    interface = HarvesterDBInterface()
    client = OpenSearchInterface.from_environment()
    reharvest = {}

    def normalize_last_harvested(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(cleaned)
            except ValueError:
                return cleaned
        else:
            return str(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        # Normalize to milliseconds to match OpenSearch docvalue precision.
        dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
        return dt.isoformat(timespec="milliseconds")

    def index_dataset_batches(
        dataset_ids: list[str], intro_message: str, log_all_errors=False
    ):
        click.echo(intro_message)
        batch_size = 1000
        total_batches = (len(dataset_ids) + batch_size - 1) // batch_size
        total_indexed = 0
        total_skipped = 0

        for batch_number, batch_ids in enumerate(
            (
                dataset_ids[i : i + batch_size]
                for i in range(0, len(dataset_ids), batch_size)
            ),
            start=1,
        ):
            click.echo(
                f"  Batch {batch_number}/{total_batches}: indexing {len(batch_ids)} "
                "dataset(s)…"
            )
            datasets = (
                interface.db.query(Dataset).filter(Dataset.id.in_(batch_ids)).all()
            )

            found_ids = {
                dataset.id: [dataset.harvest_source.id, dataset.harvest_source.name]
                for dataset in datasets
            }
            skipped = [
                dataset_id for dataset_id in batch_ids if dataset_id not in found_ids
            ]
            total_skipped += len(skipped)

            if skipped:
                click.echo(
                    "    Warning: Skipping missing DB IDs: "
                    + ", ".join(skipped[:sample_size])
                )

            if datasets:
                succeeded, failed, errors = client.index_datasets(
                    datasets, refresh_after=False
                )
                total_indexed += succeeded
                if failed:
                    click.echo(
                        f"    Warning: {failed} dataset(s) failed to index in "
                        "this batch."
                    )
                    if log_all_errors:
                        for error in errors:
                            dataset_id = error.get("dataset_id")
                            if dataset_id is not None:
                                harvest_source = found_ids[dataset_id]
                                # doesn't matter if we reassign. it's
                                # the same key: value pair
                                reharvest[harvest_source[0]] = harvest_source[1]
                            click.echo(error)

            else:
                click.echo("    No datasets found for this batch; skipping.")

        click.echo(
            f"Indexed {total_indexed} datasets. Skipped {total_skipped} missing DB "
            "rows."
        )

    click.echo("Collecting dataset IDs from DB…")
    db_rows = interface.db.query(Dataset.id, Dataset.last_harvested_date).all()
    db_last_harvested = {
        dataset_id: normalize_last_harvested(last_harvested)
        for dataset_id, last_harvested in db_rows
    }
    db_ids = set(db_last_harvested)
    click.echo(f"Database datasets: {len(db_ids)}")

    click.echo("Collecting document IDs from OpenSearch…")
    os_docs = {}
    for hit in scan(
        client.client,
        index=client.INDEX_NAME,
        size=200,
        _source=False,
        stored_fields=[],
        docvalue_fields=["last_harvested_date"],
    ):
        fields = hit.get("fields", {})
        last_harvested = None
        if fields.get("last_harvested_date"):
            last_harvested = fields["last_harvested_date"][0]
        elif hit.get("_source"):
            last_harvested = hit["_source"].get("last_harvested_date")
        os_docs[hit["_id"]] = normalize_last_harvested(last_harvested)

    os_ids = set(os_docs)
    click.echo(f"OpenSearch documents: {len(os_ids)}")

    missing = sorted(db_ids - os_ids)
    extra = sorted(os_ids - db_ids)
    shared_ids = sorted(db_ids & os_ids)
    updated_details = [
        (dataset_id, db_last_harvested.get(dataset_id), os_docs.get(dataset_id))
        for dataset_id in shared_ids
        if db_last_harvested.get(dataset_id) != os_docs.get(dataset_id)
    ]
    updated_ids = [dataset_id for dataset_id, _, _ in updated_details]

    click.echo(f"Missing in OpenSearch (should be indexed): {len(missing)}")
    if missing:
        click.echo("Example missing IDs: " + ", ".join(missing[:sample_size]))
    else:
        click.echo("Example missing IDs: none")

    click.echo(f"Extra in OpenSearch (should be deleted): {len(extra)}")
    if extra:
        click.echo("Example extra IDs: " + ", ".join(extra[:sample_size]))
    else:
        click.echo("Example extra IDs: none")

    click.echo(
        f"Updated in OpenSearch (last_harvested_date differs): {len(updated_details)}"
    )
    if updated_details:
        sample_entries = [
            f"{dataset_id} (DB: {db_value or 'None'}, OS: {os_value or 'None'})"
            for dataset_id, db_value, os_value in updated_details[:sample_size]
        ]
        click.echo("Example updated IDs: " + "; ".join(sample_entries))
    else:
        click.echo("Example updated IDs: none")

    if force_update:
        update = True

    if not update:
        return

    click.echo("\nUpdating discrepancies…")

    force_reindex_ids = sorted(db_ids) if force_update else []
    if force_reindex_ids:
        index_dataset_batches(
            force_reindex_ids,
            (
                "Force re-indexing "
                f"{len(force_reindex_ids)} datasets regardless of last_harvested_date…"
            ),
            log_all_errors=True,
        )
    else:
        if missing:
            index_dataset_batches(
                missing,
                f"Indexing {len(missing)} missing datasets…",
                log_all_errors=True,
            )

        if updated_ids:
            index_dataset_batches(
                updated_ids,
                f"Re-indexing {len(updated_ids)} updated datasets…",
                log_all_errors=True,
            )

    # print the harvest sources with datasets that couldn't sync with opensearch
    click.echo("Harvest sources not synced with opensearch...")
    for harvest_source_id, harvest_source_name in reharvest.items():
        click.echo(f"{harvest_source_id} {harvest_source_name}")

    if extra:
        click.echo(f"Deleting {len(extra)} extra documents from OpenSearch…")
        deleted = 0
        batch_size = 1000
        total_batches = (len(extra) + batch_size - 1) // batch_size
        for batch_number, batch_ids in enumerate(
            (extra[i : i + batch_size] for i in range(0, len(extra), batch_size)),
            start=1,
        ):
            click.echo(
                f"  Batch {batch_number}/{total_batches}: "
                f"deleting {len(batch_ids)} document(s)…"
            )
            for doc_id in batch_ids:
                try:
                    client.client.delete(index=client.INDEX_NAME, id=doc_id)
                    deleted += 1
                except Exception as exc:  # pragma: no cover - best-effort cleanup
                    click.echo(f"    Failed to delete document {doc_id}: {exc}")

        click.echo(f"Deleted {deleted} documents from OpenSearch.")

    if missing or extra or updated_ids or force_reindex_ids:
        click.echo("Refreshing OpenSearch index…")
        client._refresh()
        click.echo("Done.")
    else:
        click.echo("Nothing to update; datasets and index are already in sync.")
