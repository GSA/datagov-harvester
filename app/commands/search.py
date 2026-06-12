from datetime import datetime, timezone

import click
from flask import Blueprint
from opensearchpy.helpers import scan

from database.models import Dataset, db
from harvester.opensearch import OpenSearchInterface

search = Blueprint("search", __name__)


def _normalize_last_harvested(value):
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
    dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
    return dt.isoformat(timespec="milliseconds")


@search.cli.command("reset-mapping")
def reset_opensearch_mapping():
    """Delete the dataset index and recreate its empty mapping and settings."""
    client = OpenSearchInterface.from_environment()

    click.echo("Deleting OpenSearch dataset index...")
    client.client.indices.delete(index=client.INDEX_NAME)
    click.echo("Index deleted.")

    click.echo("Creating empty index with current mapping and settings...")
    client._ensure_index()

    mapping = client.client.indices.get_mapping(index=client.INDEX_NAME)
    actual_mapping = mapping[client.INDEX_NAME]["mappings"]
    if actual_mapping != client.MAPPINGS:
        raise click.ClickException(
            "Created index mapping does not match application mapping."
        )

    click.echo("Mapping reset successfully. The index is empty.")


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
    """Report and optionally repair DB/OpenSearch dataset discrepancies."""
    client = OpenSearchInterface.from_environment()

    def index_dataset_batches(
        dataset_ids: list[str], intro_message: str, log_all_errors=False
    ):
        click.echo(intro_message)
        batch_size = 1000
        total_batches = (len(dataset_ids) + batch_size - 1) // batch_size
        total_indexed = 0
        total_skipped = 0

        for batch_number, offset in enumerate(
            range(0, len(dataset_ids), batch_size), start=1
        ):
            batch_ids = dataset_ids[offset : offset + batch_size]
            click.echo(
                f"  Batch {batch_number}/{total_batches}: "
                f"indexing {len(batch_ids)} dataset(s)..."
            )
            datasets = db.session.query(Dataset).filter(Dataset.id.in_(batch_ids)).all()
            found_ids = {dataset.id for dataset in datasets}
            skipped = [
                dataset_id for dataset_id in batch_ids if dataset_id not in found_ids
            ]
            total_skipped += len(skipped)

            if skipped:
                click.echo(
                    "    Warning: Skipping missing DB IDs: "
                    + ", ".join(skipped[:sample_size])
                )

            if not datasets:
                click.echo("    No datasets found for this batch; skipping.")
                continue

            succeeded, failed, errors = client.index_datasets(
                datasets, refresh_after=False
            )
            total_indexed += succeeded
            if failed:
                click.echo(
                    f"    Warning: {failed} dataset(s) failed to index in this batch."
                )
                if log_all_errors:
                    for error in errors:
                        click.echo(error)

        click.echo(
            f"Indexed {total_indexed} datasets. "
            f"Skipped {total_skipped} missing DB rows."
        )

    click.echo("Collecting dataset IDs from DB...")
    db_rows = db.session.query(Dataset.id, Dataset.last_harvested_date).all()
    db_last_harvested = {
        dataset_id: _normalize_last_harvested(last_harvested)
        for dataset_id, last_harvested in db_rows
    }
    db_ids = set(db_last_harvested)
    click.echo(f"Database datasets: {len(db_ids)}")

    click.echo("Collecting document IDs from OpenSearch...")
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
        os_docs[hit["_id"]] = _normalize_last_harvested(last_harvested)

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
    click.echo(
        "Example missing IDs: "
        + (", ".join(missing[:sample_size]) if missing else "none")
    )
    click.echo(f"Extra in OpenSearch (should be deleted): {len(extra)}")
    click.echo(
        "Example extra IDs: " + (", ".join(extra[:sample_size]) if extra else "none")
    )
    click.echo(
        "Updated in OpenSearch (last_harvested_date differs): "
        f"{len(updated_details)}"
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

    click.echo("\nUpdating discrepancies...")
    force_reindex_ids = sorted(db_ids) if force_update else []
    if force_reindex_ids:
        index_dataset_batches(
            force_reindex_ids,
            f"Force re-indexing {len(force_reindex_ids)} datasets...",
            log_all_errors=True,
        )
    else:
        if missing:
            index_dataset_batches(
                missing,
                f"Indexing {len(missing)} missing datasets...",
                log_all_errors=True,
            )
        if updated_ids:
            index_dataset_batches(
                updated_ids,
                f"Re-indexing {len(updated_ids)} updated datasets...",
                log_all_errors=True,
            )

    if extra:
        click.echo(f"Deleting {len(extra)} extra documents from OpenSearch...")
        deleted = 0
        for doc_id in extra:
            try:
                client.client.delete(index=client.INDEX_NAME, id=doc_id)
                deleted += 1
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                click.echo(f"    Failed to delete document {doc_id}: {exc}")
        click.echo(f"Deleted {deleted} documents from OpenSearch.")

    if missing or extra or updated_ids or force_reindex_ids:
        click.echo("Refreshing OpenSearch index...")
        client._refresh()
        click.echo("Done.")
    else:
        click.echo("Nothing to update; datasets and index are already in sync.")
