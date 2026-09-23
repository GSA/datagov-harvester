from datetime import datetime, timezone

import click
from flask import Blueprint

from database.interface import HarvesterDBInterface
from database.models import Dataset
from search.client import OpenSearchClient
from search.reader import OpenSearchReader
from search.writer import OpenSearchWriter

search = Blueprint("search", __name__)

OPENSEARCH_MAX_FAILED_RECORDS = 50
OPENSEARCH_MISSING_DOCUMENTS_BANNER = "MISSING DATASET IDS (not in OpenSearch)"

db_interface = HarvesterDBInterface()


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


def _normalize_mapping_for_comparison(value):
    """Normalize mapping defaults omitted by OpenSearch responses."""
    if isinstance(value, dict):
        normalized = {
            key: _normalize_mapping_for_comparison(item) for key, item in value.items()
        }
        if normalized.get("search_analyzer") is not None and normalized.get(
            "search_analyzer"
        ) == normalized.get("analyzer"):
            normalized.pop("search_analyzer")
        # OpenSearch echoes `dynamic` back as a string ("false"), while the
        # application mapping declares it as a bool. Compare them as strings so
        # the round trip doesn't look like a mismatch.
        if "dynamic" in normalized and isinstance(normalized["dynamic"], bool):
            normalized["dynamic"] = str(normalized["dynamic"]).lower()
        return normalized

    if isinstance(value, list):
        return [_normalize_mapping_for_comparison(item) for item in value]

    return value


def _describe_failures(failed: int, total: int, max_failed: int) -> str:
    share = (failed / total * 100) if total > 0 else 0.0
    return (
        f"{failed} of {total} ({share:.3f}%) failed; "
        f"allowed up to {max_failed} record(s)"
    )


def _report_document_ids(banner: str, doc_ids: list[str]):
    click.echo("")
    click.echo(f"{banner} ({len(doc_ids)} total)")
    for doc_id in sorted(doc_ids):
        click.echo(f"  {doc_id}")
    click.echo("")


def _clear_datasets_index(client, index_name: str):
    """Remove whatever currently answers to ``index_name``.

    Constructing a client calls ``_ensure_index()``, so the name always resolves
    to something by the time this runs. It is usually a plain index, but on a
    cluster that was rebuilt by the retired ``rebuild-index`` workflow it is an
    *alias* pointing at a ``datasets-<suffix>`` index -- and ``indices.delete``
    rejects an alias with ``illegal_argument_exception``. Drop the indices behind
    the alias so a cluster in either state ends up equally clean.
    """
    if client.client.indices.exists_alias(name=index_name):
        aliased = sorted(client.client.indices.get_alias(name=index_name))
        click.echo(
            f"'{index_name}' is a leftover alias for {', '.join(aliased)}; "
            "removing both so it becomes a plain index."
        )
        # One request: deleting an aliased index removes its alias with it, so
        # this never leaves the name pointing at something already deleted.
        client.client.indices.delete(index=",".join(aliased))
        return

    if client.client.indices.exists(index=index_name):
        client.client.indices.delete(index=index_name)


@search.cli.command("reset-mapping")
def reset_opensearch_mapping():
    """Delete the dataset index and recreate its empty mapping and settings."""
    client = OpenSearchClient.from_environment()

    click.echo("Deleting OpenSearch dataset index...")
    _clear_datasets_index(client, client.INDEX_NAME)
    click.echo("Index deleted.")

    click.echo("Creating empty index with current mapping and settings...")
    client._ensure_index()

    mapping = client.client.indices.get_mapping(index=client.INDEX_NAME)
    actual_mapping = mapping[client.INDEX_NAME]["mappings"]
    if _normalize_mapping_for_comparison(
        actual_mapping
    ) != _normalize_mapping_for_comparison(client.MAPPINGS):
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
@click.option(
    "--fail-on-discrepancy",
    is_flag=True,
    help=(
        "Exit non-zero when the index has too many missing documents, any extra "
        "documents, or any stale documents."
    ),
)
@click.option(
    "--max-failed-records",
    default=OPENSEARCH_MAX_FAILED_RECORDS,
    show_default=True,
    type=click.IntRange(min=0),
    help=(
        "With --fail-on-discrepancy, how many missing datasets may be tolerated. "
        "Extra and stale documents always fail."
    ),
)
def compare_opensearch(
    sample_size: int,
    update: bool,
    force_update: bool,
    fail_on_discrepancy: bool,
    max_failed_records: int,
):
    """Report and optionally repair DB/OpenSearch dataset discrepancies."""
    os_client = OpenSearchClient.from_environment()
    os_writer = OpenSearchWriter(os_client)
    os_reader = OpenSearchReader(os_client)

    click.echo("Collecting dataset IDs from DB...")
    db_rows = db_interface.db.query(Dataset.id, Dataset.last_harvested_date).all()
    db_last_harvested = {
        dataset_id: _normalize_last_harvested(last_harvested)
        for dataset_id, last_harvested in db_rows
    }
    db_ids = set(db_last_harvested)
    click.echo(f"Database datasets: {len(db_ids)}")

    click.echo("Collecting document IDs from OpenSearch...")
    os_docs = {}

    for hit in os_reader.scan_index(
        index_name=os_client.INDEX_NAME,
        size=200,
        source=False,
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

    if fail_on_discrepancy:
        within_allowance = len(missing) <= max_failed_records
        failure_summary = _describe_failures(
            len(missing), len(db_ids), max_failed_records
        )

        if missing:
            _report_document_ids(OPENSEARCH_MISSING_DOCUMENTS_BANNER, missing)

        if extra or updated_details or not within_allowance:
            raise click.ClickException(
                f"Discrepancies found: {len(missing)} missing, {len(extra)} extra, "
                f"{len(updated_details)} updated"
                + (f" ({failure_summary})" if missing else "")
                + "."
            )
        if missing:
            click.echo(
                f"TOLERATED: {failure_summary}. Every missing id is listed above."
            )

    if force_update:
        update = True
    if not update:
        return

    click.echo("\nUpdating discrepancies...")
    force_reindex_ids = sorted(db_ids) if force_update else []
    if force_reindex_ids:
        os_writer.index_dataset_batches(
            force_reindex_ids,
            f"Force re-indexing {len(force_reindex_ids)} datasets...",
            db_interface,
            sample_size=sample_size,
            log_all_errors=True,
        )
    else:
        if missing:
            os_writer.index_dataset_batches(
                missing,
                f"Indexing {len(missing)} missing datasets...",
                db_interface,
                sample_size=sample_size,
                log_all_errors=True,
            )
        if updated_ids:
            os_writer.index_dataset_batches(
                updated_ids,
                f"Re-indexing {len(updated_ids)} updated datasets...",
                db_interface,
                sample_size=sample_size,
                log_all_errors=True,
            )
    if extra:
        click.echo(f"Deleting {len(extra)} extra documents from OpenSearch...")
        deleted = 0
        for doc_id in extra:
            try:
                os_writer.client.delete(index=os_client.INDEX_NAME, id=doc_id)
                deleted += 1
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                click.echo(f"    Failed to delete document {doc_id}: {exc}")
        click.echo(f"Deleted {deleted} documents from OpenSearch.")

    if missing or extra or updated_ids or force_reindex_ids:
        click.echo("Refreshing OpenSearch index...")
        os_writer._refresh()
        click.echo("Done.")
    else:
        click.echo("Nothing to update; datasets and index are already in sync.")
