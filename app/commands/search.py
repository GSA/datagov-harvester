import re
from datetime import datetime, timezone

import click
from flask import Blueprint
from opensearchpy import helpers

from database.interface import HarvesterDBInterface
from database.models import Dataset
from search.client import OpenSearchClient
from search.documents import DatasetDocument
from search.reader import OpenSearchReader
from search.writer import OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE, OpenSearchWriter

search = Blueprint("search", __name__)

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


@search.cli.command("reset-mapping")
def reset_opensearch_mapping():
    """Delete the dataset index and recreate its empty mapping and settings."""
    client = OpenSearchClient.from_environment()

    click.echo("Deleting OpenSearch dataset index...")
    client.client.indices.delete(index=client.INDEX_NAME)
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
def compare_opensearch(sample_size: int, update: bool, force_update: bool):
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


def _default_rebuild_index_name(alias_name: str) -> str:
    """Build a versioned physical index name like datasets-20260723152900."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{alias_name}-{timestamp}"


def _alias_indices(client, alias_name: str) -> list[str]:
    """Return the physical indices currently attached to an alias.

    Returns an empty list when the name is not (yet) an alias, which is the
    case on the very first cutover while ``datasets`` is still a concrete index.
    """
    if not client.client.indices.exists_alias(name=alias_name):
        return []
    return sorted(client.client.indices.get_alias(name=alias_name))


def _switch_datasets_alias(client, target_index: str, allow_legacy_index_removal: bool):
    """Atomically point the logical ``datasets`` name at ``target_index``.

    All alias changes are submitted in a single ``update_aliases`` request so
    readers (catalog + harvester) never observe a moment without an index. The
    first cutover finds a legacy *concrete* index named ``datasets``; OpenSearch
    can remove that index and create the alias in the same atomic request.

    Returns ``(old_indices, removed_legacy_index)``.
    """
    alias_name = client.INDEX_NAME
    if not client.client.indices.exists(index=target_index):
        raise click.ClickException(
            f"OpenSearch target index does not exist: {target_index}"
        )

    old_indices = _alias_indices(client, alias_name)
    if old_indices == [target_index]:
        return old_indices, False

    actions = [
        {"remove": {"index": index, "alias": alias_name}} for index in old_indices
    ]

    removed_legacy_index = False
    if not old_indices and client.client.indices.exists(index=alias_name):
        # ``datasets`` is still a concrete index rather than an alias.
        if not allow_legacy_index_removal:
            raise click.ClickException(
                f"'{alias_name}' is still a concrete index. Re-run with "
                "--allow-legacy-index-removal to perform the one-time atomic "
                "conversion to an alias."
            )
        actions.append({"remove_index": {"index": alias_name}})
        removed_legacy_index = True

    actions.append({"add": {"index": target_index, "alias": alias_name}})

    response = client.client.indices.update_aliases(body={"actions": actions})
    if not response.get("acknowledged"):
        raise click.ClickException("OpenSearch did not acknowledge the alias switch.")
    return old_indices, removed_legacy_index


def _backfill_from_postgres(client, target_index: str, batch_size: int):
    """Regenerate every dataset document from PostgreSQL into ``target_index``.

    PostgreSQL is the source of truth, so we rebuild documents with the same
    ``DatasetDocument`` transformer the writer uses in production and simply
    redirect ``_index`` to the new physical index. This (unlike a server-side
    OpenSearch ``_reindex``) picks up document-shape changes as well as mapping
    changes, and mirrors the ``compare --force-update`` repair path. Datasets are
    read in keyset-paginated batches to bound memory on large tables.

    Returns ``(indexed, failed, errors)``.
    """
    total_indexed = 0
    total_failed = 0
    errors: list = []
    last_id = None
    batch_number = 0

    while True:
        query = db_interface.db.query(Dataset).order_by(Dataset.id)
        if last_id is not None:
            query = query.filter(Dataset.id > last_id)
        datasets = query.limit(batch_size).all()
        if not datasets:
            break

        batch_number += 1
        click.echo(
            f"  Backfill batch {batch_number}: indexing {len(datasets)} dataset(s)..."
        )
        documents = []
        for dataset in datasets:
            document = DatasetDocument(dataset).dataset_to_document()
            document["_index"] = target_index
            documents.append(document)

        for success, item in helpers.streaming_bulk(
            client.client,
            documents,
            raise_on_error=False,
            max_retries=8,
        ):
            if success:
                total_indexed += 1
            else:
                total_failed += 1
                errors.append(item)

        last_id = datasets[-1].id
        db_interface.db.expunge_all()

        if total_failed:
            break

    return total_indexed, total_failed, errors


@search.cli.command("rebuild-index")
@click.option(
    "--target-index",
    help="Physical index name. Defaults to datasets-<UTC timestamp>.",
)
@click.option(
    "--switch-alias/--no-switch-alias",
    default=True,
    show_default=True,
    help="Switch the datasets alias to the rebuilt index after validation.",
)
@click.option(
    "--allow-legacy-index-removal",
    is_flag=True,
    help=(
        "Allow the first cutover to atomically remove a legacy concrete index "
        "named datasets so it can become an alias."
    ),
)
@click.option(
    "--batch-size",
    default=1000,
    show_default=True,
    type=click.IntRange(min=1),
    help="Number of datasets read from PostgreSQL per backfill batch.",
)
@click.option(
    "--delete-old-index",
    is_flag=True,
    help=(
        "After a successful alias switch, delete the index(es) the alias "
        "previously pointed at. Ignored with --no-switch-alias."
    ),
)
def rebuild_opensearch_index(
    target_index: str | None,
    switch_alias: bool,
    allow_legacy_index_removal: bool,
    batch_size: int,
    delete_old_index: bool,
):
    """Zero-downtime rebuild: backfill datasets into a fresh index, then swap alias.

    Builds a new physical index with the current application mapping, regenerates
    every document from PostgreSQL (the source of truth) into it, validates the
    document count, and (by default) atomically switches the ``datasets`` alias to
    the new index. Search stays available throughout because the old index keeps
    serving reads until the atomic alias switch.

    Backfilling from PostgreSQL (rather than an OpenSearch ``_reindex``) means the
    rebuild also picks up document-shape changes, not just mapping changes, and
    reuses the same repair path as ``compare --force-update``.
    """
    client = OpenSearchClient.from_environment()
    alias_name = client.INDEX_NAME

    target_index = target_index or _default_rebuild_index_name(alias_name)
    if target_index == alias_name or not target_index.startswith(f"{alias_name}-"):
        raise click.ClickException(
            f"Target index must be a physical index starting with '{alias_name}-'."
        )
    if client.client.indices.exists(index=target_index):
        raise click.ClickException(f"OpenSearch index already exists: {target_index}")

    # Fail fast, before creating anything, if the one-time legacy conversion is
    # needed but has not been explicitly allowed.
    current_alias_indices = _alias_indices(client, alias_name)
    has_legacy_concrete_index = not current_alias_indices and (
        client.client.indices.exists(index=alias_name)
    )
    if switch_alias and has_legacy_concrete_index and not allow_legacy_index_removal:
        raise click.ClickException(
            f"'{alias_name}' is still a concrete index. Re-run with "
            "--allow-legacy-index-removal to perform the one-time atomic "
            "conversion to an alias."
        )

    click.echo(f"Creating physical index {target_index} with current mapping...")
    body = {"mappings": client.MAPPINGS}
    if client.SETTINGS:
        body["settings"] = client.SETTINGS
    client.client.indices.create(index=target_index, body=body)

    mapping = client.client.indices.get_mapping(index=target_index)
    actual_mapping = mapping[target_index]["mappings"]
    if _normalize_mapping_for_comparison(
        actual_mapping
    ) != _normalize_mapping_for_comparison(client.MAPPINGS):
        raise click.ClickException(
            "Created index mapping does not match application mapping."
        )

    db_count = db_interface.db.query(Dataset).count()
    click.echo(f"Backfilling {db_count} PostgreSQL dataset(s) into {target_index}...")
    indexed, failed, errors = _backfill_from_postgres(client, target_index, batch_size)
    if failed:
        for error in errors:
            click.echo(f"  OpenSearch error: {error}")
        raise click.ClickException(
            f"Backfill {OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}: "
            f"indexed={indexed}, failed={failed}."
        )

    # Harvesting is paused and drained before this runs, so the DB is stable;
    # the index must end up with exactly one document per dataset.
    client.client.indices.refresh(index=target_index)
    target_count = client.client.count(index=target_index)["count"]
    if target_count != db_count:
        raise click.ClickException(
            f"Validation failed ({OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}): "
            f"PostgreSQL has {db_count} dataset(s) but {target_index} has "
            f"{target_count} document(s)."
        )
    click.echo(f"Validated {target_index}: {target_count} document(s).")

    if not switch_alias:
        click.echo(
            f"Rebuild complete: {target_index} is validated. The {alias_name} "
            "alias was not changed."
        )
        return

    click.echo(f"Atomically switching alias {alias_name} to {target_index}...")
    old_indices, removed_legacy = _switch_datasets_alias(
        client, target_index, allow_legacy_index_removal
    )
    if removed_legacy:
        click.echo(f"Converted the legacy concrete index '{alias_name}' into an alias.")
    click.echo(f"Rebuild complete: {alias_name} now points to {target_index}.")

    if old_indices:
        if delete_old_index:
            for old_index in old_indices:
                _delete_physical_index(client, old_index)
        else:
            click.echo(
                "Previous index no longer serving traffic (safe to delete with "
                f"'flask search delete-index'): {', '.join(old_indices)}"
            )


def _delete_physical_index(client, index_name: str):
    """Delete a physical dataset index after guarding against unsafe removals.

    Refuses to delete the logical alias name itself or any index still attached
    to an alias, so this can only ever remove an old index left behind by a
    rebuild.
    """
    alias_name = client.INDEX_NAME

    physical_index_pattern = rf"{re.escape(alias_name)}-[a-z0-9._-]+"
    if not re.fullmatch(physical_index_pattern, index_name):
        raise click.ClickException(
            f"Index name must be a physical index starting with '{alias_name}-'."
        )
    if not client.client.indices.exists(index=index_name):
        raise click.ClickException(f"OpenSearch index does not exist: {index_name}")

    alias_response = client.client.indices.get_alias(index=index_name)
    attached_aliases = sorted(
        {
            alias
            for index_details in alias_response.values()
            for alias in index_details.get("aliases", {})
        }
    )
    if attached_aliases:
        raise click.ClickException(
            f"Cannot delete {index_name}; it is still attached to alias(es): "
            + ", ".join(attached_aliases)
        )

    click.echo(f"Deleting unused physical index {index_name}...")
    response = client.client.indices.delete(index=index_name)
    if not response.get("acknowledged"):
        raise click.ClickException(
            f"OpenSearch did not acknowledge deletion of {index_name}."
        )
    click.echo(f"Deleted OpenSearch index {index_name}.")


@search.cli.command("delete-index")
@click.option(
    "--index-name",
    required=True,
    help="Exact name of an unused physical index, such as datasets-20260723152900.",
)
def delete_opensearch_index(index_name: str):
    """Delete an unused physical dataset index."""
    client = OpenSearchClient.from_environment()
    _delete_physical_index(client, index_name)
