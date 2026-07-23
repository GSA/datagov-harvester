import difflib
import json
import re
from datetime import datetime, timezone
from time import monotonic

import click
from flask import Blueprint

from database.interface import HarvesterDBInterface
from database.models import Dataset
from harvester.lib.task_handler import create_task_handler
from harvester.opensearch import OpenSearchClient, OpenSearchReader, OpenSearchWriter
from harvester.runner_settings import harvest_scheduling_is_disabled

search = Blueprint("search", __name__)
# we use this message to detect index failure in GH actions
OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE = "failed to index in this batch"

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
        dynamic = normalized.get("dynamic")
        if isinstance(dynamic, str) and dynamic in {"true", "false"}:
            normalized["dynamic"] = dynamic == "true"
        if normalized.get("search_analyzer") is not None and normalized.get(
            "search_analyzer"
        ) == normalized.get("analyzer"):
            normalized.pop("search_analyzer")
        return normalized

    if isinstance(value, list):
        return [_normalize_mapping_for_comparison(item) for item in value]

    return value


def _mapping_diff(expected, actual) -> str:
    expected_lines = json.dumps(expected, indent=2, sort_keys=True).splitlines()
    actual_lines = json.dumps(actual, indent=2, sort_keys=True).splitlines()
    return "\n".join(
        difflib.unified_diff(
            expected_lines,
            actual_lines,
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
    )


def _default_rebuild_index_name(alias_name: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{alias_name}-{timestamp}"


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def _index_dataset_batches(
    writer: OpenSearchWriter,
    batches,
    total_batches: int,
    *,
    index_name: str | None = None,
    batch_label: str = "Batch",
    indent: str = "",
    sample_size: int = 10,
    log_all_errors: bool = False,
    stop_on_failure: bool = False,
    progress_callback=None,
) -> tuple[int, int, int]:
    """Index preloaded dataset batches with shared reporting and failure handling."""
    total_indexed = 0
    total_failed = 0
    total_skipped = 0

    for batch_number, (datasets, skipped_ids) in enumerate(batches, start=1):
        click.echo(
            f"{indent}{batch_label} {batch_number}/{total_batches}: "
            f"indexing {len(datasets)} dataset(s)..."
        )
        total_skipped += len(skipped_ids)
        if skipped_ids:
            click.echo(
                f"{indent}  Warning: Skipping missing DB IDs: "
                + ", ".join(skipped_ids[:sample_size])
            )

        if not datasets:
            click.echo(f"{indent}  No datasets found for this batch; skipping.")
            continue

        index_options = {"refresh_after": False}
        if index_name is not None:
            index_options["index_name"] = index_name
        succeeded, failed, errors = writer.index_datasets(
            datasets,
            **index_options,
        )
        total_indexed += succeeded
        total_failed += failed

        if failed:
            click.echo(
                f"{indent}  Warning: {failed} dataset(s) "
                f"{OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}."
            )
        if log_all_errors:
            for error in errors:
                click.echo(f"{indent}  OpenSearch error: {error}")

        if progress_callback is not None:
            progress_callback(total_indexed)
        if failed and stop_on_failure:
            break

    return total_indexed, total_failed, total_skipped


def _backfill_index(
    writer: OpenSearchWriter,
    target_index: str,
    batch_size: int,
    total_datasets: int,
) -> tuple[int, int]:
    total_batches = (total_datasets + batch_size - 1) // batch_size
    started_at = monotonic()

    def batches():
        last_dataset_id = None
        while True:
            query = db_interface.db.query(Dataset).order_by(Dataset.id)
            if last_dataset_id is not None:
                query = query.filter(Dataset.id > last_dataset_id)
            datasets = query.limit(batch_size).all()
            if not datasets:
                return

            yield datasets, []
            last_dataset_id = datasets[-1].id
            db_interface.db.expunge_all()

    def report_progress(total_indexed: int):
        elapsed_seconds = monotonic() - started_at
        progress_percent = (
            total_indexed / total_datasets * 100 if total_datasets else 100
        )
        if elapsed_seconds > 0 and total_indexed:
            average_rate = total_indexed / elapsed_seconds
            eta_seconds = (total_datasets - total_indexed) / average_rate
            rate_and_eta = (
                f"average {average_rate:.1f} datasets/s; "
                f"ETA {_format_duration(eta_seconds)}"
            )
        else:
            rate_and_eta = "average rate and ETA unavailable"
        click.echo(
            f"Progress: {total_indexed:,}/{total_datasets:,} datasets "
            f"({progress_percent:.1f}%); elapsed {_format_duration(elapsed_seconds)}; "
            f"{rate_and_eta}."
        )

    total_indexed, total_failed, _ = _index_dataset_batches(
        writer,
        batches(),
        total_batches,
        index_name=target_index,
        batch_label="Backfill batch",
        log_all_errors=True,
        stop_on_failure=True,
        progress_callback=report_progress,
    )
    return total_indexed, total_failed


@search.cli.command("reset-mapping")
def reset_opensearch_mapping():
    """Delete the dataset index and recreate its empty mapping and settings."""
    client = OpenSearchClient.from_environment()
    if client.alias_indices():
        raise click.ClickException(
            "reset-mapping cannot run after datasets becomes an alias; "
            "use search rebuild-index instead."
        )

    click.echo("Deleting OpenSearch dataset index...")
    client.client.indices.delete(index=client.INDEX_NAME)
    click.echo("Index deleted.")

    click.echo("Creating empty index with current mapping and settings...")
    client._ensure_index()

    mapping = client.client.indices.get_mapping(index=client.INDEX_NAME)
    actual_mapping = mapping[client.INDEX_NAME]["mappings"]
    normalized_actual = _normalize_mapping_for_comparison(actual_mapping)
    normalized_expected = _normalize_mapping_for_comparison(client.MAPPINGS)
    if normalized_actual != normalized_expected:
        raise click.ClickException(
            "Created index mapping does not match application mapping:\n"
            + _mapping_diff(normalized_expected, normalized_actual)
        )

    click.echo("Mapping reset successfully. The index is empty.")


def _delete_physical_index(
    client: OpenSearchClient,
    index_name: str,
) -> None:
    physical_index_pattern = rf"{re.escape(client.INDEX_NAME)}-[a-z0-9._-]+"
    if not re.fullmatch(physical_index_pattern, index_name):
        raise click.ClickException(
            f"Index name must be a physical index starting with "
            f"'{client.INDEX_NAME}-'."
        )

    active_indices = client.alias_indices()
    if index_name in active_indices:
        raise click.ClickException(
            f"Cannot delete {index_name}; the {client.INDEX_NAME} alias currently "
            "points to it."
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
            f"Cannot delete {index_name}; attached aliases: "
            + ", ".join(attached_aliases)
        )

    click.echo(f"Deleting unused physical index {index_name}...")
    response = client.client.indices.delete(index=index_name)
    if not response.get("acknowledged"):
        raise click.ClickException(
            f"OpenSearch did not acknowledge deletion of {index_name}."
        )
    click.echo(f"Deleted OpenSearch index {index_name}.")


@search.cli.command("rebuild-index")
@click.option(
    "--target-index",
    help="Physical index name. Defaults to datasets-<UTC timestamp>.",
)
@click.option(
    "--batch-size",
    default=1000,
    show_default=True,
    type=click.IntRange(min=1),
)
@click.option(
    "--allow-legacy-index-removal",
    is_flag=True,
    help=(
        "Allow the first alias cutover to atomically remove a legacy concrete "
        "index named datasets."
    ),
)
@click.option(
    "--switch-alias/--no-switch-alias",
    default=True,
    show_default=True,
    help="Switch the logical datasets alias after a successful backfill.",
)
@click.option(
    "--delete-old-index",
    is_flag=True,
    help="Delete the previous physical index after a successful alias switch.",
)
def rebuild_opensearch_index(
    target_index: str | None,
    batch_size: int,
    allow_legacy_index_removal: bool,
    switch_alias: bool,
    delete_old_index: bool,
):
    """Build and validate a physical index, optionally switching the alias."""
    if delete_old_index and not switch_alias:
        raise click.ClickException(
            "--delete-old-index requires --switch-alias so the previous index "
            "is no longer active."
        )
    if not harvest_scheduling_is_disabled():
        raise click.ClickException(
            "HARVEST_RUNNER_MAX_TASKS must be 0 before rebuilding the index."
        )

    handler = create_task_handler()
    active_tasks = handler.get_active_harvest_tasks()
    if active_tasks is None:
        raise click.ClickException("Could not verify active harvest CF tasks.")
    if active_tasks:
        raise click.ClickException(
            f"{len(active_tasks)} harvest task(s) are still active."
        )

    client = OpenSearchClient.from_environment()
    writer = OpenSearchWriter(client)
    target_index = target_index or _default_rebuild_index_name(client.INDEX_NAME)
    if target_index == client.INDEX_NAME or not target_index.startswith(
        f"{client.INDEX_NAME}-"
    ):
        raise click.ClickException(
            f"Target index must start with '{client.INDEX_NAME}-'."
        )

    current_alias_indices = client.alias_indices()
    has_legacy_concrete_index = (
        not current_alias_indices
        and client.client.indices.exists(index=client.INDEX_NAME)
    )
    if switch_alias and has_legacy_concrete_index and not allow_legacy_index_removal:
        raise click.ClickException(
            "The logical index name is still a concrete index. Re-run with "
            "--allow-legacy-index-removal to perform the one-time atomic conversion."
        )

    initial_db_count = db_interface.db.query(Dataset).count()
    click.echo(f"Creating physical index {target_index}...")
    client.create_index(target_index)

    mapping = client.client.indices.get_mapping(index=target_index)
    actual_mapping = mapping[target_index]["mappings"]
    normalized_actual = _normalize_mapping_for_comparison(actual_mapping)
    normalized_expected = _normalize_mapping_for_comparison(client.MAPPINGS)
    if normalized_actual != normalized_expected:
        raise click.ClickException(
            "New index mapping does not match the application mapping:\n"
            + _mapping_diff(normalized_expected, normalized_actual)
        )

    click.echo(f"Backfilling {initial_db_count} PostgreSQL dataset(s)...")
    indexed, failed = _backfill_index(
        writer, target_index, batch_size, initial_db_count
    )
    if failed or indexed != initial_db_count:
        raise click.ClickException(
            f"Backfill failed: indexed={indexed}, failed={failed}, "
            f"expected={initial_db_count}."
        )

    click.echo("Refreshing and validating the new index...")
    writer._refresh(index_name=target_index)
    final_db_count = db_interface.db.query(Dataset).count()
    index_count = client.index_count(target_index)
    if final_db_count != initial_db_count:
        raise click.ClickException(
            "PostgreSQL dataset count changed during the paused backfill."
        )
    if index_count != final_db_count:
        raise click.ClickException(
            f"Validation failed: PostgreSQL has {final_db_count} datasets but "
            f"{target_index} has {index_count} documents."
        )

    if not switch_alias:
        click.echo(
            f"Rebuild complete: {target_index} is validated; "
            f"{client.INDEX_NAME} was not changed."
        )
        return

    click.echo(f"Atomically switching alias {client.INDEX_NAME} to {target_index}...")
    old_indices, removed_legacy = client.switch_alias(target_index)
    if removed_legacy:
        click.echo("Removed the legacy concrete index during the alias switch.")
    elif old_indices:
        if delete_old_index:
            for old_index in old_indices:
                _delete_physical_index(client, old_index)
        else:
            click.echo("Previous index retained: " + ", ".join(old_indices))
    elif delete_old_index:
        click.echo("No previous physical index was attached to the alias.")
    click.echo(f"Rebuild complete: {client.INDEX_NAME} now points to {target_index}.")


@search.cli.command("delete-index")
@click.option(
    "--index-name",
    required=True,
    help="Exact name of an unused physical index, such as datasets-123456-1.",
)
def delete_opensearch_index(index_name: str):
    """Delete an unused physical dataset index."""
    client = OpenSearchClient.from_environment()
    _delete_physical_index(client, index_name)


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
