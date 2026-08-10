import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone

import click
from flask import Blueprint
from opensearchpy import helpers
from opensearchpy.exceptions import RequestError

from database.interface import HarvesterDBInterface
from database.models import Dataset
from search.client import OpenSearchClient
from search.documents import DatasetDocument
from search.reader import OpenSearchReader
from search.writer import OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE, OpenSearchWriter

search = Blueprint("search", __name__)
# indices.create waits for shards to become active, which can exceed the client's
# default 60s socket timeout on a loaded cluster; see _create_rebuild_index.
OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS = 300
# Grep target for the skipped-id block in CI logs and task output.
OPENSEARCH_SKIPPED_DOCUMENTS_BANNER = "SKIPPED DATASET IDS (not indexed)"

# Which cluster a command talks to. ``live`` is the cluster currently serving
# catalog and harvester traffic; ``next`` is a replacement cluster bound
# alongside it during a migration. See docs/ops/migrate-opensearch-cluster.md.
CLUSTER_LIVE = "live"
CLUSTER_NEXT = "next"
# ``OpenSearchClient`` reads the cluster host and credentials from these fixed
# environment variable names, so pointing it at the replacement cluster means
# rebinding them for the duration of the constructor call.
OPENSEARCH_NEXT_ENVIRONMENT_VARIABLES = {
    "OPENSEARCH_HOST": "OPENSEARCH_NEXT_HOST",
    "OPENSEARCH_ACCESS_KEY": "OPENSEARCH_NEXT_ACCESS_KEY",
    "OPENSEARCH_SECRET_KEY": "OPENSEARCH_NEXT_SECRET_KEY",
}

db_interface = HarvesterDBInterface()


def _is_aws_opensearch_host(host: str | None) -> bool:
    """Whether ``host`` selects the client's AWS SigV4 path.

    Delegates to ``OpenSearchClient._extract_hostname`` so this check cannot
    drift from the suffix test ``from_environment`` uses to choose between the
    signed AWS transport and the local admin:admin one.
    """
    if not host:
        return False
    try:
        hostname = OpenSearchClient._extract_hostname(host)
    except ValueError:
        # urlparse raises on malformed URLs such as "http://[::1". Treat an
        # unparseable value as non-AWS; the client will fail on it anyway, and a
        # raw traceback out of a cf task log helps nobody.
        return False
    if not hostname:
        return False
    # Normalize before the suffix test. Without this, a real AWS endpoint written
    # with different case, a trailing FQDN dot, or an explicit port classifies as
    # "local", which drops the credential requirement and sends the client down
    # the admin:admin path against a signed endpoint.
    hostname = hostname.strip().lower().rstrip(".")
    hostname = hostname.rsplit(":", 1)[0] if hostname.count(":") == 1 else hostname
    return hostname == "es.amazonaws.com" or hostname.endswith(".es.amazonaws.com")


@contextmanager
def _next_cluster_environment():
    """Expose the replacement cluster under the env names the client reads.

    ``OpenSearchClient._create_aws_opensearch_client`` reads ``OPENSEARCH_HOST``
    and the access/secret key pair from fixed environment variable names, and
    captures all three at construction time -- the host goes into the transport's
    host list and the keys into the SigV4 signer. Temporarily rebinding those
    names around the constructor therefore yields a client permanently pinned to
    the replacement cluster, without having to pass credentials on a command line
    (which would leak them into ``cf run-task`` strings and CI logs).

    Now that ``search/`` is vendored rather than an external dependency, a
    ``for_host(host, access_key=..., secret_key=...)`` constructor there could
    replace this whole dance; see GSA/data.gov#6211.
    """
    next_host = (os.environ.get("OPENSEARCH_NEXT_HOST") or "").strip()
    live_host = (os.environ.get("OPENSEARCH_HOST") or "").strip()
    # The access/secret pair is only consulted on the AWS SigV4 path, which the
    # client selects by hostname suffix. Requiring them for a local host would
    # mean inventing dummy values just to exercise this path in development.
    required = (
        list(OPENSEARCH_NEXT_ENVIRONMENT_VARIABLES.values())
        if _is_aws_opensearch_host(next_host)
        else ["OPENSEARCH_NEXT_HOST"]
    )
    missing = sorted(
        name for name in required if not (os.environ.get(name) or "").strip()
    )
    if missing:
        raise click.ClickException(
            f"--cluster {CLUSTER_NEXT} requires " + ", ".join(missing) + ". Bind the "
            "replacement OpenSearch service and set OPENSEARCH_NEXT_SERVICE_NAME so "
            ".profile exports its credentials."
        )

    if next_host == live_host:
        # The whole point of --cluster next is to keep load and destructive
        # operations off the live cluster. If both names resolve to the same host
        # every such command would silently hit production while reporting
        # "next" -- so refuse rather than pretend. This is reachable in normal
        # operation: after adopting a replacement cluster, both variables name it
        # until OPENSEARCH_NEXT_SERVICE_NAME is unset.
        raise click.ClickException(
            f"OPENSEARCH_NEXT_HOST is the same host as the live cluster "
            f"({live_host}), so --cluster {CLUSTER_NEXT} would operate on live. "
            "Unset OPENSEARCH_NEXT_SERVICE_NAME (the replacement cluster is now "
            "the live one), or point it at a different instance."
        )

    def _apply(values: dict):
        """Set each live name, or unset it when the paired value is absent.

        Unsetting matters: an absent replacement key must not leave the live
        cluster's credential visible to a client aimed at the other host.
        """
        for name, value in values.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    previous = {
        live_name: os.environ.get(live_name)
        for live_name in OPENSEARCH_NEXT_ENVIRONMENT_VARIABLES
    }
    # Enter the try BEFORE mutating, so an interruption partway through the swap
    # (a SIGTERM during a cf run-task, say) cannot leave a half-swapped, mismatched
    # credential set behind.
    try:
        _apply(
            {
                live_name: os.environ.get(next_name)
                for live_name, next_name in (
                    OPENSEARCH_NEXT_ENVIRONMENT_VARIABLES.items()
                )
            }
        )
        yield
    finally:
        # Restore unconditionally: a later command in the same process (and the
        # harvest write path) must still resolve the live cluster.
        _apply(previous)


def _client_for_cluster(
    cluster: str, announce: bool = False, ensure_index: bool = True
):
    """Build an ``OpenSearchClient`` for the live or the replacement cluster.

    With ``announce``, echo which cluster and host was resolved. Every command
    that can mutate or verify a cluster should say which one it touched -- that
    line is the operator's only cross-check that a ``next`` run really did stay
    off the live cluster.

    ``ensure_index=False`` is for ``rebuild-index``, which owns index creation
    and applies its longer timeout and idempotent retry handling.
    """
    if cluster == CLUSTER_NEXT:
        with _next_cluster_environment():
            client = OpenSearchClient.from_environment(ensure_index=ensure_index)
    else:
        client = OpenSearchClient.from_environment(ensure_index=ensure_index)
    if announce:
        click.echo(f"Target cluster: {cluster} ({_cluster_host(cluster)})")
    return client


def _cluster_host(cluster: str) -> str | None:
    """Return the host a ``--cluster`` choice resolves to, for logging.

    Read from the cluster's own variable rather than ``OPENSEARCH_HOST``, because
    ``_next_cluster_environment`` has already restored the live value by the time
    a command reports where it is pointed.
    """
    if cluster == CLUSTER_NEXT:
        return os.environ.get("OPENSEARCH_NEXT_HOST")
    return os.environ.get("OPENSEARCH_HOST")


def cluster_option(command):
    """Attach the shared ``--cluster`` option to a command."""
    return click.option(
        "--cluster",
        type=click.Choice([CLUSTER_LIVE, CLUSTER_NEXT]),
        default=CLUSTER_LIVE,
        show_default=True,
        help=(
            "Which OpenSearch cluster to operate on. 'next' targets the "
            "replacement cluster bound as OPENSEARCH_NEXT_SERVICE_NAME, leaving "
            "the live cluster completely untouched."
        ),
    )(command)


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
    """Normalize equivalent mapping representations returned by OpenSearch."""
    if isinstance(value, dict):
        normalized = {
            key: _normalize_mapping_for_comparison(item) for key, item in value.items()
        }
        # The application mapping declares `dynamic` as a Python bool, but
        # OpenSearch stores and returns it as the string "false"/"true". Compare
        # both as the string form so an equivalent mapping is not read as a
        # mismatch.
        if isinstance(normalized.get("dynamic"), bool):
            normalized["dynamic"] = str(normalized["dynamic"]).lower()
        if normalized.get("search_analyzer") is not None and normalized.get(
            "search_analyzer"
        ) == normalized.get("analyzer"):
            normalized.pop("search_analyzer")
        return normalized

    if isinstance(value, list):
        return [_normalize_mapping_for_comparison(item) for item in value]

    return value


@search.cli.command("reset-mapping")
def reset_opensearch_mapping():
    """Delete the dataset index and recreate its empty mapping and settings.

    Deliberately has no ``--cluster`` option: it empties the index it runs
    against, and the only reason to touch a replacement cluster is to *fill* it.
    Use ``rebuild-index --cluster next`` for that, which creates the mapping as
    part of the rebuild.
    """
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
@click.option(
    "--fail-on-discrepancy",
    is_flag=True,
    help=(
        "Exit non-zero when anything is missing, extra, or stale. Without this the "
        "command only reports, so an automated gate must pass it to actually gate."
    ),
)
@cluster_option
def compare_opensearch(
    sample_size: int,
    update: bool,
    force_update: bool,
    fail_on_discrepancy: bool,
    cluster: str,
):
    """Report and optionally repair DB/OpenSearch dataset discrepancies."""
    os_client = _client_for_cluster(cluster, announce=True)
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

    # Raise before any repair: a CI gate wants the verification verdict, and a run
    # that both repaired and reported success would hide the fact that the index was
    # wrong. Checked even with --update so `compare --update --fail-on-discrepancy`
    # still surfaces anything the repair could not fix.
    if fail_on_discrepancy and (missing or extra or updated_details):
        raise click.ClickException(
            f"Discrepancies found: {len(missing)} missing, {len(extra)} extra, "
            f"{len(updated_details)} updated."
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


def _clear_datasets_index(client, index_name: str):
    """Remove whatever currently answers to ``index_name``.

    A fresh replacement has no index because ``rebuild-index`` suppresses the
    client's create-if-missing behavior. A retry can have a partial plain index,
    while a cluster rebuilt by an older release can have an *alias* pointing at
    a ``datasets-<suffix>`` index. ``indices.delete`` rejects an alias with
    ``illegal_argument_exception``, so drop the alias and the indices behind it
    and leave every starting state equally clean.
    """
    if client.client.indices.exists_alias(name=index_name):
        aliased = sorted(client.client.indices.get_alias(name=index_name))
        click.echo(
            f"'{index_name}' is a leftover alias for {', '.join(aliased)}; "
            "removing both so it becomes a plain index."
        )
        # One atomic request: dropping an aliased index removes its alias with it,
        # so this never leaves the name pointing at something already deleted.
        client.client.indices.delete(index=",".join(aliased))
        return

    if client.client.indices.exists(index=index_name):
        click.echo(f"Deleting existing index {index_name}...")
        client.client.indices.delete(index=index_name)


def _create_rebuild_index(client, target_index: str, body: dict):
    """Create the index, tolerating a timed-out-but-successful attempt.

    ``indices.create`` waits for shards to become active before responding, which
    can outlast the client's 60s socket timeout on a busy cluster. The client then
    retries (``retry_on_timeout=True``), but the first attempt already created the
    index server-side, so the retry fails with ``resource_already_exists_exception``
    and the whole rebuild aborts -- leaving an orphaned empty index behind.

    Passing an explicit ``request_timeout`` gives the call room to finish, and
    treating "already exists" as success makes the retry idempotent. The caller has
    already deleted any pre-existing ``target_index``, so an existing index here
    can only be this command's own timed-out attempt.
    """
    try:
        client.client.indices.create(
            index=target_index,
            body=body,
            request_timeout=OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS,
        )
    except RequestError as error:
        if error.error != "resource_already_exists_exception":
            raise
        click.echo(
            f"  {target_index} already exists after a timed-out create; "
            "treating the earlier attempt as successful."
        )


def _rejection_details(item: dict) -> tuple[str, str, str]:
    """Pull ``(doc_id, error_type, reason)`` out of a bulk-rejection item."""
    action = next(iter(item.values()), {}) if isinstance(item, dict) else {}
    error = action.get("error") or {}
    if isinstance(error, str):
        return str(action.get("_id", "?")), "error", error
    reason = error.get("reason", "")
    caused_by = error.get("caused_by") or {}
    if caused_by.get("reason"):
        reason = f"{reason}: {caused_by['reason']}"
    return (
        str(action.get("_id", "?")),
        str(error.get("type", "unknown")),
        str(reason),
    )


def _report_skipped_documents(errors: list):
    """Print every skipped dataset id so an admin can investigate each record.

    Emitted as a block at the end of the run, grouped by error type, because the
    per-batch lines scroll far out of view during a multi-hundred-thousand
    document backfill. The ids are printed in full -- never truncated with an
    ellipsis -- since chasing the source record is the whole point.
    """
    grouped: dict[tuple[str, str], list[str]] = {}
    for item in errors:
        doc_id, error_type, reason = _rejection_details(item)
        grouped.setdefault((error_type, reason), []).append(doc_id)

    click.echo("")
    click.echo(f"{OPENSEARCH_SKIPPED_DOCUMENTS_BANNER} ({len(errors)} total)")
    for (error_type, reason), doc_ids in sorted(grouped.items()):
        click.echo(f"  {error_type}: {reason}")
        click.echo(f"    {len(doc_ids)} dataset id(s):")
        for doc_id in doc_ids:
            click.echo(f"      {doc_id}")
    click.echo(
        "  Investigate each id with: "
        "flask search compare --sample-size 50  (or query the dataset table "
        "directly by id)"
    )
    click.echo("")


def _backfill_from_postgres(
    client, target_index: str, batch_size: int, max_skipped: int = 0
):
    """Regenerate every dataset document from PostgreSQL into ``target_index``.

    PostgreSQL is the source of truth, so we rebuild documents with the same
    ``DatasetDocument`` transformer the writer uses in production and simply
    redirect ``_index`` to the new physical index. This (unlike a server-side
    OpenSearch ``_reindex``) picks up document-shape changes as well as mapping
    changes, and mirrors the ``compare --force-update`` repair path. Datasets are
    read in keyset-paginated batches to bound memory on large tables.

    A single malformed upstream record should not discard a whole rebuild. Up to
    ``max_skipped`` documents that OpenSearch *rejects individually* are logged
    and skipped; exceeding that budget aborts, because a large rejection count
    means something systemic (bad mapping, cluster trouble) rather than dirty
    source data. Every skipped id is reported so an admin can chase the record.

    Returns ``(indexed, failed, errors)`` where ``failed`` counts skipped
    documents and ``errors`` holds their raw rejection items.
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
                continue

            total_failed += 1
            errors.append(item)
            doc_id, error_type, reason = _rejection_details(item)
            click.echo(f"  Skipping {doc_id}: {error_type}: {reason}")

        last_id = datasets[-1].id
        db_interface.db.expunge_all()

        if total_failed > max_skipped:
            break

    return total_indexed, total_failed, errors


@search.cli.command("rebuild-index")
@click.option(
    "--batch-size",
    default=1000,
    show_default=True,
    type=click.IntRange(min=1),
    help="Number of datasets read from PostgreSQL per backfill batch.",
)
@click.option(
    "--max-skipped",
    default=10,
    show_default=True,
    type=click.IntRange(min=0),
    help=(
        "How many individually-rejected documents to skip (with their ids "
        "reported) before aborting the rebuild. Use 0 to fail on the first "
        "rejection."
    ),
)
@cluster_option
def rebuild_opensearch_index(
    batch_size: int,
    max_skipped: int,
    cluster: str,
):
    """Rebuild the ``datasets`` index from PostgreSQL on a replacement cluster.

    Recreates the ``datasets`` index with the current application mapping and
    regenerates every document from PostgreSQL (the source of truth) into it, then
    validates the document count.

    Backfilling from PostgreSQL (rather than an OpenSearch ``_reindex``) means the
    rebuild also picks up document-shape changes, not just mapping changes, and
    reuses the same repair path as ``compare --force-update``.

    This is destructive to the index it runs against, so run it with
    ``--cluster next`` against a freshly provisioned replacement cluster and cut
    over afterwards -- the live cluster is then never touched and serves queries at
    full speed throughout. ``--cluster live`` rebuilds in place and search returns
    nothing until the backfill finishes. See
    docs/ops/migrate-opensearch-cluster.md.
    """
    target_index = OpenSearchClient.INDEX_NAME
    client = _client_for_cluster(cluster, announce=True, ensure_index=False)
    _clear_datasets_index(client, target_index)

    click.echo(f"Creating index {target_index} with current mapping...")
    body = {"mappings": client.MAPPINGS}
    if client.SETTINGS:
        body["settings"] = client.SETTINGS
    _create_rebuild_index(client, target_index, body)

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
    indexed, failed, errors = _backfill_from_postgres(
        client, target_index, batch_size, max_skipped=max_skipped
    )
    if failed:
        _report_skipped_documents(errors)
    if failed > max_skipped:
        raise click.ClickException(
            f"Backfill {OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}: "
            f"indexed={indexed}, failed={failed}, which exceeds "
            f"--max-skipped={max_skipped}."
        )

    # Harvesting is paused and drained before this runs, so the DB is stable, so
    # the index must hold one document per dataset -- minus any we deliberately
    # skipped above. Keeping the skipped count in the expectation is what makes
    # tolerant skipping safe: a document lost for any *other* reason still fails
    # this check.
    client.client.indices.refresh(index=target_index)
    target_count = client.client.count(index=target_index)["count"]
    expected_count = db_count - failed
    if target_count != expected_count:
        raise click.ClickException(
            f"Validation failed ({OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}): "
            f"PostgreSQL has {db_count} dataset(s) and {failed} were skipped, so "
            f"{target_index} should have {expected_count} document(s) but has "
            f"{target_count}."
        )
    if failed:
        click.echo(
            f"Validated {target_index}: {target_count} document(s) "
            f"({failed} skipped)."
        )
    else:
        click.echo(f"Validated {target_index}: {target_count} document(s).")

    click.echo(f"Rebuild complete: {target_index} is ready on the {cluster} cluster.")


def _delete_physical_index(client, index_name: str):
    """Delete a leftover ``datasets-*`` index.

    The name must carry a suffix, which is what keeps the live ``datasets`` index
    itself un-deletable: rebuilds write to ``datasets`` directly, so deleting that
    name would take search down rather than reclaim disk.
    """
    index_prefix = client.INDEX_NAME

    suffixed_index_pattern = rf"{re.escape(index_prefix)}-[a-z0-9._-]+"
    if not re.fullmatch(suffixed_index_pattern, index_name):
        raise click.ClickException(
            f"Index name must start with '{index_prefix}-'. The live "
            f"'{index_prefix}' index cannot be deleted this way."
        )
    if not client.client.indices.exists(index=index_name):
        raise click.ClickException(f"OpenSearch index does not exist: {index_name}")

    click.echo(f"Deleting unused index {index_name}...")
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
    help="Exact name of a leftover index, such as datasets-20260723152900.",
)
@cluster_option
def delete_opensearch_index(index_name: str, cluster: str):
    """Delete a leftover ``datasets-*`` index."""
    client = _client_for_cluster(cluster, announce=True)
    _delete_physical_index(client, index_name)
