import logging
import os
from datetime import datetime
from unittest.mock import Mock, patch

import click
import pytest
from opensearchpy.exceptions import RequestError

from app.commands.search import (
    OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS,
    OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE,
    OPENSEARCH_SKIPPED_DOCUMENTS_BANNER,
    _is_aws_opensearch_host,
    _next_cluster_environment,
    db_interface,
)


def test_reset_mapping_recreates_empty_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "datagov_text",
                "search_analyzer": "datagov_text",
            }
        }
    }
    client.client.indices.get_mapping.return_value = {
        "datasets": {
            "mappings": {
                "properties": {"title": {"type": "text", "analyzer": "datagov_text"}}
            }
        }
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    client.client.indices.delete.assert_called_once_with(index="datasets")
    client._ensure_index.assert_called_once_with()
    client.index_datasets.assert_not_called()
    assert "Mapping reset successfully. The index is empty." in result.output


def test_reset_mapping_accepts_stringified_dynamic_flag(app):
    """OpenSearch echoes `dynamic` back as the string "false", not a bool."""
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {
        "properties": {
            "dcat": {
                "type": "nested",
                "dynamic": False,
                "properties": {"modified": {"type": "keyword"}},
            }
        }
    }
    client.client.indices.get_mapping.return_value = {
        "datasets": {
            "mappings": {
                "properties": {
                    "dcat": {
                        "type": "nested",
                        "dynamic": "false",
                        "properties": {"modified": {"type": "keyword"}},
                    }
                }
            }
        }
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    assert "Mapping reset successfully. The index is empty." in result.output


def test_reset_mapping_rejects_real_mapping_mismatch(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {"properties": {"title": {"type": "text"}}}
    client.client.indices.get_mapping.return_value = {
        "datasets": {"mappings": {"properties": {"title": {"type": "keyword"}}}}
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code != 0
    assert "Created index mapping does not match application mapping." in result.output


def test_compare_update_indexes_missing_and_deletes_extra(app, caplog):
    os_client = Mock()
    os_client.INDEX_NAME = "datasets"
    writer = Mock()
    writer.client = Mock()
    writer.index_dataset_batches.return_value = None

    missing_dataset = Mock()
    missing_dataset.id = "db-only"
    rows_query = Mock()
    rows_query.all.return_value = [("db-only", datetime(2024, 1, 1))]
    dataset_query = Mock()
    dataset_query.filter.return_value.all.return_value = [missing_dataset]

    def query_side_effect(*columns):
        if len(columns) == 2:
            return rows_query
        return dataset_query

    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=os_client,
        ),
        patch(
            "app.commands.search.OpenSearchWriter",
            return_value=writer,
        ),
        patch(
            "app.commands.search.db_interface.db.query", side_effect=query_side_effect
        ),
        patch(
            "app.commands.search.OpenSearchReader.scan_index",
            return_value=iter(
                [{"_id": "extra-only", "fields": {"last_harvested_date": []}}]
            ),
        ),
    ):
        result = app.test_cli_runner().invoke(args=["search", "compare", "--update"])

    assert result.exit_code == 0
    writer.index_dataset_batches.assert_called_once_with(
        ["db-only"],
        "Indexing 1 missing datasets...",
        db_interface,
        sample_size=10,
        log_all_errors=True,
    )
    writer.client.delete.assert_called_once_with(index="datasets", id="extra-only")
    writer._refresh.assert_called_once_with()


def test_compare_update_uses_index_batch_failure_message_constant(app, caplog):
    os_client = Mock()
    os_client.INDEX_NAME = "datasets"
    writer = Mock()

    def log_index_failure(*args, **kwargs):
        logging.info(f"1 dataset(s) {OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}.")

    writer.index_dataset_batches.side_effect = log_index_failure

    missing_dataset = Mock()
    missing_dataset.id = "db-only"
    rows_query = Mock()
    rows_query.all.return_value = [("db-only", datetime(2024, 1, 1))]
    dataset_query = Mock()
    dataset_query.filter.return_value.all.return_value = [missing_dataset]

    def query_side_effect(*columns):
        if len(columns) == 2:
            return rows_query
        return dataset_query

    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=os_client,
        ),
        patch(
            "app.commands.search.OpenSearchWriter",
            return_value=writer,
        ),
        patch(
            "app.commands.search.db_interface.db.query", side_effect=query_side_effect
        ),
        patch("app.commands.search.OpenSearchReader.scan_index", return_value=iter([])),
    ):
        result = app.test_cli_runner().invoke(args=["search", "compare", "--update"])

    assert result.exit_code == 0
    assert f"1 dataset(s) {OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE}." in caplog.text


def test_compare_is_read_only_without_update(app):
    client = Mock()
    client.INDEX_NAME = "datasets"

    rows_query = Mock()
    rows_query.all.return_value = []

    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.db_interface.db.query", return_value=rows_query),
        patch("app.commands.search.OpenSearchReader.scan_index", return_value=iter([])),
    ):
        result = app.test_cli_runner().invoke(args=["search", "compare"])

    assert result.exit_code == 0
    client.index_datasets.assert_not_called()
    client.client.delete.assert_not_called()
    client._refresh.assert_not_called()


def _run_compare(app, args, db_rows=(), os_hits=()):
    """Invoke compare with the DB and OpenSearch id sets stubbed."""
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client = Mock()

    rows_query = Mock()
    rows_query.all.return_value = list(db_rows)
    dataset_query = Mock()
    dataset_query.filter.return_value.all.return_value = []

    def query_side_effect(*columns):
        return rows_query if len(columns) == 2 else dataset_query

    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
        patch(
            "app.commands.search.db_interface.db.query", side_effect=query_side_effect
        ),
        patch(
            "app.commands.search.OpenSearchReader.scan_index",
            return_value=iter(list(os_hits)),
        ),
        patch("app.commands.search.OpenSearchWriter", return_value=client),
    ):
        return app.test_cli_runner().invoke(args=["search", "compare", *args])


# One dataset in the DB that is not indexed, and one indexed document with no
# dataset -- i.e. 1 missing and 1 extra.
_MISMATCH = {
    "db_rows": [("db-only", datetime(2024, 1, 1))],
    "os_hits": [{"_id": "extra-only", "fields": {"last_harvested_date": []}}],
}


def test_compare_reports_but_does_not_fail_by_default(app):
    """The default stays a report, so the nightly sync and manual runs are unchanged."""
    result = _run_compare(app, [], **_MISMATCH)

    assert result.exit_code == 0
    assert "Missing in OpenSearch (should be indexed): 1" in result.output


def test_compare_fails_on_discrepancy_when_asked(app):
    """Without this flag `compare` cannot gate anything: it prints the counts and
    exits 0, so a CI step that treats it as a verification gate would pass an index
    that is missing documents."""
    result = _run_compare(app, ["--fail-on-discrepancy"], **_MISMATCH)

    assert result.exit_code != 0
    assert "Discrepancies found: 1 missing, 1 extra, 0 updated." in result.output


def test_compare_succeeds_with_the_flag_when_in_sync(app):
    result = _run_compare(app, ["--fail-on-discrepancy"])

    assert result.exit_code == 0
    assert "Missing in OpenSearch (should be indexed): 0" in result.output


def test_compare_fails_on_stale_documents_too(app):
    """A stale last_harvested_date is a discrepancy even though the id set matches --
    which is exactly what a slug edit during a migration window looks like."""
    result = _run_compare(
        app,
        ["--fail-on-discrepancy"],
        db_rows=[("shared", datetime(2024, 2, 1))],
        os_hits=[
            {
                "_id": "shared",
                "fields": {"last_harvested_date": ["2024-01-01T00:00:00"]},
            }
        ],
    )

    assert result.exit_code != 0
    assert "1 updated" in result.output


def test_compare_fails_before_repairing_when_both_flags_are_given(app):
    """The verification verdict must survive a repair, so the raise comes first."""
    result = _run_compare(app, ["--update", "--fail-on-discrepancy"], **_MISMATCH)

    assert result.exit_code != 0
    assert "Updating discrepancies..." not in result.output


def _rebuild_client(existing_index=True, aliased_indices=None, target_count=5):
    """Build a mocked OpenSearchClient for rebuild-index tests.

    ``indices.exists`` is stateful: rebuild-index checks for ``datasets`` before
    deleting it, then recreates it. ``created`` tracks that transition.
    ``existing_index`` seeds whether ``datasets`` is already present, as it can
    be on a retry or live-cluster rebuild. A fresh replacement starts without it
    because ``rebuild-index`` suppresses the client's create-if-missing behavior.
    ``aliased_indices`` instead makes ``datasets`` an *alias* over those indices,
    the state a cluster rebuilt by an older release is left in; ``indices.delete``
    then rejects the bare name the way OpenSearch really does.
    ``target_count`` is what ``count()`` reports for the post-backfill validation.

    The mapping includes a ``dynamic`` flag declared as a Python bool, and
    ``get_mapping`` echoes it back as OpenSearch really does -- the string
    ``"false"`` -- so the round-trip comparison is exercised rather than assumed.
    """
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {
        "properties": {
            "title": {"type": "text"},
            "dcat": {"type": "nested", "dynamic": False},
        }
    }
    client.SETTINGS = {"analysis": {}}

    aliases = set(aliased_indices or [])
    created = set(aliases)
    if existing_index and not aliases:
        created.add("datasets")

    def exists(index):
        return index in created or (index == "datasets" and bool(aliases))

    def create(index, body, request_timeout=None):
        created.add(index)
        return {"acknowledged": True}

    def delete(index):
        names = index.split(",")
        # OpenSearch refuses to delete an alias by name; the caller must name the
        # concrete indices behind it. Reproduce that rather than assuming it.
        if "datasets" in names and aliases:
            raise RequestError(
                400,
                "illegal_argument_exception",
                {
                    "error": {
                        "reason": (
                            "The provided expression [datasets] matches an alias, "
                            "specify the corresponding concrete indices instead."
                        )
                    },
                    "status": 400,
                },
            )
        for name in names:
            created.discard(name)
            aliases.discard(name)
        return {"acknowledged": True}

    client.client.indices.exists.side_effect = exists
    client.client.indices.create.side_effect = create
    client.client.indices.delete.side_effect = delete
    client.client.indices.exists_alias.side_effect = lambda name: bool(aliases)
    client.client.indices.get_alias.side_effect = lambda name: {
        index: {"aliases": {"datasets": {}}} for index in sorted(aliases)
    }
    client.client.indices.get_mapping.side_effect = lambda index: {
        index: {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "dcat": {"type": "nested", "dynamic": "false"},
                }
            }
        }
    }
    client.client.count.return_value = {"count": target_count}
    return client


def _run_rebuild(
    app,
    client,
    args,
    db_count=5,
    backfill=None,
    expected_ensure_index=None,
):
    """Invoke rebuild-index with the PostgreSQL backfill path mocked out.

    ``backfill`` overrides the (indexed, failed, errors) return of
    ``_backfill_from_postgres``; by default it reports ``db_count`` indexed with
    no failures. A single query mock answers the two ``.count()`` calls the
    command makes before and after the backfill.
    """
    if backfill is None:
        backfill = (db_count, 0, [])
    query_result = Mock()
    query_result.count.return_value = db_count
    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ) as client_factory,
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch(
            "app.commands.search._backfill_from_postgres", return_value=backfill
        ) as backfill_mock,
    ):
        result = app.test_cli_runner().invoke(args=["search", "rebuild-index", *args])
    if expected_ensure_index is not None:
        client_factory.assert_called_once_with(ensure_index=expected_ensure_index)
    return result, backfill_mock


def test_rebuild_index_recreates_datasets_and_backfills(app):
    client = _rebuild_client()

    result, backfill_mock = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    # The pre-existing index is dropped, then recreated with the current mapping.
    client.client.indices.delete.assert_called_once_with(index="datasets")
    create_kwargs = client.client.indices.create.call_args.kwargs
    assert create_kwargs["index"] == "datasets"
    # Backfill targets `datasets` itself, sourced from PostgreSQL.
    backfill_mock.assert_called_once()
    assert backfill_mock.call_args.args[1] == "datasets"
    assert "Rebuild complete: datasets is ready on the live cluster." in result.output


def test_rebuild_index_creates_datasets_when_absent(app):
    """A cluster with no `datasets` index yet needs no delete."""
    client = _rebuild_client(existing_index=False)

    result, _ = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    client.client.indices.delete.assert_not_called()
    assert client.client.indices.create.call_args.kwargs["index"] == "datasets"


def test_rebuild_index_replaces_a_leftover_alias(app):
    """A cluster last rebuilt by an older release has `datasets` as an alias.

    Reproduces the development failure of 2026-07-29: `indices.delete("datasets")`
    returned ``illegal_argument_exception`` because the name matched an alias, and
    the rebuild aborted. Both dev clusters -- and staging/prod after any earlier
    rebuild -- start in exactly this state.
    """
    client = _rebuild_client(aliased_indices=["datasets-30503327609-1"])

    result, backfill_mock = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    # The concrete index behind the alias is what gets deleted, by name.
    client.client.indices.delete.assert_called_once_with(index="datasets-30503327609-1")
    assert "leftover alias" in result.output
    # `datasets` is then recreated as a plain index and backfilled.
    assert client.client.indices.create.call_args.kwargs["index"] == "datasets"
    assert backfill_mock.call_args.args[1] == "datasets"


def test_rebuild_index_deletes_every_index_behind_a_multi_index_alias(app):
    """An alias spanning several indices must not leave any of them behind."""
    client = _rebuild_client(aliased_indices=["datasets-one", "datasets-two"])

    result, _ = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    # One atomic request naming both, so the alias never outlives its indices.
    client.client.indices.delete.assert_called_once_with(
        index="datasets-one,datasets-two"
    )


def test_rebuild_index_aborts_on_count_mismatch(app):
    # PostgreSQL has 5 datasets but only 4 land in the index.
    client = _rebuild_client(target_count=4)

    result, _ = _run_rebuild(app, client, [], db_count=5)

    assert result.exit_code != 0
    assert OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE in result.output


def test_rebuild_index_aborts_when_skipped_exceeds_budget(app):
    client = _rebuild_client()

    result, _ = _run_rebuild(
        app,
        client,
        ["--max-skipped", "0"],
        db_count=5,
        backfill=(4, 1, [{"index": {"_id": "doomed", "error": "boom"}}]),
    )

    assert result.exit_code != 0
    assert OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE in result.output
    # Even on abort, the ids must be reported so they can be investigated.
    assert "doomed" in result.output


def test_rebuild_index_skips_rejected_document_and_completes(app):
    """One malformed record must not discard the whole rebuild.

    Mirrors the staging failure of 2026-07-29: a single dataset carrying an
    empty-string JSON key was rejected by OpenSearch, and the all-or-nothing
    backfill threw away 397,999 successfully indexed documents.
    """
    # 4 of 5 datasets land; the rejected one is skipped, so the index holds 4.
    client = _rebuild_client(target_count=4)
    rejection = {
        "index": {
            "_id": "ba35e626-c015-4c15-819f-892ce8e6baa9",
            "error": {
                "type": "mapper_parsing_exception",
                "reason": "failed to parse",
                "caused_by": {"reason": "field name cannot be an empty string"},
            },
        }
    }

    result, _ = _run_rebuild(
        app,
        client,
        [],
        db_count=5,
        backfill=(4, 1, [rejection]),
    )

    assert result.exit_code == 0, result.output
    # The rebuild runs to completion rather than discarding the indexed documents.
    assert "Rebuild complete" in result.output
    # The id, the error, and the reason are all reported for follow-up.
    assert OPENSEARCH_SKIPPED_DOCUMENTS_BANNER in result.output
    assert "ba35e626-c015-4c15-819f-892ce8e6baa9" in result.output
    assert "mapper_parsing_exception" in result.output
    assert "field name cannot be an empty string" in result.output
    assert "1 skipped" in result.output


def test_rebuild_index_reports_every_skipped_id_without_truncating(app):
    client = _rebuild_client(target_count=94)
    rejections = [
        {
            "index": {
                "_id": f"dataset-{n:03d}",
                "error": {"type": "mapper_parsing_exception", "reason": "failed"},
            }
        }
        for n in range(6)
    ]

    result, _ = _run_rebuild(
        app,
        client,
        [],
        db_count=100,
        backfill=(94, 6, rejections),
    )

    assert result.exit_code == 0, result.output
    for n in range(6):
        assert f"dataset-{n:03d}" in result.output


def test_rebuild_index_validation_accounts_for_skipped_documents(app):
    """A document missing for any reason *other* than a skip must still fail."""
    # 1 skipped out of 5 means 4 expected, but the index only has 3.
    client = _rebuild_client(target_count=3)

    result, _ = _run_rebuild(
        app,
        client,
        [],
        db_count=5,
        backfill=(4, 1, [{"index": {"_id": "skipped-one", "error": "boom"}}]),
    )

    assert result.exit_code != 0
    assert "should have 4 document(s) but has 3" in result.output


def test_rebuild_index_creates_with_extended_request_timeout(app):
    # indices.create waits for shards to become active, which can outlast the
    # client's default 60s socket timeout on a loaded cluster.
    client = _rebuild_client()

    result, _ = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    create_kwargs = client.client.indices.create.call_args.kwargs
    assert create_kwargs["request_timeout"] == OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS


def test_rebuild_index_survives_already_exists_after_timed_out_create(app):
    """A create that times out client-side but succeeded server-side must not abort.

    Reproduces the staging failure of 2026-07-28: the first PUT hit the 60s socket
    timeout, opensearch-py retried, and the retry got
    ``resource_already_exists_exception`` because the original request had in fact
    created the index. The rebuild aborted and left an orphaned empty index.
    """
    client = _rebuild_client()
    # The timed-out first attempt did create the index server-side, so record it
    # as existing before raising the error the retry actually received.
    original_create = client.client.indices.create.side_effect

    def create_then_conflict(index, body, request_timeout=None):
        original_create(index, body)
        raise RequestError(
            400,
            "resource_already_exists_exception",
            {"error": {"index": index}, "status": 400},
        )

    client.client.indices.create.side_effect = create_then_conflict

    result, backfill_mock = _run_rebuild(app, client, [])

    assert result.exit_code == 0, result.output
    assert "treating the earlier attempt as successful" in result.output
    # The rebuild must carry on to the backfill rather than aborting.
    backfill_mock.assert_called_once()


def test_rebuild_index_still_aborts_on_other_create_errors(app):
    client = _rebuild_client()
    client.client.indices.create.side_effect = RequestError(
        400,
        "invalid_index_name_exception",
        {"error": {"reason": "bad name"}, "status": 400},
    )

    result, backfill_mock = _run_rebuild(app, client, [])

    assert result.exit_code != 0
    backfill_mock.assert_not_called()


def test_backfill_from_postgres_overrides_index_and_tallies_failures(app):
    from app.commands.search import _backfill_from_postgres

    client = Mock()
    first, second = Mock(), Mock()
    first.id, second.id = "a", "b"

    # Keyset pagination: first batch returns two datasets, second is empty.
    chain = Mock()
    chain.filter.return_value = chain
    chain.limit.return_value.all.side_effect = [[first, second], []]
    query_result = Mock()
    query_result.order_by.return_value = chain

    def fake_streaming_bulk(_client, documents, **_kwargs):
        # First doc succeeds, second fails; both should target the new index.
        assert all(doc["_index"] == "datasets-new" for doc in documents)
        yield True, {"index": {"_id": "a"}}
        yield False, {"index": {"_id": "b", "error": "boom"}}

    with (
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch("app.commands.search.DatasetDocument") as document_cls,
        patch(
            "app.commands.search.helpers.streaming_bulk",
            side_effect=fake_streaming_bulk,
        ),
    ):
        document_cls.side_effect = lambda dataset: Mock(
            dataset_to_document=lambda: {"_index": "datasets", "_id": dataset.id}
        )
        indexed, failed, errors = _backfill_from_postgres(
            client, "datasets-new", batch_size=1000
        )

    assert indexed == 1
    assert failed == 1
    assert len(errors) == 1


def test_backfill_continues_past_rejection_within_budget(app):
    """With budget remaining, the backfill keeps paginating instead of breaking.

    This is the behavior the all-or-nothing version lacked: batch 2 must still be
    read and indexed after batch 1 had a rejected document.
    """
    from app.commands.search import _backfill_from_postgres

    client = Mock()

    def dataset(name):
        d = Mock()
        d.id = name
        return d

    chain = Mock()
    chain.filter.return_value = chain
    # Three pages: the first has a bad doc, the second is clean, then exhausted.
    chain.limit.return_value.all.side_effect = [
        [dataset("a"), dataset("bad")],
        [dataset("c")],
        [],
    ]
    query_result = Mock()
    query_result.order_by.return_value = chain

    def fake_streaming_bulk(_client, documents, **_kwargs):
        for doc in documents:
            if doc["_id"] == "bad":
                yield (
                    False,
                    {
                        "index": {
                            "_id": "bad",
                            "error": {
                                "type": "mapper_parsing_exception",
                                "reason": "failed to parse",
                            },
                        }
                    },
                )
            else:
                yield True, {"index": {"_id": doc["_id"]}}

    with (
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch("app.commands.search.DatasetDocument") as document_cls,
        patch(
            "app.commands.search.helpers.streaming_bulk",
            side_effect=fake_streaming_bulk,
        ),
    ):
        document_cls.side_effect = lambda dataset: Mock(
            dataset_to_document=lambda: {"_index": "datasets", "_id": dataset.id}
        )
        indexed, failed, errors = _backfill_from_postgres(
            client, "datasets-new", batch_size=2, max_skipped=10
        )

    # "c" from the second page proves pagination continued past the rejection.
    assert indexed == 2
    assert failed == 1
    assert len(errors) == 1


def test_backfill_stops_once_skip_budget_is_exhausted(app):
    from app.commands.search import _backfill_from_postgres

    client = Mock()

    def dataset(name):
        d = Mock()
        d.id = name
        return d

    chain = Mock()
    chain.filter.return_value = chain
    pages = [[dataset("bad1")], [dataset("bad2")], [dataset("never-read")], []]
    chain.limit.return_value.all.side_effect = pages
    query_result = Mock()
    query_result.order_by.return_value = chain

    def fake_streaming_bulk(_client, documents, **_kwargs):
        for doc in documents:
            yield False, {"index": {"_id": doc["_id"], "error": {"type": "bad_doc"}}}

    with (
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch("app.commands.search.DatasetDocument") as document_cls,
        patch(
            "app.commands.search.helpers.streaming_bulk",
            side_effect=fake_streaming_bulk,
        ),
    ):
        document_cls.side_effect = lambda dataset: Mock(
            dataset_to_document=lambda: {"_index": "datasets", "_id": dataset.id}
        )
        indexed, failed, errors = _backfill_from_postgres(
            client, "datasets-new", batch_size=1, max_skipped=1
        )

    # Budget of 1 tolerates the first rejection, aborts after the second.
    assert indexed == 0
    assert failed == 2
    assert [e["index"]["_id"] for e in errors] == ["bad1", "bad2"]


def test_delete_index_removes_leftover_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client.indices.exists.return_value = True
    client.client.indices.delete.return_value = {"acknowledged": True}

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets-old"]
        )

    assert result.exit_code == 0, result.output
    client.client.indices.delete.assert_called_once_with(index="datasets-old")


def test_delete_index_refuses_the_live_datasets_index(app):
    """The suffix requirement is the only thing protecting live search.

    Rebuilds write to `datasets` directly, so an unsuffixed name here would take
    search down rather than reclaim disk from a leftover.
    """
    client = Mock()
    client.INDEX_NAME = "datasets"

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets"]
        )

    assert result.exit_code != 0
    assert "cannot be deleted this way" in result.output
    client.client.indices.delete.assert_not_called()


# --- Cluster targeting -------------------------------------------------------
#
# A rebuild loads whichever cluster it runs against, so `--cluster next` exists
# to keep that load off the cluster serving live queries. These tests pin the
# credential swap, its restoration, and the fact that `live` is unchanged.


@pytest.fixture
def next_cluster_environment(monkeypatch):
    """Bind a replacement cluster the way .profile does, and a live one."""
    monkeypatch.setenv("OPENSEARCH_HOST", "live.example")
    monkeypatch.setenv("OPENSEARCH_ACCESS_KEY", "live-access")
    monkeypatch.setenv("OPENSEARCH_SECRET_KEY", "live-secret")
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "next.example")
    monkeypatch.setenv("OPENSEARCH_NEXT_ACCESS_KEY", "next-access")
    monkeypatch.setenv("OPENSEARCH_NEXT_SECRET_KEY", "next-secret")


@pytest.fixture
def aws_next_cluster_environment(monkeypatch, next_cluster_environment):
    """A replacement cluster on a host that selects the AWS SigV4 path."""
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "next.us-gov-west-1.es.amazonaws.com")


def test_next_cluster_environment_swaps_and_restores_credentials(
    next_cluster_environment,
):
    with _next_cluster_environment():
        # Inside the block the client's fixed env var names resolve to the
        # replacement cluster, which is what pins a constructed client to it.
        assert os.environ["OPENSEARCH_HOST"] == "next.example"
        assert os.environ["OPENSEARCH_ACCESS_KEY"] == "next-access"
        assert os.environ["OPENSEARCH_SECRET_KEY"] == "next-secret"

    assert os.environ["OPENSEARCH_HOST"] == "live.example"
    assert os.environ["OPENSEARCH_ACCESS_KEY"] == "live-access"
    assert os.environ["OPENSEARCH_SECRET_KEY"] == "live-secret"


def test_next_cluster_environment_restores_credentials_on_error(
    next_cluster_environment,
):
    """A failed rebuild must not leave the process pointed at the wrong cluster."""
    with pytest.raises(RuntimeError):
        with _next_cluster_environment():
            raise RuntimeError("backfill exploded")

    assert os.environ["OPENSEARCH_HOST"] == "live.example"
    assert os.environ["OPENSEARCH_ACCESS_KEY"] == "live-access"
    assert os.environ["OPENSEARCH_SECRET_KEY"] == "live-secret"


def test_next_cluster_refuses_when_it_resolves_to_the_live_host(monkeypatch):
    """The one state where `--cluster next` would silently hit production.

    Reachable in normal operation: once a replacement cluster has been adopted,
    OPENSEARCH_SERVICE_NAME and OPENSEARCH_NEXT_SERVICE_NAME both name it until
    the latter is unset. Every `--cluster next` command would then target live
    while reporting "next".
    """
    monkeypatch.setenv("OPENSEARCH_HOST", "same.example")
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "same.example")

    with pytest.raises(click.ClickException) as excinfo:
        with _next_cluster_environment():
            pytest.fail("must not yield when both names resolve to one host")

    assert "same host as the live cluster" in str(excinfo.value)
    assert "OPENSEARCH_NEXT_SERVICE_NAME" in str(excinfo.value)


def test_next_cluster_restores_when_interrupted_mid_swap(monkeypatch):
    """A signal landing partway through the swap must not leave a mixed set.

    Without entering the try before mutating, an interruption here strands the
    process on the replacement host holding the live cluster's secret key.
    """
    monkeypatch.setenv("OPENSEARCH_HOST", "live.example")
    monkeypatch.setenv("OPENSEARCH_ACCESS_KEY", "live-access")
    monkeypatch.setenv("OPENSEARCH_SECRET_KEY", "live-secret")
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "next.example")
    monkeypatch.setenv("OPENSEARCH_NEXT_ACCESS_KEY", "next-access")
    monkeypatch.setenv("OPENSEARCH_NEXT_SECRET_KEY", "next-secret")

    # Interrupt the swap once it has applied the host but not yet the secret key:
    # the state that would otherwise strand a mismatched credential pair. Patch
    # the type, not the instance -- `os.environ[k] = v` resolves __setitem__ on
    # os._Environ, so patching the instance attribute would never fire.
    real_setitem = type(os.environ).__setitem__

    def exploding_setitem(self, key, value):
        if key == "OPENSEARCH_SECRET_KEY" and value == "next-secret":
            raise KeyboardInterrupt("signal mid-swap")
        real_setitem(self, key, value)

    with patch.object(type(os.environ), "__setitem__", exploding_setitem):
        with pytest.raises(KeyboardInterrupt):
            with _next_cluster_environment():
                pytest.fail("should not reach the body")

    assert os.environ["OPENSEARCH_HOST"] == "live.example"
    assert os.environ["OPENSEARCH_ACCESS_KEY"] == "live-access"
    assert os.environ["OPENSEARCH_SECRET_KEY"] == "live-secret"


@pytest.mark.parametrize(
    ("host", "is_aws"),
    [
        ("vpc-x.us-gov-west-1.es.amazonaws.com", True),
        # Case, a trailing FQDN dot, and an explicit port must not smuggle a real
        # AWS endpoint past the credential requirement onto the admin:admin path.
        ("VPC-X.US-GOV-WEST-1.ES.AMAZONAWS.COM", True),
        ("vpc-x.us-gov-west-1.es.amazonaws.com.", True),
        ("vpc-x.us-gov-west-1.es.amazonaws.com:443", True),
        ("https://vpc-x.us-gov-west-1.es.amazonaws.com", True),
        ("es.amazonaws.com", True),
        ("evil.es.amazonaws.com.attacker.net", False),
        ("opensearch-next", False),
        # urlparse raises on this; it must not escape as a traceback.
        ("http://[::1", False),
        ("", False),
    ],
)
def test_is_aws_opensearch_host_classification(host, is_aws):
    assert _is_aws_opensearch_host(host) is is_aws


def test_next_cluster_environment_removes_vars_that_were_unset(monkeypatch):
    """Restoring must not invent a live host that was never set."""
    monkeypatch.delenv("OPENSEARCH_HOST", raising=False)
    monkeypatch.delenv("OPENSEARCH_ACCESS_KEY", raising=False)
    monkeypatch.delenv("OPENSEARCH_SECRET_KEY", raising=False)
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "next.example")
    monkeypatch.setenv("OPENSEARCH_NEXT_ACCESS_KEY", "next-access")
    monkeypatch.setenv("OPENSEARCH_NEXT_SECRET_KEY", "next-secret")

    with _next_cluster_environment():
        assert os.environ["OPENSEARCH_HOST"] == "next.example"

    assert "OPENSEARCH_HOST" not in os.environ
    assert "OPENSEARCH_ACCESS_KEY" not in os.environ
    assert "OPENSEARCH_SECRET_KEY" not in os.environ


@pytest.mark.parametrize(
    "missing",
    [
        "OPENSEARCH_NEXT_HOST",
        "OPENSEARCH_NEXT_ACCESS_KEY",
        "OPENSEARCH_NEXT_SECRET_KEY",
    ],
)
def test_rebuild_index_next_cluster_requires_its_credentials(
    app, monkeypatch, aws_next_cluster_environment, missing
):
    monkeypatch.delenv(missing, raising=False)
    client = _rebuild_client()

    result, backfill_mock = _run_rebuild(app, client, ["--cluster", "next"])

    assert result.exit_code != 0
    # The error has to name the variable and how to get it, since the operator
    # is reading this out of a cf task log.
    assert missing in result.output
    assert "OPENSEARCH_NEXT_SERVICE_NAME" in result.output
    # Nothing was created, deleted, or indexed anywhere.
    backfill_mock.assert_not_called()
    client.client.indices.create.assert_not_called()
    client.client.indices.delete.assert_not_called()


def test_next_cluster_allows_a_local_host_without_aws_keys(monkeypatch):
    """A local replacement node needs no keys; the client uses admin:admin there.

    Requiring them anyway would force dummy values just to exercise this path
    against docker-compose's opensearch-next node.
    """
    monkeypatch.setenv("OPENSEARCH_HOST", "opensearch")
    monkeypatch.setenv("OPENSEARCH_NEXT_HOST", "opensearch-next")
    monkeypatch.delenv("OPENSEARCH_NEXT_ACCESS_KEY", raising=False)
    monkeypatch.delenv("OPENSEARCH_NEXT_SECRET_KEY", raising=False)

    with _next_cluster_environment():
        assert os.environ["OPENSEARCH_HOST"] == "opensearch-next"

    assert os.environ["OPENSEARCH_HOST"] == "opensearch"


def test_rebuild_index_next_cluster_builds_client_against_replacement(
    app, next_cluster_environment
):
    """The whole rebuild must resolve the replacement cluster's credentials."""
    client = _rebuild_client()
    observed = {}

    def record_host(ensure_index=True):
        observed["ensure_index"] = ensure_index
        observed["host"] = os.environ["OPENSEARCH_HOST"]
        observed["access_key"] = os.environ["OPENSEARCH_ACCESS_KEY"]
        return client

    query_result = Mock()
    query_result.count.return_value = 5
    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            side_effect=record_host,
        ),
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch("app.commands.search._backfill_from_postgres", return_value=(5, 0, [])),
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "rebuild-index", "--cluster", "next"]
        )

    assert result.exit_code == 0, result.output
    assert observed == {
        "ensure_index": False,
        "host": "next.example",
        "access_key": "next-access",
    }
    # The log names the cluster actually loaded, not the live one it restored to.
    assert "Target cluster: next (next.example)" in result.output
    # Live credentials are back in place for anything that runs afterward.
    assert os.environ["OPENSEARCH_HOST"] == "live.example"


def test_rebuild_index_defaults_to_the_live_cluster(app, next_cluster_environment):
    """Omitting --cluster must target live, not the replacement cluster."""
    client = _rebuild_client()
    observed = {}

    def record_host(ensure_index=True):
        observed["ensure_index"] = ensure_index
        observed["host"] = os.environ["OPENSEARCH_HOST"]
        return client

    query_result = Mock()
    query_result.count.return_value = 5
    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            side_effect=record_host,
        ),
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch("app.commands.search._backfill_from_postgres", return_value=(5, 0, [])),
    ):
        result = app.test_cli_runner().invoke(args=["search", "rebuild-index"])

    assert result.exit_code == 0, result.output
    assert observed["host"] == "live.example"
    assert observed["ensure_index"] is False
    assert "Target cluster: live (live.example)" in result.output


def test_rebuild_index_on_fresh_cluster_creates_index_once(
    app, next_cluster_environment
):
    """A fresh replacement skips the constructor's redundant index creation."""
    client = _rebuild_client(existing_index=False)

    result, backfill_mock = _run_rebuild(
        app,
        client,
        ["--cluster", "next"],
        expected_ensure_index=False,
    )

    assert result.exit_code == 0, result.output
    client.client.indices.delete.assert_not_called()
    client.client.indices.create.assert_called_once()
    assert client.client.indices.create.call_args.kwargs["index"] == "datasets"
    backfill_mock.assert_called_once()
    assert "Rebuild complete: datasets is ready on the next cluster." in result.output


def test_compare_targets_the_replacement_cluster(app, next_cluster_environment):
    """`compare --cluster next` verifies the rebuild without touching live."""
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client = Mock()
    observed = {}

    def record_host(ensure_index=True):
        observed["host"] = os.environ["OPENSEARCH_HOST"]
        return client

    rows_query = Mock()
    rows_query.all.return_value = [("shared", datetime(2024, 1, 1))]

    with (
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            side_effect=record_host,
        ),
        patch("app.commands.search.OpenSearchWriter", return_value=client),
        patch("app.commands.search.db_interface.db.query", return_value=rows_query),
        patch(
            "app.commands.search.OpenSearchReader.scan_index",
            return_value=iter([]),
        ),
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "compare", "--cluster", "next"]
        )

    assert result.exit_code == 0, result.output
    assert observed["host"] == "next.example"


def test_delete_index_targets_the_replacement_cluster(app, next_cluster_environment):
    """Cleaning up a failed rebuild must be possible on the replacement cluster."""
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client.indices.exists.return_value = True
    client.client.indices.delete.return_value = {"acknowledged": True}
    observed = {}

    def record_host(ensure_index=True):
        observed["host"] = os.environ["OPENSEARCH_HOST"]
        return client

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        side_effect=record_host,
    ):
        result = app.test_cli_runner().invoke(
            args=[
                "search",
                "delete-index",
                "--index-name",
                "datasets-stale",
                "--cluster",
                "next",
            ]
        )

    assert result.exit_code == 0, result.output
    assert observed["host"] == "next.example"
    client.client.indices.delete.assert_called_once_with(index="datasets-stale")
