from datetime import datetime
from unittest.mock import Mock, patch

from opensearchpy.exceptions import RequestError

from app.commands.search import (
    OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS,
    OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE,
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
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client = Mock()
    client.index_dataset_batches.return_value = None

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
            "app.commands.search.OpenSearchWriter",
            return_value=client,
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
    client.index_dataset_batches.assert_called_once_with(
        ["db-only"],
        "Indexing 1 missing datasets...",
        db_interface,
        sample_size=10,
        log_all_errors=True,
    )
    client.client.delete.assert_called_once_with(index="datasets", id="extra-only")
    client._refresh.assert_called_once_with()


def test_compare_update_uses_index_batch_failure_message_constant(app, caplog):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client = Mock()
    client.index_datasets.return_value = (0, 1, ["index error"])

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
            "app.commands.search.OpenSearchWriter.index_datasets",
            return_value=(0, 1, ["index error"]),
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


def _rebuild_client(alias_indices=None, legacy_concrete=False, target_count=5):
    """Build a mocked OpenSearchClient for rebuild-index tests.

    ``indices.exists`` is stateful because rebuild-index queries it before the
    new index is created (expects False) and again during the alias switch
    (expects True). ``created`` tracks that transition. ``target_count`` is what
    ``count(target_index)`` reports for the post-backfill validation.

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

    created = set(alias_indices or [])
    if legacy_concrete:
        created.add("datasets")

    def exists(index):
        return index in created

    def create(index, body, request_timeout=None):
        created.add(index)
        return {"acknowledged": True}

    client.client.indices.exists.side_effect = exists
    client.client.indices.create.side_effect = create
    client.client.indices.exists_alias.return_value = bool(alias_indices)
    client.client.indices.get_alias.return_value = {
        index: {} for index in (alias_indices or [])
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
    client.client.indices.update_aliases.return_value = {"acknowledged": True}
    return client


def _run_rebuild(app, client, args, db_count=5, backfill=None):
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
        ),
        patch("app.commands.search.db_interface.db.query", return_value=query_result),
        patch(
            "app.commands.search._backfill_from_postgres", return_value=backfill
        ) as backfill_mock,
    ):
        result = app.test_cli_runner().invoke(args=["search", "rebuild-index", *args])
    return result, backfill_mock


def test_rebuild_index_backfills_and_switches_alias(app):
    client = _rebuild_client(alias_indices=["datasets-old"])

    result, backfill_mock = _run_rebuild(
        app, client, ["--target-index", "datasets-new"]
    )

    assert result.exit_code == 0, result.output
    create_kwargs = client.client.indices.create.call_args.kwargs
    assert create_kwargs["index"] == "datasets-new"
    # Backfill targets the new physical index, sourced from PostgreSQL.
    backfill_mock.assert_called_once()
    assert backfill_mock.call_args.args[1] == "datasets-new"

    client.client.indices.update_aliases.assert_called_once()
    actions = client.client.indices.update_aliases.call_args.kwargs["body"]["actions"]
    assert {"remove": {"index": "datasets-old", "alias": "datasets"}} in actions
    assert {"add": {"index": "datasets-new", "alias": "datasets"}} in actions
    assert "datasets now points to datasets-new" in result.output


def test_rebuild_index_aborts_before_alias_switch_on_count_mismatch(app):
    # PostgreSQL has 5 datasets but only 4 land in the new index.
    client = _rebuild_client(alias_indices=["datasets-old"], target_count=4)

    result, _ = _run_rebuild(
        app, client, ["--target-index", "datasets-new"], db_count=5
    )

    assert result.exit_code != 0
    assert OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE in result.output
    client.client.indices.update_aliases.assert_not_called()


def test_rebuild_index_aborts_on_backfill_failures(app):
    client = _rebuild_client(alias_indices=["datasets-old"])

    result, _ = _run_rebuild(
        app,
        client,
        ["--target-index", "datasets-new"],
        db_count=5,
        backfill=(4, 1, [{"index": {"error": "boom"}}]),
    )

    assert result.exit_code != 0
    assert OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE in result.output
    client.client.indices.update_aliases.assert_not_called()


def test_rebuild_index_requires_flag_to_convert_legacy_concrete_index(app):
    client = _rebuild_client(alias_indices=None, legacy_concrete=True)

    result, _ = _run_rebuild(app, client, ["--target-index", "datasets-new"])

    assert result.exit_code != 0
    assert "--allow-legacy-index-removal" in result.output
    client.client.indices.create.assert_not_called()


def test_rebuild_index_converts_legacy_concrete_index_with_flag(app):
    client = _rebuild_client(alias_indices=None, legacy_concrete=True)

    result, _ = _run_rebuild(
        app,
        client,
        ["--target-index", "datasets-new", "--allow-legacy-index-removal"],
    )

    assert result.exit_code == 0, result.output
    actions = client.client.indices.update_aliases.call_args.kwargs["body"]["actions"]
    assert {"remove_index": {"index": "datasets"}} in actions
    assert {"add": {"index": "datasets-new", "alias": "datasets"}} in actions


def test_rebuild_index_no_switch_alias_leaves_alias_untouched(app):
    client = _rebuild_client(alias_indices=["datasets-old"])

    result, backfill_mock = _run_rebuild(
        app, client, ["--target-index", "datasets-new", "--no-switch-alias"]
    )

    assert result.exit_code == 0, result.output
    backfill_mock.assert_called_once()
    client.client.indices.update_aliases.assert_not_called()


def test_rebuild_index_deletes_old_index_after_switch(app):
    client = _rebuild_client(alias_indices=["datasets-old"])
    # After the switch, delete-old-index re-checks that the old index is no
    # longer attached to any alias before deleting it.
    client.client.indices.delete.return_value = {"acknowledged": True}

    result, _ = _run_rebuild(
        app, client, ["--target-index", "datasets-new", "--delete-old-index"]
    )

    assert result.exit_code == 0, result.output
    client.client.indices.delete.assert_called_once_with(index="datasets-old")


def test_rebuild_index_creates_with_extended_request_timeout(app):
    # indices.create waits for shards to become active, which can outlast the
    # client's default 60s socket timeout on a loaded cluster.
    client = _rebuild_client(alias_indices=["datasets-old"])

    result, _ = _run_rebuild(app, client, ["--target-index", "datasets-new"])

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
    client = _rebuild_client(alias_indices=["datasets-old"])
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

    result, backfill_mock = _run_rebuild(
        app, client, ["--target-index", "datasets-new"]
    )

    assert result.exit_code == 0, result.output
    assert "treating the earlier attempt as successful" in result.output
    # The rebuild must carry on to backfill and swap the alias.
    backfill_mock.assert_called_once()
    client.client.indices.update_aliases.assert_called_once()


def test_rebuild_index_still_aborts_on_other_create_errors(app):
    client = _rebuild_client(alias_indices=["datasets-old"])
    client.client.indices.create.side_effect = RequestError(
        400,
        "invalid_index_name_exception",
        {"error": {"reason": "bad name"}, "status": 400},
    )

    result, backfill_mock = _run_rebuild(
        app, client, ["--target-index", "datasets-new"]
    )

    assert result.exit_code != 0
    backfill_mock.assert_not_called()
    client.client.indices.update_aliases.assert_not_called()


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


def test_delete_index_removes_unused_physical_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client.indices.exists.return_value = True
    client.client.indices.get_alias.return_value = {"datasets-old": {"aliases": {}}}
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


def test_delete_index_refuses_index_still_attached_to_alias(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client.indices.exists.return_value = True
    client.client.indices.get_alias.return_value = {
        "datasets-live": {"aliases": {"datasets": {}}}
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets-live"]
        )

    assert result.exit_code != 0
    assert "still attached to alias" in result.output
    client.client.indices.delete.assert_not_called()


def test_delete_index_refuses_logical_alias_name(app):
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
    assert "physical index starting with" in result.output
    client.client.indices.delete.assert_not_called()
