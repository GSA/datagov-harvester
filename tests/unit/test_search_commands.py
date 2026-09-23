import logging
from datetime import datetime
from unittest.mock import Mock, patch

from app.commands.search import OPENSEARCH_MISSING_DOCUMENTS_BANNER, db_interface
from search.writer import OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE


def test_reset_mapping_recreates_empty_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client.indices.exists_alias.return_value = False
    client.client.indices.exists.return_value = True
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
    client.client.indices.exists_alias.return_value = False
    client.client.indices.exists.return_value = True
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
    client.client.indices.exists_alias.return_value = False
    client.client.indices.exists.return_value = True
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


def _reset_mapping_client(mappings=None):
    """Build a mocked client whose live mapping matches the application mapping.

    Keeps the alias-shape tests focused on which index gets deleted rather than
    on the mapping comparison, which the tests above already cover.
    """
    mappings = mappings or {"properties": {"title": {"type": "text"}}}
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = mappings
    client.client.indices.get_mapping.return_value = {
        "datasets": {"mappings": mappings}
    }
    return client


def test_reset_mapping_deletes_indices_behind_a_leftover_alias(app):
    """A cluster rebuilt by the retired rebuild-index workflow has `datasets` as
    an alias, and ``indices.delete`` rejects an alias by name."""
    client = _reset_mapping_client()
    client.client.indices.exists_alias.return_value = True
    client.client.indices.get_alias.return_value = {
        "datasets-20260827150000": {"aliases": {"datasets": {}}}
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    # The backing index, not the alias name, is what can actually be deleted.
    client.client.indices.delete.assert_called_once_with(
        index="datasets-20260827150000"
    )
    assert "is a leftover alias for datasets-20260827150000" in result.output
    assert "Mapping reset successfully. The index is empty." in result.output


def test_reset_mapping_deletes_every_index_behind_a_multi_index_alias(app):
    client = _reset_mapping_client()
    client.client.indices.exists_alias.return_value = True
    client.client.indices.get_alias.return_value = {
        "datasets-second": {"aliases": {"datasets": {}}},
        "datasets-first": {"aliases": {"datasets": {}}},
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    client.client.indices.delete.assert_called_once_with(
        index="datasets-first,datasets-second"
    )


def test_reset_mapping_skips_delete_when_nothing_exists(app):
    client = _reset_mapping_client()
    client.client.indices.exists_alias.return_value = False
    client.client.indices.exists.return_value = False

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    client.client.indices.delete.assert_not_called()
    client._ensure_index.assert_called_once_with()


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
        patch("app.commands.search.OpenSearchWriter", return_value=Mock()),
    ):
        return app.test_cli_runner().invoke(args=["search", "compare", *args])


def _dataset_rows(count):
    return [(f"dataset-{index}", datetime(2024, 1, 1)) for index in range(count)]


def _opensearch_hits(count):
    return [
        {
            "_id": f"dataset-{index}",
            "fields": {"last_harvested_date": ["2024-01-01T00:00:00"]},
        }
        for index in range(count)
    ]


def test_compare_validation_allows_fifty_missing_records(app):
    result = _run_compare(
        app,
        ["--fail-on-discrepancy", "--max-failed-records", "50"],
        db_rows=_dataset_rows(100),
        os_hits=_opensearch_hits(50),
    )

    assert result.exit_code == 0, result.output
    assert "50 of 100 (50.000%) failed; allowed up to 50 record(s)" in result.output
    assert OPENSEARCH_MISSING_DOCUMENTS_BANNER in result.output
    assert "dataset-99" in result.output


def test_compare_validation_rejects_fifty_one_missing_records(app):
    result = _run_compare(
        app,
        ["--fail-on-discrepancy", "--max-failed-records", "50"],
        db_rows=_dataset_rows(100),
        os_hits=_opensearch_hits(49),
    )

    assert result.exit_code != 0
    assert "51 of 100 (51.000%) failed; allowed up to 50 record(s)" in result.output
    assert OPENSEARCH_MISSING_DOCUMENTS_BANNER in result.output
    assert "dataset-99" in result.output


def test_compare_validation_never_tolerates_extra_documents(app):
    result = _run_compare(
        app,
        ["--fail-on-discrepancy", "--max-failed-records", "50"],
        os_hits=[{"_id": "extra", "fields": {"last_harvested_date": []}}],
    )

    assert result.exit_code != 0
    assert "Discrepancies found: 0 missing, 1 extra, 0 updated." in result.output


def test_compare_validation_never_tolerates_stale_documents(app):
    result = _run_compare(
        app,
        ["--fail-on-discrepancy", "--max-failed-records", "50"],
        db_rows=[("dataset-0", datetime(2024, 2, 1))],
        os_hits=[
            {
                "_id": "dataset-0",
                "fields": {"last_harvested_date": ["2024-01-01T00:00:00"]},
            }
        ],
    )

    assert result.exit_code != 0
    assert "Discrepancies found: 0 missing, 0 extra, 1 updated." in result.output
