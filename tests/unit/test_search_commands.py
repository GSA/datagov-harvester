from datetime import datetime
from unittest.mock import Mock, patch

from app.commands.search import (
    OPENSEARCH_INDEX_BATCH_FAILURE_MESSAGE,
    _backfill_index,
    _format_duration,
    _normalize_mapping_for_comparison,
    db_interface,
)


def test_mapping_comparison_normalizes_dynamic_boolean_strings():
    assert _normalize_mapping_for_comparison({"dynamic": "false"}) == {"dynamic": False}
    assert _normalize_mapping_for_comparison({"dynamic": "true"}) == {"dynamic": True}


def test_format_duration_uses_operator_friendly_units():
    assert _format_duration(0) == "0s"
    assert _format_duration(65) == "1m 5s"
    assert _format_duration(3661) == "1h 1m 1s"


def test_backfill_index_reports_progress_rate_and_eta(capsys):
    client = Mock()
    client.index_datasets.side_effect = [
        (2, 0, []),
        (2, 0, []),
    ]
    dataset_query = Mock()
    dataset_query.order_by.return_value = dataset_query
    dataset_query.filter.return_value = dataset_query
    dataset_query.limit.return_value.all.side_effect = [
        [Mock(id=1), Mock(id=2)],
        [Mock(id=3), Mock(id=4)],
        [],
    ]

    with (
        patch("app.commands.search.db_interface.db.query", return_value=dataset_query),
        patch("app.commands.search.db_interface.db.expunge_all"),
        patch("app.commands.search.monotonic", side_effect=[100, 110, 120]),
    ):
        indexed, failed = _backfill_index(client, "datasets-new", 2, 4)

    assert (indexed, failed) == (4, 0)
    output = capsys.readouterr().out
    assert "Backfill batch 1/2: indexing 2 dataset(s)..." in output
    assert (
        "Progress: 2/4 datasets (50.0%); elapsed 10s; "
        "average 0.2 datasets/s; ETA 10s."
    ) in output
    assert (
        "Progress: 4/4 datasets (100.0%); elapsed 20s; "
        "average 0.2 datasets/s; ETA 0s."
    ) in output


def test_backfill_index_stops_after_failed_batch(capsys):
    client = Mock()
    client.index_datasets.return_value = (1, 1, ["failed document"])
    dataset_query = Mock()
    dataset_query.order_by.return_value = dataset_query
    dataset_query.limit.return_value.all.return_value = [Mock(id=1), Mock(id=2)]

    with (
        patch("app.commands.search.db_interface.db.query", return_value=dataset_query),
        patch("app.commands.search.monotonic", side_effect=[100, 110]),
    ):
        indexed, failed = _backfill_index(client, "datasets-new", 2, 4)

    assert (indexed, failed) == (1, 1)
    client.index_datasets.assert_called_once()
    output = capsys.readouterr().out
    assert "1 dataset(s) failed to index in this batch" in output
    assert "OpenSearch error: failed document" in output


def test_reset_mapping_recreates_empty_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = []
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
    client.alias_indices.return_value = []
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
    assert "Created index mapping does not match application mapping:" in result.output
    assert "--- expected" in result.output
    assert "+++ actual" in result.output
    assert '-      "type": "text"' in result.output
    assert '+      "type": "keyword"' in result.output


def test_reset_mapping_refuses_to_delete_alias_target(app):
    client = Mock()
    client.alias_indices.return_value = ["datasets-current"]

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code != 0
    assert "cannot run after datasets becomes an alias" in result.output
    client.client.indices.delete.assert_not_called()


def test_rebuild_index_requires_zero_harvest_capacity(app):
    with patch(
        "app.commands.search.harvest_scheduling_is_disabled",
        return_value=False,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "rebuild-index", "--target-index", "datasets-new"]
        )

    assert result.exit_code != 0
    assert "HARVEST_RUNNER_MAX_TASKS must be 0" in result.output


def test_rebuild_index_rejects_old_index_deletion_without_alias_switch(app):
    result = app.test_cli_runner().invoke(
        args=[
            "search",
            "rebuild-index",
            "--target-index",
            "datasets-new",
            "--no-switch-alias",
            "--delete-old-index",
        ]
    )

    assert result.exit_code != 0
    assert "--delete-old-index requires --switch-alias" in result.output


def test_rebuild_index_requires_explicit_legacy_index_removal(app):
    handler = Mock()
    handler.get_active_harvest_tasks.return_value = []
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = []
    client.client.indices.exists.return_value = True

    with (
        patch(
            "app.commands.search.harvest_scheduling_is_disabled",
            return_value=True,
        ),
        patch(
            "app.commands.search.create_task_handler",
            return_value=handler,
        ),
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "rebuild-index", "--target-index", "datasets-new"]
        )

    assert result.exit_code != 0
    assert "--allow-legacy-index-removal" in result.output
    client.create_index.assert_not_called()


def test_rebuild_index_backfills_validates_and_switches_alias(app):
    handler = Mock()
    handler.get_active_harvest_tasks.return_value = []
    client = Mock()
    writer = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {"properties": {"title": {"type": "text"}}}
    client.alias_indices.return_value = ["datasets-old"]
    client.index_count.return_value = 2
    client.client.indices.get_mapping.return_value = {
        "datasets-new": {"mappings": client.MAPPINGS}
    }
    client.switch_alias.return_value = (["datasets-old"], False)
    dataset_query = Mock()
    dataset_query.count.side_effect = [2, 2]

    with (
        patch(
            "app.commands.search.harvest_scheduling_is_disabled",
            return_value=True,
        ),
        patch(
            "app.commands.search.create_task_handler",
            return_value=handler,
        ),
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.OpenSearchWriter", return_value=writer),
        patch("app.commands.search.db_interface.db.query", return_value=dataset_query),
        patch("app.commands.search._backfill_index", return_value=(2, 0)),
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "rebuild-index", "--target-index", "datasets-new"]
        )

    assert result.exit_code == 0
    client.create_index.assert_called_once_with("datasets-new")
    writer._refresh.assert_called_once_with(index_name="datasets-new")
    client.switch_alias.assert_called_once_with("datasets-new")
    client.client.indices.delete.assert_not_called()
    assert "Previous index retained: datasets-old" in result.output
    assert "datasets now points to datasets-new" in result.output


def test_rebuild_index_can_delete_previous_index_after_alias_switch(app):
    events = []
    handler = Mock()
    handler.get_active_harvest_tasks.return_value = []
    client = Mock()
    writer = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {"properties": {"title": {"type": "text"}}}
    client.alias_indices.side_effect = [["datasets-old"], ["datasets-new"]]
    client.index_count.return_value = 2
    client.client.indices.exists.return_value = True
    client.client.indices.get_mapping.return_value = {
        "datasets-new": {"mappings": client.MAPPINGS}
    }
    client.client.indices.get_alias.return_value = {"datasets-old": {"aliases": {}}}
    client.switch_alias.side_effect = lambda target: (
        events.append(("switch", target)) or (["datasets-old"], False)
    )
    client.client.indices.delete.side_effect = lambda index: (
        events.append(("delete", index)) or {"acknowledged": True}
    )
    dataset_query = Mock()
    dataset_query.count.side_effect = [2, 2]

    with (
        patch(
            "app.commands.search.harvest_scheduling_is_disabled",
            return_value=True,
        ),
        patch(
            "app.commands.search.create_task_handler",
            return_value=handler,
        ),
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.OpenSearchWriter", return_value=writer),
        patch("app.commands.search.db_interface.db.query", return_value=dataset_query),
        patch("app.commands.search._backfill_index", return_value=(2, 0)),
    ):
        result = app.test_cli_runner().invoke(
            args=[
                "search",
                "rebuild-index",
                "--target-index",
                "datasets-new",
                "--delete-old-index",
            ]
        )

    assert result.exit_code == 0
    assert events == [
        ("switch", "datasets-new"),
        ("delete", "datasets-old"),
    ]
    assert "Deleted OpenSearch index datasets-old." in result.output


def test_rebuild_index_can_validate_without_switching_alias(app):
    handler = Mock()
    handler.get_active_harvest_tasks.return_value = []
    client = Mock()
    writer = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {"properties": {"title": {"type": "text"}}}
    client.alias_indices.return_value = []
    client.client.indices.exists.return_value = True
    client.index_count.return_value = 1
    client.client.indices.get_mapping.return_value = {
        "datasets-new": {"mappings": client.MAPPINGS}
    }
    dataset_query = Mock()
    dataset_query.count.side_effect = [1, 1]

    with (
        patch(
            "app.commands.search.harvest_scheduling_is_disabled",
            return_value=True,
        ),
        patch(
            "app.commands.search.create_task_handler",
            return_value=handler,
        ),
        patch(
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.OpenSearchWriter", return_value=writer),
        patch("app.commands.search.db_interface.db.query", return_value=dataset_query),
        patch("app.commands.search._backfill_index", return_value=(1, 0)),
    ):
        result = app.test_cli_runner().invoke(
            args=[
                "search",
                "rebuild-index",
                "--target-index",
                "datasets-new",
                "--no-switch-alias",
            ]
        )

    assert result.exit_code == 0
    client.create_index.assert_called_once_with("datasets-new")
    client.switch_alias.assert_not_called()
    assert "datasets-new is validated; datasets was not changed" in result.output


def test_delete_index_deletes_unused_physical_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = ["datasets-current"]
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

    assert result.exit_code == 0
    client.client.indices.delete.assert_called_once_with(index="datasets-old")
    assert "Deleted OpenSearch index datasets-old." in result.output


def test_delete_index_refuses_active_alias_target(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = ["datasets-current"]

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets-current"]
        )

    assert result.exit_code != 0
    assert "datasets alias currently points to it" in result.output
    client.client.indices.delete.assert_not_called()


def test_delete_index_rejects_non_physical_index_name(app):
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
    assert "must be a physical index starting with 'datasets-'" in result.output
    client.client.indices.delete.assert_not_called()


def test_delete_index_reports_missing_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = ["datasets-current"]
    client.client.indices.exists.return_value = False

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets-missing"]
        )

    assert result.exit_code != 0
    assert "OpenSearch index does not exist: datasets-missing" in result.output
    client.client.indices.delete.assert_not_called()


def test_delete_index_refuses_index_with_attached_alias(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.alias_indices.return_value = []
    client.client.indices.exists.return_value = True
    client.client.indices.get_alias.return_value = {
        "datasets-old": {"aliases": {"rollback": {}}}
    }

    with patch(
        "app.commands.search.OpenSearchClient.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(
            args=["search", "delete-index", "--index-name", "datasets-old"]
        )

    assert result.exit_code != 0
    assert "attached aliases: rollback" in result.output
    client.client.indices.delete.assert_not_called()


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
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
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
            "app.commands.search.OpenSearchClient.from_environment",
            return_value=client,
        ),
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
