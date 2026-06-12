from datetime import datetime
from unittest.mock import Mock, patch


def test_reset_mapping_recreates_empty_index(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.MAPPINGS = {"properties": {"title": {"type": "text"}}}
    client.client.indices.get_mapping.return_value = {
        "datasets": {"mappings": client.MAPPINGS}
    }

    with patch(
        "app.commands.search.OpenSearchInterface.from_environment",
        return_value=client,
    ):
        result = app.test_cli_runner().invoke(args=["search", "reset-mapping"])

    assert result.exit_code == 0
    client.client.indices.delete.assert_called_once_with(index="datasets")
    client._ensure_index.assert_called_once_with()
    client.index_datasets.assert_not_called()
    assert "Mapping reset successfully. The index is empty." in result.output


def test_compare_update_indexes_missing_and_deletes_extra(app):
    client = Mock()
    client.INDEX_NAME = "datasets"
    client.client = Mock()
    client.index_datasets.return_value = (1, 0, [])

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
            "app.commands.search.OpenSearchInterface.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.db.session.query", side_effect=query_side_effect),
        patch(
            "app.commands.search.scan",
            return_value=iter(
                [{"_id": "extra-only", "fields": {"last_harvested_date": []}}]
            ),
        ),
    ):
        result = app.test_cli_runner().invoke(args=["search", "compare", "--update"])

    assert result.exit_code == 0
    client.index_datasets.assert_called_once_with(
        [missing_dataset], refresh_after=False
    )
    client.client.delete.assert_called_once_with(index="datasets", id="extra-only")
    client._refresh.assert_called_once_with()


def test_compare_is_read_only_without_update(app):
    client = Mock()
    client.INDEX_NAME = "datasets"

    rows_query = Mock()
    rows_query.all.return_value = []

    with (
        patch(
            "app.commands.search.OpenSearchInterface.from_environment",
            return_value=client,
        ),
        patch("app.commands.search.db.session.query", return_value=rows_query),
        patch("app.commands.search.scan", return_value=iter([])),
    ):
        result = app.test_cli_runner().invoke(args=["search", "compare"])

    assert result.exit_code == 0
    client.index_datasets.assert_not_called()
    client.client.delete.assert_not_called()
    client._refresh.assert_not_called()
