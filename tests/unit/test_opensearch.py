from unittest.mock import Mock, patch

from harvester.opensearch import OpenSearchClient, OpenSearchWriter


def test_writer_can_target_physical_index():
    raw_client = Mock()
    writer = OpenSearchWriter(Mock(client=raw_client))
    document = {"_index": "datasets", "_id": "dataset-1"}

    with (
        patch("harvester.opensearch.DatasetDocument") as document_class,
        patch(
            "harvester.opensearch.helpers.streaming_bulk",
            return_value=iter(
                [(True, {"index": {"result": "created", "_id": "dataset-1"}})]
            ),
        ) as streaming_bulk,
    ):
        document_class.return_value.dataset_to_document.return_value = document
        result = writer.index_datasets(
            [Mock()],
            refresh_after=False,
            index_name="datasets-20260716",
        )

    assert result == (1, 0, [])
    assert document["_index"] == "datasets-20260716"
    streaming_bulk.assert_called_once_with(
        raw_client,
        [document],
        raise_on_error=False,
        max_retries=8,
    )


def test_switch_alias_keeps_previous_physical_index():
    client = OpenSearchClient.__new__(OpenSearchClient)
    client.client = Mock()
    client.client.indices.exists.return_value = True
    client.client.indices.exists_alias.return_value = True
    client.client.indices.get_alias.return_value = {
        "datasets-old": {"aliases": {}}
    }
    client.client.indices.update_aliases.return_value = {"acknowledged": True}

    old_indices, removed_legacy = client.switch_alias("datasets-new")

    assert old_indices == ["datasets-old"]
    assert removed_legacy is False
    client.client.indices.update_aliases.assert_called_once_with(
        body={
            "actions": [
                {"remove": {"index": "datasets-old", "alias": "datasets"}},
                {
                    "add": {
                        "index": "datasets-new",
                        "alias": "datasets",
                        "is_write_index": True,
                    }
                },
            ]
        }
    )


def test_switch_alias_atomically_replaces_legacy_concrete_index():
    client = OpenSearchClient.__new__(OpenSearchClient)
    client.client = Mock()
    client.client.indices.exists.return_value = True
    client.client.indices.exists_alias.return_value = False
    client.client.indices.update_aliases.return_value = {"acknowledged": True}

    old_indices, removed_legacy = client.switch_alias("datasets-new")

    assert old_indices == []
    assert removed_legacy is True
    client.client.indices.update_aliases.assert_called_once_with(
        body={
            "actions": [
                {"remove_index": {"index": "datasets"}},
                {
                    "add": {
                        "index": "datasets-new",
                        "alias": "datasets",
                        "is_write_index": True,
                    }
                },
            ]
        }
    )
