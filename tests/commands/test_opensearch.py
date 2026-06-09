from datetime import datetime
from unittest.mock import Mock

from database.models import Dataset


def _insert_dataset(interface, dataset_id, last_harvested):
    dataset = Dataset(
        id=dataset_id,
        slug=dataset_id,
        dcat={"title": dataset_id},
        harvest_record_id="09f073b3-00e3-4147-ba69-a5d0fd7ce021",
        harvest_source_id="2f2652de-91df-4c63-8b53-bfced20b276b",
        organization_id="d925f84d-955b-4cb7-812f-dcfd6681a18f",
        last_harvested_date=last_harvested,
    )
    interface.db.add(dataset)
    interface.db.commit()
    return dataset


class TestCompareCommand:
    @staticmethod
    def _prepare_environment(interface, hits, monkeypatch):
        monkeypatch.setattr(
            "app.commands.opensearch.HarvesterDBInterface", lambda: interface
        )

        os_client = Mock()
        os_client.INDEX_NAME = "datasets"
        os_client.client = Mock()
        os_client.client.delete = Mock()
        os_client.index_datasets = Mock(return_value=(1, 0, 0))
        os_client._refresh = Mock()

        monkeypatch.setattr(
            "app.commands.opensearch.OpenSearchInterface.from_environment",
            lambda: os_client,
        )
        monkeypatch.setattr(
            "app.commands.opensearch.scan",
            lambda *args, **kwargs: iter(hits),
        )
        return os_client

    def test_compare_reports_discrepancies(
        self, cli_runner, interface_with_fixture_json, monkeypatch
    ):
        _insert_dataset(
            interface_with_fixture_json,
            "db-only",
            datetime(2024, 1, 1, 0, 0, 0),
        )
        _insert_dataset(
            interface_with_fixture_json,
            "stale",
            datetime(2024, 1, 2, 0, 0, 0),
        )

        hits = [
            {
                "_id": "stale",
                "_source": {"last_harvested_date": "2024-01-05T00:00:00Z"},
            },
            {"_id": "extra-only", "_source": {"last_harvested_date": None}},
        ]

        os_client = self._prepare_environment(
            interface_with_fixture_json, hits, monkeypatch
        )

        result = cli_runner.invoke(args=["search", "compare", "--sample-size", "5"])

        assert result.exit_code == 0
        assert "Missing in OpenSearch (should be indexed): 1" in result.output
        assert "Example missing IDs: db-only" in result.output
        assert "Extra in OpenSearch (should be deleted): 1" in result.output
        assert "Example extra IDs: extra-only" in result.output
        assert "Updated in OpenSearch (last_harvested_date differs): 1" in result.output
        assert "stale (DB: 2024-01-02T00:00:00.000+00:00" in result.output
        assert "OS: 2024-01-05T00:00:00.000+00:00" in result.output
        os_client.index_datasets.assert_not_called()
        os_client.client.delete.assert_not_called()
        os_client._refresh.assert_not_called()

    def test_compare_update_indexes_and_deletes(
        self, cli_runner, interface_with_fixture_json, monkeypatch
    ):
        _insert_dataset(
            interface_with_fixture_json,
            "db-only",
            datetime(2024, 2, 1, 0, 0, 0),
        )
        _insert_dataset(
            interface_with_fixture_json,
            "stale",
            datetime(2024, 2, 2, 0, 0, 0),
        )

        hits = [
            {
                "_id": "stale",
                "_source": {"last_harvested_date": "2024-02-05T00:00:00Z"},
            },
            {"_id": "extra-only", "_source": {"last_harvested_date": None}},
        ]

        os_client = self._prepare_environment(
            interface_with_fixture_json, hits, monkeypatch
        )

        result = cli_runner.invoke(args=["search", "compare", "--update"])
        assert result.exit_code == 0
        assert "Updating discrepancies" in result.output
        assert os_client.index_datasets.call_count == 2
        reindexed_sets = [
            {dataset.id for dataset in call.args[0]}
            for call in os_client.index_datasets.call_args_list
        ]
        assert {"db-only"} in reindexed_sets
        assert {"stale"} in reindexed_sets
        os_client.client.delete.assert_called_once()
        delete_call = os_client.client.delete.call_args
        assert delete_call.kwargs["id"] == "extra-only"
        os_client._refresh.assert_called_once()

    def test_compare_harvest_sources_known_on_dataset_error(
        self, cli_runner, interface_with_fixture_json, monkeypatch
    ):
        _insert_dataset(
            interface_with_fixture_json,
            "db-only",
            datetime(2024, 1, 1, 0, 0, 0),
        )
        hits = []

        os_client = self._prepare_environment(
            interface_with_fixture_json, hits, monkeypatch
        )

        test_errors = [
            {
                "dataset_id": "db-only",
                "status_code": 400,
                "error_type": "mapper_parsing_exception",
                "error_reason": "failed to parse field [dcat.modified]",
                "caused_by": {"type": "illegal_argument_exception"},
            }
        ]

        os_client.index_datasets = Mock(return_value=(98, 2, test_errors))

        result = cli_runner.invoke(args=["search", "compare", "--update"])

        assert (
            "Harvest sources not synced with opensearch..."
            "\n2f2652de-91df-4c63-8b53-bfced20b276b" in result.output
        )

    def test_compare_force_update_reindexes_all_datasets(
        self, cli_runner, interface_with_fixture_json, monkeypatch
    ):
        _insert_dataset(
            interface_with_fixture_json,
            "current",
            datetime(2024, 3, 1, 0, 0, 0),
        )
        _insert_dataset(
            interface_with_fixture_json,
            "also-current",
            datetime(2024, 3, 2, 0, 0, 0),
        )

        hits = [
            {
                "_id": "current",
                "_source": {"last_harvested_date": "2024-03-01T00:00:00Z"},
            },
            {
                "_id": "also-current",
                "_source": {"last_harvested_date": "2024-03-02T00:00:00Z"},
            },
        ]

        os_client = self._prepare_environment(
            interface_with_fixture_json, hits, monkeypatch
        )

        result = cli_runner.invoke(args=["search", "compare", "--force-update"])

        assert result.exit_code == 0
        assert "Force re-indexing 2 datasets" in result.output
        assert os_client.index_datasets.call_count == 1
        reindexed_ids = {
            dataset.id for dataset in os_client.index_datasets.call_args.args[0]
        }
        assert reindexed_ids == {"current", "also-current"}
        os_client.client.delete.assert_not_called()
        os_client._refresh.assert_called_once()
