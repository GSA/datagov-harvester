from unittest.mock import MagicMock

import pytest

from harvester.harvest import HarvestSource


@pytest.fixture
def harvest_source_for_identifier_filter():
    source = HarvestSource.__new__(HarvestSource)
    source._job_id = "test-job-id"
    source.id = "test-source-id"
    source.name = "Test Source"
    source.external_records = []
    source._db_interface = MagicMock()
    source._db_interface.add_harvest_record.return_value = MagicMock(
        id="error-record-id",
        identifier="Dataset With Invalid Object Identifier",
    )
    return source


class TestFilterDatasetsWithNoIdentifier:
    def test_rejects_object_identifier_without_atid(
        self, harvest_source_for_identifier_filter
    ):
        """Harvest fails when an object identifier has no @id."""
        harvest_source_for_identifier_filter.external_records = [
            {
                "title": "Dataset With Invalid Object Identifier",
                "identifier": {"@type": "Identifier"},
            }
        ]

        harvest_source_for_identifier_filter.filter_datasets_with_no_identifier()

        assert harvest_source_for_identifier_filter.external_records == []
        harvest_source_for_identifier_filter._db_interface.add_harvest_record.assert_called_once()

    def test_keeps_object_identifier_with_atid(
        self, harvest_source_for_identifier_filter
    ):
        """Harvest does not fail when @id is present on an object identifier."""
        dataset = {
            "title": "Dataset With Object Identifier",
            "identifier": {
                "@type": "Identifier",
                "@id": "https://example.gov/datasets/three",
            },
        }
        harvest_source_for_identifier_filter.external_records = [dataset]

        harvest_source_for_identifier_filter.filter_datasets_with_no_identifier()

        assert harvest_source_for_identifier_filter.external_records == [dataset]
        harvest_source_for_identifier_filter._db_interface.add_harvest_record.assert_not_called()
