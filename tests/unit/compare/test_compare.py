from unittest.mock import patch

from harvester.harvest import HarvestSource


class TestCompare:
    @patch.object(HarvestSource, "get_ckan_records")
    def test_compare(self, get_ckan_records_mock, dcatus_compare_config, ckan_compare):
        get_ckan_records_mock.return_value = ckan_compare

        harvest_source = HarvestSource(**dcatus_compare_config)
        harvest_source.get_record_changes()

        assert len(harvest_source.compare_data["create"]) == 1
        assert len(harvest_source.compare_data["update"]) == 3
        assert len(harvest_source.compare_data["delete"]) == 1

    # TODO: add sort test
