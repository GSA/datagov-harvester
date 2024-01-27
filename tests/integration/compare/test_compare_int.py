from harvester.harvest import HarvestSource


class TestCompareInt:
    def test_compare_int(self, dcatus_compare_config):
        harvest_source = HarvestSource(**dcatus_compare_config)
        harvest_source.get_record_changes()

        assert len(harvest_source.compare_data["create"]) == 1
        assert len(harvest_source.compare_data["update"]) == 3
        assert len(harvest_source.compare_data["delete"]) == 1
