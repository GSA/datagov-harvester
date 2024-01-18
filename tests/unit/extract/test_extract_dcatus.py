from harvester.harvest import HarvestSource


class TestExtractDCATUS:
    def test_request_dcatus_and_hash(self, dcatus_config):
        harvest_source = HarvestSource(**dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        assert len(harvest_source.records) == 7
