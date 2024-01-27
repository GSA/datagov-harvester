from harvester.harvest import HarvestSource


class TestExtractWAF:
    def test_extract_waf(self, waf_config):
        harvest_source = HarvestSource(**waf_config)
        harvest_source.get_harvest_records_as_id_hash()

        assert len(harvest_source.records) == 7
