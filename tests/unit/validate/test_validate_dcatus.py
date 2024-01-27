from harvester.harvest import HarvestSource


class TestValidateDCATUS:
    def test_validate_dcatus(self, dcatus_config):
        harvest_source = HarvestSource(**dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        test_record = harvest_source.records["cftc-dc1"]
        test_record.validate()

        assert test_record.valid is True
