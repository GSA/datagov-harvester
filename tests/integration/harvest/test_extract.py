from harvester.harvest import HarvestSource
from harvester.utils.general_utils import traverse_waf


class TestExtract:
    def test_traverse_waf_ms_iis(self, mock_requests_get_ms_iis_waf):
        """Test to ensure that we're able to traverse the ms-iis-waf"""
        files = traverse_waf(url="https://example.com")
        assert len(files) == 2

    def test_extract_waf(
        self,
        interface,
        organization_data,
        source_data_waf_csdgm,
        job_data_waf_csdgm,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_csdgm)
        harvest_job = interface.add_harvest_job(job_data_waf_csdgm)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        assert len(harvest_source.external_records) == 7

    def test_extract_dcatus(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        assert len(harvest_source.external_records) == 7

    def test_check_iso_dcatus_schema(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)

        assert str(harvest_source.schema_file).endswith("iso-non-federal_dataset.json")
