from harvester.harvest import HarvestSource


class TestExtract:
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
