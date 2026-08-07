from harvester.harvest import HarvestSource


class TestMissingIdentifierErrorCount:
    def test_missing_identifier_increments_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus_no_identifier,
        job_data_dcatus_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 1

    def test_missing_identifier_persists_error_count_to_job(
        self,
        interface,
        organization_data,
        source_data_dcatus_no_identifier,
        job_data_dcatus_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        stored_job = interface.get_harvest_job(harvest_job.id)
        assert stored_job.records_errored == 1

    def test_dcatus3_object_identifier_without_atid_increments_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_no_identifier,
        job_data_dcatus3_0_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 1

    def test_dcatus3_object_identifier_without_atid_persists_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_no_identifier,
        job_data_dcatus3_0_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        stored_job = interface.get_harvest_job(harvest_job.id)
        assert stored_job.records_errored == 1

    def test_multiple_missing_identifiers_count_all_errors(
        self,
        interface,
        organization_data,
        source_data_dcatus_multiple_no_identifier,
        job_data_dcatus_multiple_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_multiple_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_multiple_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 3

    def test_multiple_missing_identifiers_persist_total_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus_multiple_no_identifier,
        job_data_dcatus_multiple_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_multiple_no_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_multiple_no_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        stored_job = interface.get_harvest_job(harvest_job.id)
        assert stored_job.records_errored == 3

    def test_mixed_valid_and_missing_identifiers_counts_only_errors(
        self,
        interface,
        organization_data,
        source_data_dcatus_mixed_identifiers,
        job_data_dcatus_mixed_identifiers,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_mixed_identifiers)
        harvest_job = interface.add_harvest_job(job_data_dcatus_mixed_identifiers)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 2
        assert len(harvest_source.external_records) == 5

    def test_empty_string_identifier_increments_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus_empty_identifier,
        job_data_dcatus_empty_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_empty_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_empty_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 1

    def test_whitespace_only_identifier_increments_error_count(
        self,
        interface,
        organization_data,
        source_data_dcatus_whitespace_identifier,
        job_data_dcatus_whitespace_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_whitespace_identifier)
        harvest_job = interface.add_harvest_job(job_data_dcatus_whitespace_identifier)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert harvest_source.reporter.errored == 1
