from harvester.harvest import DT_PLACEHOLDER, HarvestSource
from harvester.utils.general_utils import traverse_waf


class TestExtract:
    def test_traverse_waf_ms_iis(self, mock_requests_get_ms_iis_waf):
        """Test to ensure that we're able to traverse the ms-iis-waf"""
        files = traverse_waf(url="https://example.com")
        assert len(files) == 2

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
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records) == 7

    def test_extract_dcatus3_0(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0,
        job_data_dcatus3_0,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records) == 4

    def test_extract_dcatus3_0_nested_catalog(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_nested_catalog,
        job_data_dcatus3_0_nested_catalog,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_nested_catalog)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_nested_catalog)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()

        # datasets from the top-level catalog plus every nested sub-catalog
        assert len(harvest_source.external_records) == 3
        identifiers = {
            record["identifier"] for record in harvest_source.external_records
        }
        assert identifiers == {
            "https://example.gov/datasets/nested-parent",
            "https://example.gov/datasets/nested-child",
            "https://example.gov/datasets/nested-grandchild",
        }

        # catalog-level metadata is persisted on the job, with dataset/
        # service/record stripped at every nesting level, while the
        # (cleaned) "catalog" field itself is preserved
        updated_job = interface.get_harvest_job(harvest_job.id)
        assert updated_job.dcatus_catalog == {
            "@type": "Catalog",
            "@id": "https://example.gov/nested-data.json",
            "conformsTo": {"@type": "Standard", "title": "DCAT-US 3.0"},
            "title": "Example DCAT-US 3.0 Nested Catalog",
            "description": (
                "A sample DCAT-US 3.0 catalog with nested sub-catalogs used for "
                "harvest testing."
            ),
            "publisher": {"@type": "Organization", "name": "Test Agency"},
            "catalog": [
                {
                    "@type": "Catalog",
                    "title": "Child Catalog",
                    "description": "A sub-catalog nested within the parent catalog.",
                    "catalog": [
                        {
                            "@type": "Catalog",
                            "title": "Grandchild Catalog",
                            "description": ("A sub-catalog nested two levels deep."),
                        }
                    ],
                }
            ],
        }

    def test_extract_dcatus3_0_with_services(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_with_services,
        job_data_dcatus3_0_with_services,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_with_services)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_with_services)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()

        # dataset and service objects are extracted independently
        assert len(harvest_source.external_records) == 1
        assert len(harvest_source.external_service_records) == 2
        service_identifiers = {
            record["identifier"] for record in harvest_source.external_service_records
        }
        assert service_identifiers == {
            "https://example.gov/services/one",
            "https://example.gov/services/two",
        }

    def test_extract_dcatus3_0_service_missing_identifier(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_service_no_identifier,
        job_data_dcatus3_0_service_no_identifier,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_service_no_identifier)
        harvest_job = interface.add_harvest_job(
            job_data_dcatus3_0_service_no_identifier
        )

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        # the dataset is unaffected by the service's missing identifier
        assert len(harvest_source.external_records) == 1
        assert len(harvest_source.external_service_records) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_job.id)
        msg = (
            "Test Source DCAT-US 3.0 (service no identifier) "
            "Data Service Without Identifier is missing 'identifier' field"
        )
        assert errors[0][0].message == msg

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

    def test_extract_source_with_dataset_missing_identifier(
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

        assert len(harvest_source.external_records) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_job.id)
        harvest_job = interface.get_harvest_job(harvest_job.id)

        msg = (
            "Test Source (no identifier) Commitment of Traders is "
            "missing 'identifier' field"
        )
        assert errors[0][0].message == msg
        assert harvest_job.records_errored == 1

    def test_extract_dcatus3_0_object_identifier_without_atid(
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

        assert len(harvest_source.external_records) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_job.id)

        msg = (
            "Test Source DCAT-US 3.0 (no identifier) "
            "Dataset With Invalid Object Identifier has an object "
            "'identifier' with no usable '@id' field"
        )
        assert errors[0][0].message == msg

    def test_extract_waf_collection_parent_has_recent_datetime(
        self,
        interface,
        organization_data,
        source_data_waf_collection,
    ):
        """Test that waf-collection parent record gets a datetime of now."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_collection)
        harvest_job = interface.add_harvest_job(
            {"status": "new", "harvest_source_id": source_data_waf_collection["id"]}
        )

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()

        parent_record = harvest_source.external_records[0]
        assert (
            parent_record["identifier"]
            == source_data_waf_collection["collection_parent_url"]
        )

        assert parent_record["modified_date"] == DT_PLACEHOLDER
