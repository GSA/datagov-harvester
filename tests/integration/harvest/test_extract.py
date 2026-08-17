from harvester.harvest import DT_PLACEHOLDER
from harvester.utils.general_utils import traverse_waf


class TestExtract:
    def test_traverse_waf_ms_iis(self, mock_requests_get_ms_iis_waf):
        """Test to ensure that we're able to traverse the ms-iis-waf"""
        files = traverse_waf(url="https://example.com")
        assert len(files) == 2

    def test_extract_dcatus(
        self,
        make_harvest_source,
        source_data_dcatus,
        job_data_dcatus,
    ):
        harvest_source = make_harvest_source(source_data_dcatus, job_data_dcatus)
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records) == 7

    def test_extract_dcatus3_0(
        self,
        make_harvest_source,
        source_data_dcatus3_0,
        job_data_dcatus3_0,
    ):
        harvest_source = make_harvest_source(source_data_dcatus3_0, job_data_dcatus3_0)
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records) == 4

    def test_extract_dcatus3_0_nested_catalog(
        self,
        interface,
        make_harvest_source,
        source_data_dcatus3_0_nested_catalog,
        job_data_dcatus3_0_nested_catalog,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_nested_catalog, job_data_dcatus3_0_nested_catalog
        )
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
        updated_job = interface.get_harvest_job(harvest_source.job_id)
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
        make_harvest_source,
        source_data_dcatus3_0_with_services,
        job_data_dcatus3_0_with_services,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_with_services, job_data_dcatus3_0_with_services
        )
        harvest_source.acquire_minimum_external_data()

        # dataset and service objects are extracted independently
        assert len(harvest_source.external_records) == 1
        service_records = harvest_source.external_records_by_type["data_service"]
        assert len(service_records) == 2
        service_identifiers = {record["identifier"] for record in service_records}
        assert service_identifiers == {
            "https://example.gov/services/one",
            "https://example.gov/services/two",
        }

    def test_extract_dcatus3_0_service_missing_identifier(
        self,
        interface,
        make_harvest_source,
        source_data_dcatus3_0_service_no_identifier,
        job_data_dcatus3_0_service_no_identifier,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_service_no_identifier,
            job_data_dcatus3_0_service_no_identifier,
        )
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        # the dataset is unaffected by the service's missing identifier
        assert len(harvest_source.external_records) == 1
        assert len(harvest_source.external_records_by_type["data_service"]) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_source.job_id)
        msg = (
            "Test Source DCAT-US 3.0 (service no identifier) "
            "Data Service Without Identifier is missing 'identifier' field"
        )
        assert errors[0][0].message == msg

    def test_extract_dcatus3_0_with_records(
        self,
        make_harvest_source,
        source_data_dcatus3_0_with_records,
        job_data_dcatus3_0_with_records,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_with_records, job_data_dcatus3_0_with_records
        )
        harvest_source.acquire_minimum_external_data()

        # dataset and catalog record objects are extracted independently
        assert len(harvest_source.external_records) == 1
        record_records = harvest_source.external_records_by_type["catalog_record"]
        assert len(record_records) == 2
        record_ids = {record["@id"] for record in record_records}
        assert record_ids == {
            "https://example.gov/catalog-records/one",
            "https://example.gov/catalog-records/two",
        }

    def test_extract_dcatus3_0_record_missing_id(
        self,
        interface,
        make_harvest_source,
        source_data_dcatus3_0_record_no_id,
        job_data_dcatus3_0_record_no_id,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_record_no_id,
            job_data_dcatus3_0_record_no_id,
        )
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        # the dataset is unaffected by the catalog record's missing @id
        assert len(harvest_source.external_records) == 1
        assert len(harvest_source.external_records_by_type["catalog_record"]) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_source.job_id)
        msg = (
            "Test Source DCAT-US 3.0 (catalog record no @id) "
            "Catalog Record Without An Id is missing '@id' field"
        )
        assert errors[0][0].message == msg

    def test_extract_dcatus3_0_service_serves_dataset(
        self,
        make_harvest_source,
        source_data_dcatus3_0_service_serves_dataset,
        job_data_dcatus3_0_service_serves_dataset,
    ):
        """A DataService's servesDataset embeds a full Dataset object, which
        must be harvested like any other dataset, tagged with the service's
        identifier as its parent."""
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_service_serves_dataset,
            job_data_dcatus3_0_service_serves_dataset,
        )
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records_by_type["data_service"]) == 1
        assert len(harvest_source.external_records) == 1

        served_dataset = harvest_source.external_records[0]
        assert served_dataset["identifier"] == (
            "https://example.gov/datasets/served-by-service-one"
        )
        assert served_dataset["parent_identifier"] == (
            "https://example.gov/services/one"
        )

    def test_extract_dcatus3_0_series_with_members(
        self,
        make_harvest_source,
        source_data_dcatus3_0_series_with_members,
        job_data_dcatus3_0_series_with_members,
    ):
        """A DatasetSeries's seriesMember/first/last embed full Dataset
        objects; first/last duplicate entries already in seriesMember must
        not be double-harvested, and each surviving dataset is tagged with
        the series' identifier as its parent."""
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_series_with_members,
            job_data_dcatus3_0_series_with_members,
        )
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records_by_type["data_series"]) == 1
        assert len(harvest_source.external_records) == 2

        identifiers = {r["identifier"] for r in harvest_source.external_records}
        assert identifiers == {
            "https://example.gov/datasets/annual-report-2023",
            "https://example.gov/datasets/annual-report-2024",
        }
        assert all(
            r["parent_identifier"] == "https://example.gov/series/annual-report"
            for r in harvest_source.external_records
        )

    def test_extract_dcatus3_0_series_member_also_top_level(
        self,
        make_harvest_source,
        source_data_dcatus3_0_series_member_also_top_level,
        job_data_dcatus3_0_series_member_also_top_level,
    ):
        """A dataset listed both at the catalog's top level and as a
        DatasetSeries seriesMember is the same dataset, not a duplicate --
        it must be harvested once, tagged with the series as its parent."""
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_series_member_also_top_level,
            job_data_dcatus3_0_series_member_also_top_level,
        )
        harvest_source.acquire_minimum_external_data()

        assert len(harvest_source.external_records_by_type["data_series"]) == 1
        assert len(harvest_source.external_records) == 2

        by_identifier = {r["identifier"]: r for r in harvest_source.external_records}
        assert by_identifier.keys() == {
            "https://example.gov/datasets/annual-report-2023",
            "https://example.gov/datasets/annual-report-2024",
        }
        assert all(
            r["parent_identifier"] == "https://example.gov/series/annual-report"
            for r in harvest_source.external_records
        )
        # The top-level entry is canonical: its description survives, not
        # the (possibly different) copy embedded in the series member.
        assert by_identifier["https://example.gov/datasets/annual-report-2023"][
            "description"
        ] == (
            "The first dataset in the series, also listed at the "
            "catalog's top level."
        )

        harvest_source.filter_duplicate_identifiers()
        assert len(harvest_source.external_records) == 2

    def test_check_iso_dcatus_schema(
        self,
        make_harvest_source,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
    ):
        harvest_source = make_harvest_source(
            source_data_waf_iso19115_2, job_data_waf_iso19115_2
        )

        assert str(harvest_source.schema_file).endswith("iso-non-federal_dataset.json")

    def test_extract_source_with_dataset_missing_identifier(
        self,
        interface,
        make_harvest_source,
        source_data_dcatus_no_identifier,
        job_data_dcatus_no_identifier,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus_no_identifier, job_data_dcatus_no_identifier
        )
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert len(harvest_source.external_records) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_source.job_id)
        harvest_job = interface.get_harvest_job(harvest_source.job_id)

        msg = (
            "Test Source (no identifier) Commitment of Traders is "
            "missing 'identifier' field"
        )
        assert errors[0][0].message == msg
        assert harvest_job.records_errored == 1

    def test_extract_dcatus3_0_object_identifier_without_atid(
        self,
        interface,
        make_harvest_source,
        source_data_dcatus3_0_no_identifier,
        job_data_dcatus3_0_no_identifier,
    ):
        harvest_source = make_harvest_source(
            source_data_dcatus3_0_no_identifier, job_data_dcatus3_0_no_identifier
        )
        harvest_source.acquire_data_sources()
        harvest_source.filter_datasets_with_no_identifier()

        assert len(harvest_source.external_records) == 0

        errors = interface.get_harvest_record_errors_by_job(harvest_source.job_id)

        msg = (
            "Test Source DCAT-US 3.0 (no identifier) "
            "Dataset With Invalid Object Identifier has an object "
            "'identifier' with no usable '@id' field"
        )
        assert errors[0][0].message == msg

    def test_extract_waf_collection_parent_has_recent_datetime(
        self,
        make_harvest_source,
        source_data_waf_collection,
    ):
        """Test that waf-collection parent record gets a datetime of now."""
        harvest_source = make_harvest_source(
            source_data_waf_collection,
            {"status": "new", "harvest_source_id": source_data_waf_collection["id"]},
        )
        harvest_source.acquire_minimum_external_data()

        parent_record = harvest_source.external_records[0]
        assert (
            parent_record["identifier"]
            == source_data_waf_collection["collection_parent_url"]
        )

        assert parent_record["modified_date"] == DT_PLACEHOLDER
