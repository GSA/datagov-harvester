import json

import pytest

from harvester.harvest import HarvestSource, Record


@pytest.fixture
def valid_iso_2_record(
    interface, organization_data, source_data_waf_iso19115_2, job_data_waf_iso19115_2
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_waf_iso19115_2)
    harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

    harvest_source = HarvestSource(harvest_job.id)
    harvest_source.acquire_minimum_external_data()
    external_records_to_process = harvest_source.external_records_to_process()

    records = list(external_records_to_process)

    # Filter for the record with 'valid_iso2' in the identifier
    target_record = next(
        (
            record
            for record in records
            if "http://localhost:80/iso_2_waf/valid_iso2.xml" == record.identifier
        ),
        None,
    )

    if target_record is None:
        raise ValueError("Could not find record with 'valid_iso2.xml' in identifier")

    yield target_record


@pytest.fixture
def valid_iso_2_too_many_keywords_record(
    interface, organization_data, source_data_waf_iso19115_2, job_data_waf_iso19115_2
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_waf_iso19115_2)
    harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

    harvest_source = HarvestSource(harvest_job.id)
    harvest_source.acquire_minimum_external_data()
    external_records_to_process = harvest_source.external_records_to_process()
    records = list(external_records_to_process)

    # Filter for the record with 'valid_iso_too_many_keywords' in the identifier
    target_record = next(
        (
            record
            for record in records
            if "valid_iso_too_many_keywords" in record.identifier
        ),
        None,
    )

    if target_record is None:
        raise ValueError(
            "Could not find record with 'valid_iso_too_many_keywords' in identifier"
        )

    yield target_record


class TestValidateDataset:
    def test_validate_dcatus_federal(
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
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)
        assert test_record.validate()

    def test_validate_dcatus_non_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_single_record_non_federal,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record_non_federal)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)
        assert test_record.validate()

    def test_validate_dcatus3_0(
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
        external_records_to_process = harvest_source.external_records_to_process()

        test_record = next(external_records_to_process)
        assert test_record.validate()

    def test_invalid_dcatus3_0(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_invalid,
        job_data_dcatus3_0_invalid,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_invalid)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_invalid)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # the first dataset is missing the mandatory contactPoint
        test_record = next(external_records_to_process)
        assert not test_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert len(errors) == 1
        assert "contactPoint" in errors[0].message

    def test_validate_dcatus3_0_data_service(
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
        external_records_to_process = harvest_source.external_records_to_process()

        records = list(external_records_to_process)
        dataset_records = [r for r in records if r.record_type == "dataset"]
        service_records = [r for r in records if r.record_type == "data_service"]

        assert len(dataset_records) == 1
        assert len(service_records) == 2
        assert all(record.validate() for record in service_records)

    def test_invalid_dcatus3_0_data_service_missing_endpoint_url(
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
        external_records_to_process = harvest_source.external_records_to_process()

        service_record = next(
            r for r in external_records_to_process if r.record_type == "data_service"
        )

        # tamper with the raw payload to drop the mandatory endpointURL
        record = json.loads(service_record._source_raw)
        del record["endpointURL"]
        service_record._source_raw = json.dumps(record)

        assert not service_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert len(errors) == 1
        assert "endpointURL" in errors[0].message

    def test_validate_dcatus3_0_catalog_record(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_with_records,
        job_data_dcatus3_0_with_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_with_records)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_with_records)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        records = list(external_records_to_process)
        dataset_records = [r for r in records if r.record_type == "dataset"]
        catalog_records = [r for r in records if r.record_type == "catalog_record"]

        assert len(dataset_records) == 1
        assert len(catalog_records) == 2
        assert all(record.validate() for record in catalog_records)

    def test_invalid_dcatus3_0_catalog_record_missing_primary_topic(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_with_records,
        job_data_dcatus3_0_with_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_with_records)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_with_records)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        catalog_record = next(
            r for r in external_records_to_process if r.record_type == "catalog_record"
        )

        # tamper with the raw payload to drop the mandatory primaryTopic
        record = json.loads(catalog_record._source_raw)
        del record["primaryTopic"]
        catalog_record._source_raw = json.dumps(record)

        assert not catalog_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert len(errors) == 1
        assert "primaryTopic" in errors[0].message

    def test_invalid_license_uri_dcatus_non_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_bad_license_uri,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_bad_license_uri)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)

        assert not test_record.validate()

        # omitting the entire message for brevity
        # see the fixture for more details
        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]

        assert len(errors) == 1

        # ruff: noqa E501
        expected = "<ValidationError: \"$.license, 'center' does not match any of the acceptable formats: 'uri', 'null', '^(\\[\\[REDACTED).*?(\\]\\])$'\">"
        assert errors[0].message == expected

    def test_multiple_invalid(
        self,
        interface,
        organization_data,
        source_data_dcatus_multiple_invalid,
        job_data_dcatus_non_federal,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_multiple_invalid)
        harvest_job = interface.add_harvest_job(job_data_dcatus_non_federal)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # "cftc-dc1" is always the first one
        test_record = next(external_records_to_process)

        assert not test_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]
        assert len(errors) == 3

        # ruff: noqa E501
        expected = [
            "<ValidationError: '$.contactPoint.hasEmail, \\'ocagoadmin@oakgov.com\\' does not match any of the acceptable formats: \"^mailto:[\\w\\_\\~\\!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:.-]+@[\\w.-]+\\.[\\w.-]+?$\", \\'^(\\[\\[REDACTED).*?(\\]\\])$\\''>",
            "<ValidationError: \"$.distribution[0].accessURL, '//////not-a-url.example.com/' does not match any of the acceptable formats: 'uri', 'null', '^(\\[\\[REDACTED).*?(\\]\\])$'\">",
            "<ValidationError: \"$.license, 'center' does not match any of the acceptable formats: 'uri', 'null', '^(\\[\\[REDACTED).*?(\\]\\])$'\">",
        ]

        for i in range(len(errors)):
            assert errors[i].message.startswith(expected[i])

    def test_invalid_description_too_long_dcatus_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_long_description,
        job_data_dcatus_long_description,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_long_description)
        harvest_job = interface.add_harvest_job(job_data_dcatus_long_description)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # Get the first record
        test_record = next(external_records_to_process)

        assert not test_record.validate()

        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]

        assert len(errors) == 1

        # Check that the error is about description being too long
        expected_error_message = "does not match any of the acceptable formats: max string length of 10000 characters"
        assert expected_error_message in errors[0].message

    def test_none_in_dcatus_federal(
        self,
        interface,
        organization_data,
        source_data_dcatus_none_value,
        job_data_dcatus_long_description,
    ):
        """
        Test for the specific bug where the modified field was is Null
        which then gets translated to None, and creates a weird message. The
        format error string should not have duplicates of the same type.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_none_value)
        harvest_job = interface.add_harvest_job(job_data_dcatus_long_description)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # Get the first record
        test_record: Record = next(external_records_to_process)
        assert not test_record.validate()
        errors = [
            e[0] for e in interface.get_harvest_record_errors_by_job(harvest_job.id)
        ]

        # the bug we're testing for is modified is Null
        assert len(errors) == 1

        expected_error_message = "<ValidationError: \"$.modified, None does not match any of the acceptable formats: 'string'\">"
        assert expected_error_message == errors[0].message

    def test_valid_transformed_iso(
        self,
        valid_iso_2_record,
    ):
        valid_iso_2_record.transform()
        assert valid_iso_2_record.validate()

    def test_invalid_transformed_iso(
        self,
        interface,
        valid_iso_2_record,
    ):
        valid_iso_2_record.transform()

        # we increased our contactPoint options in mdtranslator
        # so this actually gets pulled so deleting it here
        del valid_iso_2_record.transformed_data["contactPoint"]

        # validator logs exceptions when the dataset is invalid
        valid_iso_2_record.validate()
        errors = [
            e[0]  # returns a tuple, first is the error
            for e in interface.get_harvest_record_errors_by_job(
                valid_iso_2_record.harvest_source.job_id
            )
        ]

        # 'ExternalRecordToClass' caused by decoding error. not needed for this test.
        del errors[0]

        assert (
            errors[0].message
            == """<ValidationError: "$, 'contactPoint' is a required property">"""
        )

    def test_invalid_transformed_iso_too_many_keywords(
        self,
        interface,
        valid_iso_2_too_many_keywords_record,
    ):
        # Transform the ISO record
        valid_iso_2_too_many_keywords_record.transform()
        valid_iso_2_too_many_keywords_record.fill_placeholders()

        # This should fail validation due to too many keywords (2518 > 1000 limit)
        assert not valid_iso_2_too_many_keywords_record.validate()

        errors = [
            e[0]  # returns a tuple, first is the error
            for e in interface.get_harvest_record_errors_by_job(
                valid_iso_2_too_many_keywords_record.harvest_source.job_id
            )
        ]

        # 'ExternalRecordToClass' caused by decoding error. not needed for this test.
        del errors[0]

        assert len(errors) == 1

        # Check that the error is about having too many keywords
        expected_error_message = (
            "does not match any of the acceptable formats: max 1000 items"
        )
        assert expected_error_message in errors[0].message

    def test_transformed_iso_contact_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["contactPoint"]

        # now fill in the missing contactPoint
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()
        assert (
            "@gsa.gov"
            in valid_iso_2_record.transformed_data["contactPoint"]["hasEmail"]
        )
        assert valid_iso_2_record.transformed_data["contactPoint"][
            "hasEmail"
        ].startswith("mailto:")

    def test_transformed_iso_description_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["description"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert "No description" in valid_iso_2_record.transformed_data["description"]

    def test_transformed_iso_keyword_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["keyword"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert len(valid_iso_2_record.transformed_data["keyword"]) == 1
        assert valid_iso_2_record.transformed_data["keyword"][0] == "__"

    def test_transformed_iso_publisher_placeholder(
        self, organization_data, valid_iso_2_record
    ):
        valid_iso_2_record.transform()
        del valid_iso_2_record.transformed_data["publisher"]

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert valid_iso_2_record.transformed_data["publisher"] == {
            "name": organization_data["name"]
        }

    def test_transformed_iso_downloadURL_placeholder(
        self, organization_data, valid_iso_2_record
    ):
        valid_iso_2_record.transform()
        valid_iso_2_record.transformed_data["distribution"][0][
            "downloadURL"
        ] = "www.example.com/"
        # makes the record invalid
        assert not valid_iso_2_record.validate()

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert (
            valid_iso_2_record.transformed_data["distribution"][0]["downloadURL"]
            == "https://www.example.com/"
        )

    def test_transformed_iso_accessURL_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()
        valid_iso_2_record.transformed_data["distribution"][0][
            "accessURL"
        ] = "www.example.com/"
        # makes the record invalid
        assert not valid_iso_2_record.validate()

        # now fill in the missing items
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()

        assert (
            valid_iso_2_record.transformed_data["distribution"][0]["accessURL"]
            == "https://www.example.com/"
        )

    def test_transformed_iso_root_describedByType_placeholder(self, valid_iso_2_record):
        valid_iso_2_record.transform()

        # if `describedByType` is not in the root its valid
        assert valid_iso_2_record.validate()

        # test for invalid describedByType
        valid_iso_2_record.transformed_data["describedByType"] = (
            "WWW:LINK-1.0-http--link"
        )
        assert not valid_iso_2_record.validate()

        # replace invalid describedByType with placeholder
        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()
        assert (
            valid_iso_2_record.transformed_data["describedByType"]
            == "application/octet-stream"
        )

    def test_transformed_iso_distribution_describedByType_placeholder(
        self, valid_iso_2_record
    ):
        valid_iso_2_record.transform()
        valid_iso_2_record.transformed_data["distribution"][0][
            "describedByType"
        ] = "WWW:LINK-1.0-http--link"
        assert not valid_iso_2_record.validate()

        valid_iso_2_record.fill_placeholders()
        assert valid_iso_2_record.validate()
        assert (
            valid_iso_2_record.transformed_data["distribution"][0]["describedByType"]
            == "application/octet-stream"
        )


class TestValidateWarnings:
    """DCAT-US 3 warning detection wired into Record.validate (#6128)."""

    def _first_dcatus3_record(
        self, interface, organization_data, source_data, job_data
    ) -> Record:
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data)
        harvest_job = interface.add_harvest_job(job_data)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()
        return next(harvest_source.external_records_to_process())

    def test_valid_record_with_warning(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0,
        job_data_dcatus3_0,
    ):
        """A valid dcatus3.0 record with warning-triggering content still
        validates, is counted as both validated and warned, and its warning is
        stored at severity="warning" (excluded from the default error query)."""
        test_record = self._first_dcatus3_record(
            interface, organization_data, source_data_dcatus3_0, job_data_dcatus3_0
        )

        # an unknown ISO 639-1 language is a content warning, not a schema error
        record = json.loads(test_record.source_raw)
        record["language"] = ["zz"]
        test_record._source_raw = json.dumps(record)

        assert test_record.validate()

        harvest_source = test_record.harvest_source
        assert harvest_source.reporter.validated == 1
        assert harvest_source.reporter.warned == 1
        assert harvest_source.reporter.errored == 0

        # the count is persisted to the harvest_job row, not just the reporter
        harvest_job = interface.get_harvest_job(harvest_source.job_id)
        assert harvest_job.records_warned == 1

        issues = interface.get_harvest_record_errors_by_job(harvest_source.job_id)
        assert len(issues) == 1

        errors = interface.get_harvest_record_errors_by_job(
            harvest_source.job_id, severity="error"
        )
        assert len(errors) == 0

        warnings = interface.get_harvest_record_errors_by_job(
            harvest_source.job_id, severity="warning"
        )
        assert len(warnings) == 1
        assert warnings[0][0].type == "invalid_language"
        assert warnings[0][0].severity == "warning"

    def test_invalid_record_with_warning_preserves_error_status(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_invalid,
        job_data_dcatus3_0_invalid,
    ):
        """An invalid record that also warns is counted as errored and warned.

        The error and warning must come from independent defects (missing
        `contactPoint` vs an unknown `language` code), not the same value twice.
        """
        test_record = self._first_dcatus3_record(
            interface,
            organization_data,
            source_data_dcatus3_0_invalid,
            job_data_dcatus3_0_invalid,
        )

        # give the record a real db id so we can assert its persisted status
        test_record.compare()

        record = json.loads(test_record.source_raw)
        record["language"] = ["zz"]
        test_record._source_raw = json.dumps(record)

        assert not test_record.validate()

        harvest_source = test_record.harvest_source
        assert harvest_source.reporter.errored == 1
        assert harvest_source.reporter.warned == 1
        assert harvest_source.reporter.validated == 0

        # the warning write must not have overwritten the error status
        stored = interface.get_harvest_record(test_record.id)
        assert stored.status == "error"

        warnings = interface.get_harvest_record_errors_by_job(
            harvest_source.job_id, severity="warning"
        )
        assert any(w[0].type == "invalid_language" for w in warnings)

    def test_warnings_are_segregated_per_record_end_to_end(
        self,
        interface,
        organization_data,
        source_data_dcatus3_0_warning,
        job_data_dcatus3_0_warning,
    ):
        """Warnings from a real harvest land on the right records at the right
        severity, with no in-memory patching of source_raw.

        The other tests in this class mutate a single record's source_raw to
        trip a rule. This one harvests a fixture authored to warn
        (example_data/dcatus/dcatus3_0_warning.json), so it also covers what
        those cannot: that warnings are attributed per record rather than
        pooled on the job, and that a warning-free record in the same job
        stays clean. get_harvest_record_errors_by_record is the call that
        backs GET /api/harvest_record/<id>/errors.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus3_0_warning)
        harvest_job = interface.add_harvest_job(job_data_dcatus3_0_warning)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_external_data()

        # compare() gives each record a db id, so warnings can be traced back
        # to the record that caused them
        records = {}
        for record in harvest_source.external_records_to_process():
            record.compare()
            assert record.validate(), f"{record.identifier} should be schema-valid"
            records[record.identifier] = record

        assert len(records) == 3

        # every dataset in the fixture is schema-valid: warnings only
        assert harvest_source.reporter.errored == 0
        assert harvest_source.reporter.validated == 3
        assert harvest_source.reporter.warned == 2

        stored_job = interface.get_harvest_job(harvest_job.id)
        assert stored_job.records_warned == 2

        # job-level reads default to all issues (#799), so the warnings show up
        # there; narrowing to errors is what comes back empty
        job_issues = interface.get_harvest_record_errors_by_job(harvest_job.id)
        assert {issue[0].severity for issue in job_issues} == {"warning"}
        assert (
            interface.get_harvest_record_errors_by_job(harvest_job.id, severity="error")
            == []
        )

        warned = {
            "https://example.gov/datasets/warning-one": "date_out_of_order",
            "https://example.gov/datasets/warning-two": "invalid_media_type",
        }
        for identifier, expected_type in warned.items():
            record_id = records[identifier].id
            warnings = interface.get_harvest_record_errors_by_record(
                record_id, severity="warning"
            )
            assert [w.type for w in warnings] == [expected_type]
            assert warnings[0].severity == "warning"

            # the same record has no errors, so the default read is empty
            assert interface.get_harvest_record_errors_by_record(record_id) == []

        # the clean record in the same job carries no issues at either severity
        clean_id = records["https://example.gov/datasets/warning-three"].id
        assert interface.get_harvest_record_errors_by_record(clean_id) == []
        assert (
            interface.get_harvest_record_errors_by_record(clean_id, severity="warning")
            == []
        )

    def test_non_dcatus3_record_is_not_warned(
        self,
        interface,
        organization_data,
        source_data_dcatus_single_record_non_federal,
        job_data_dcatus_non_federal,
    ):
        """Warning detection is gated on schema_type == "dcatus3.0"; a 1.1
        record is never walked and never counted as warned."""
        test_record = self._first_dcatus3_record(
            interface,
            organization_data,
            source_data_dcatus_single_record_non_federal,
            job_data_dcatus_non_federal,
        )

        test_record.validate()

        harvest_source = test_record.harvest_source
        assert harvest_source.reporter.warned == 0

        warnings = interface.get_harvest_record_errors_by_job(
            harvest_source.job_id, severity="warning"
        )
        assert len(warnings) == 0
