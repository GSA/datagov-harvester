import json

import pytest

from dcatus_validation.messages import build_dcatus3_validator
from dcatus_validation.validate import (
    NESTING_TOO_DEEP_MESSAGE,
    CatalogTooDeeplyNested,
    validate_records,
    validate_records_limited,
)


class TestValidateRecords:
    def test_dcatus1_1_errors_are_keyed_by_identifier(
        self, dcatus_bad_license_uri_json
    ):
        errors = validate_records(
            json.loads(dcatus_bad_license_uri_json), "dcatus1.1: non-federal dataset"
        )

        assert errors == [
            (
                "https://www.arcgis.com/home/item.html?id=99731bb0369848169d98f31ce83fb0e2",  # noqa: E501
                "$.license, 'center' does not match any of the acceptable formats: 'uri', 'null', '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",  # noqa: E501
            )
        ]

    def test_dcatus1_1_falls_back_to_dataset_position(self, dcatus_no_identifier_json):
        errors = validate_records(
            json.loads(dcatus_no_identifier_json), "dcatus1.1: non-federal dataset"
        )

        assert errors == [(0, "$, 'identifier' is a required property")]

    def test_dcatus3_errors_carry_the_json_path(
        self, dcatus_3_catalog_missing_identifier
    ):
        errors = validate_records(
            json.loads(dcatus_3_catalog_missing_identifier), "dcatus3.0 catalog"
        )

        assert errors == [("", "$.dataset[0], 'identifier' is a required property")]

    def test_recursion_is_reported_as_too_deeply_nested(self):
        catalog = {"@type": "Catalog", "title": "t", "description": "d", "dataset": []}
        for _ in range(200):
            catalog = {**catalog, "catalog": [catalog]}

        with pytest.raises(CatalogTooDeeplyNested, match=NESTING_TOO_DEEP_MESSAGE):
            validate_records(catalog, "dcatus3.0 catalog")

    def test_missing_dcatus3_definitions_say_how_to_fix_it(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="git submodule update"):
            build_dcatus3_validator(tmp_path)

    @pytest.mark.parametrize(
        ("catalog", "expected"),
        [
            ({}, "$, 'dataset' is a required property"),
            ([], "$, [] does not match any of the acceptable formats: 'object'"),
            (
                {"dataset": {}},
                "$.dataset, object value does not match any of the acceptable "
                "formats: 'array'",
            ),
            (
                {"dataset": [1]},
                "$.dataset[0], 1 does not match any of the acceptable "
                "formats: 'object'",
            ),
            (
                {"dataset": [True]},
                "$.dataset[0], True does not match any of the acceptable "
                "formats: 'object'",
            ),
        ],
    )
    def test_dcatus1_1_catalog_structure_is_a_validation_error(self, catalog, expected):
        assert validate_records(catalog, "dcatus1.1: federal dataset") == [
            ("", expected)
        ]

    def test_invalid_identifier_falls_back_to_dataset_position(self):
        errors = validate_records(
            {"dataset": [{"identifier": {"not": "a string"}}]},
            "dcatus1.1: federal dataset",
        )

        assert errors
        assert {identifier for identifier, _ in errors} == {0}

    def test_direct_validation_remains_uncapped(self):
        catalog = {"dataset": [{} for _ in range(101)]}

        errors = validate_records(catalog, "dcatus1.1: federal dataset")

        assert len(errors) == 1010

    def test_limited_validation_reports_when_it_stops(self):
        catalog = {"dataset": [{} for _ in range(101)]}

        errors, incomplete = validate_records_limited(
            catalog,
            "dcatus1.1: federal dataset",
            max_errors=1000,
        )

        assert len(errors) == 1000
        assert incomplete is True
