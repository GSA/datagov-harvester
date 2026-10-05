import json

import pytest

from dcatus_validation.messages import build_dcatus3_validator
from dcatus_validation.validate import (
    NESTING_TOO_DEEP_MESSAGE,
    CatalogTooDeeplyNested,
    validate_records,
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
