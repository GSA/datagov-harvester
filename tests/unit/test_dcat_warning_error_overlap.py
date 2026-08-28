"""Lock in the schema/warning boundary from GSA/data.gov#6243.

A value that trips a DCAT warning must be schema-valid, and a value that
trips a schema error must produce no warning. Checked against the real
Draft202012Validator from `_external/dcat-us/jsonschema/definitions/`, so a
future rule that duplicates a schema constraint fails at runtime.
"""

import copy

import pytest

from harvester.utils.dcat_warnings import detect_dcat_warnings
from harvester.utils.general_utils import (
    assemble_validation_errors,
    build_dcatus3_validator,
    open_json,
)
from harvester.utils.schema_paths import (
    DCATUS3_COMPLETE_EXAMPLE,
    DCATUS3_DEFINITIONS_DIR,
)

DATASET_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataset"

# Building the validator compiles every DCAT-US 3 definition; build it once
# per module rather than once per test.
VALIDATOR = build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR, root_ref=DATASET_REF)

# The upstream complete example is schema-valid but not warning-free: its
# vanity tel number ("+1-555-CLIMATE") already trips a warning (invalid_tel).
# That's a separate, pre-existing false-positive question and out of scope
# here, so every test below diffs against this baseline instead of assuming
# a record starts with zero warnings. (Its WKT `geometry` used to trip a
# second baseline warning, unresolvable_spatial_value, until WKT support
# landed in GSA/data.gov#6264.)
#
# Baseline warnings are deliberately *not* precomputed into a module-level
# constant here (unlike VALIDATOR above). Detecting warnings can exercise
# `translate_spatial`'s DB-backed lookup fallback, which tests/conftest.py's
# autouse `default_function_fixture` only points at a safe, per-test,
# rolled-back session once a test is actually running. Computing it at
# import time (module collection, before any test/fixture runs) would
# instead reach the real module-level `harvester.db_interface` and leak an
# uncommitted transaction. So `_new_warning_types` recomputes the baseline
# fresh on every call, inside the test.
BASE_RECORD = open_json(DCATUS3_COMPLETE_EXAMPLE)


def _record() -> dict:
    """A fresh deep copy of the base record for a test to mutate."""
    return copy.deepcopy(BASE_RECORD)


def _new_warning_types(mutated: dict) -> list:
    """Warning types produced by `mutated` that are not already produced by
    the unmutated base record."""
    baseline = detect_dcat_warnings(BASE_RECORD)
    produced = detect_dcat_warnings(mutated)
    return [w.warning_type for w in produced if w not in baseline]


def _schema_errors(mutated: dict) -> list:
    return assemble_validation_errors(VALIDATOR.iter_errors(mutated))


# --- Case table 1: every warning fires only on schema-clean data -----------


def _duplicate_keyword(r):
    r["keyword"][1] = r["keyword"][0]


def _invalid_spatial_resolution(r):
    r["spatialResolutionInMeters"] = "about 1km"


def _invalid_temporal_resolution(r):
    r["temporalResolution"] = "daily"


def _legacy_access_rights(r):
    r["accessRights"] = "non-public"


def _date_out_of_order(r):
    r["created"] = "2025-06-01"  # later than modified and issued


def _invalid_language(r):
    r["language"] = ["zz"]  # 2 chars, not a recognized ISO 639-1 code


def _invalid_byte_size(r):
    r["distribution"][0]["byteSize"] = "big"


def _invalid_character_encoding(r):
    r["distribution"][0]["characterEncoding"] = ["bogus"]


def _invalid_media_type(r):
    r["distribution"][0]["mediaType"] = "application/notreal"


def _invalid_restriction_status(r):
    r["distribution"][0]["accessRestriction"] = [
        {"@type": "AccessRestriction", "restrictionStatus": "Bogus"}
    ]


def _invalid_specific_restriction(r):
    # "Copyright" is a recognized *use* restriction term but not an access one.
    r["distribution"][0]["accessRestriction"] = [
        {
            "@type": "AccessRestriction",
            "restrictionStatus": "Unrestricted",
            "specificRestriction": "Copyright",
        }
    ]


def _invalid_tel(r):
    # Distinct from the baseline vanity number so this shows up as a new warning.
    r["contactPoint"][0]["tel"] = "1-800-CALLME"


def _invalid_postal_code(r):
    r["contactPoint"][0]["address"][0]["postal-code"] = "K1A0B1"


def _empty_address(r):
    r["contactPoint"][0]["address"].append({"@type": "Address"})


def _invalid_expected_data_type(r):
    r["hasQualityMeasurement"][0]["isMeasurementOf"]["expectedDataType"] = "decimal"


def _invalid_cui_banner_marking(r):
    r["distribution"][0]["cuiRestriction"] = {
        "@type": "CUIRestriction",
        "cuiBannerMarking": "SP-CTI",
        "designationIndicator": "Controlled by: Agency XYZ",
    }


def _unresolvable_spatial_value(r):
    # Distinct from the baseline WKT polygon so this shows up as a new warning.
    r["spatial"][0]["geometry"] = "somewhere over there"


def _date_out_of_order_date_time_with_utc_offset(r):
    # "Z" is schema-valid RFC 3339; the date parser must still warn on it.
    r["created"] = "2025-01-01T00:00:00Z"  # later than modified and issued


def _date_out_of_order_lowercase_rfc3339(r):
    # Lowercase RFC 3339 "t"/"z" is schema-valid and must still warn.
    r["created"] = "2025-01-01t00:00:00z"  # later than modified and issued


def _empty_address_explicit_empty_strings(r):
    # Explicit "" is still schema-valid and must still warn as empty.
    r["contactPoint"][0]["address"].append(
        {
            "@type": "Address",
            "street-address": "",
            "locality": "",
            "region": "",
            "postal-code": "",
            "country-name": "",
        }
    )


WARNING_ON_CLEAN_DATA_CASES = [
    pytest.param(_duplicate_keyword, "duplicate_keyword", id="duplicate_keyword"),
    pytest.param(
        _invalid_spatial_resolution,
        "invalid_spatial_resolution",
        id="invalid_spatial_resolution",
    ),
    pytest.param(
        _invalid_temporal_resolution,
        "invalid_temporal_resolution",
        id="invalid_temporal_resolution",
    ),
    pytest.param(
        _legacy_access_rights, "legacy_access_rights", id="legacy_access_rights"
    ),
    pytest.param(_date_out_of_order, "date_out_of_order", id="date_out_of_order"),
    pytest.param(_invalid_language, "invalid_language", id="invalid_language"),
    pytest.param(_invalid_byte_size, "invalid_byte_size", id="invalid_byte_size"),
    pytest.param(
        _invalid_character_encoding,
        "invalid_character_encoding",
        id="invalid_character_encoding",
    ),
    pytest.param(_invalid_media_type, "invalid_media_type", id="invalid_media_type"),
    pytest.param(
        _invalid_restriction_status,
        "invalid_restriction_status",
        id="invalid_restriction_status",
    ),
    pytest.param(
        _invalid_specific_restriction,
        "invalid_specific_restriction",
        id="invalid_specific_restriction",
    ),
    pytest.param(_invalid_tel, "invalid_tel", id="invalid_tel"),
    pytest.param(_invalid_postal_code, "invalid_postal_code", id="invalid_postal_code"),
    pytest.param(_empty_address, "empty_address", id="empty_address"),
    pytest.param(
        _invalid_expected_data_type,
        "invalid_expected_data_type",
        id="invalid_expected_data_type",
    ),
    pytest.param(
        _invalid_cui_banner_marking,
        "invalid_cui_banner_marking",
        id="invalid_cui_banner_marking",
    ),
    pytest.param(
        _unresolvable_spatial_value,
        "unresolvable_spatial_value",
        id="unresolvable_spatial_value",
    ),
    pytest.param(
        _date_out_of_order_date_time_with_utc_offset,
        "date_out_of_order",
        id="date_out_of_order_date_time_with_utc_offset",
    ),
    pytest.param(
        _empty_address_explicit_empty_strings,
        "empty_address",
        id="empty_address_explicit_empty_strings",
    ),
    pytest.param(
        _date_out_of_order_lowercase_rfc3339,
        "date_out_of_order",
        id="date_out_of_order_lowercase_rfc3339",
    ),
]


class TestWarningsOnlyFireOnSchemaCleanData:
    @pytest.mark.parametrize("mutate, expected_type", WARNING_ON_CLEAN_DATA_CASES)
    def test_warning_fires_without_a_schema_error(self, mutate, expected_type):
        mutated = _record()
        mutate(mutated)

        # Would fail if a warning rule restated a schema constraint.
        assert _schema_errors(mutated) == []

        assert expected_type in _new_warning_types(mutated)


# --- Case table 2: schema violations produce no warning ---------------------


def _bad_dataset_id(r):
    r["@id"] = "qwer"  # the exact case from GSA/data.gov#6243


def _bad_relation_iri(r):
    r["relation"] = ["not a valid iri"]


def _bad_is_referenced_by_iri(r):
    r["isReferencedBy"] = ["not a valid iri"]


def _bad_image_iri(r):
    r["image"] = "not a valid iri"


def _string_entry_in_conforms_to(r):
    r["conformsTo"][0] = "just a string, not a Standard object"


def _language_array_of_three_char_codes(r):
    r["language"] = ["eng"]


def _non_string_spatial_resolution_in_meters(r):
    r["spatialResolutionInMeters"] = 1000


def _non_string_temporal_resolution(r):
    r["temporalResolution"] = 1


def _non_empty_list_byte_size(r):
    r["distribution"][0]["byteSize"] = ["524288000"]


def _bare_string_character_encoding(r):
    r["distribution"][0]["characterEncoding"] = "UTF-8"


def _blank_keywords(r):
    r["keyword"] = ["", ""]


def _distribution_type_dict(r):
    # Non-string @type used to raise TypeError: unhashable type.
    r["distribution"][0]["@type"] = {"foo": "bar"}


def _distribution_type_list(r):
    r["distribution"][0]["@type"] = ["Distribution"]


def _spatial_geometry_non_string_non_dict(r):
    r["spatial"][0]["geometry"] = 5


def _spatial_geometry_dict_missing_required_keys(r):
    r["spatial"][0]["geometry"] = {"foo": 1}


def _spatial_bbox_non_string_non_dict(r):
    r["spatial"][0]["bbox"] = 5


def _created_date_time_without_utc_offset(r):
    r["created"] = "2025-01-01T00:00:00"


def _address_replaced_with_non_string_postal_code_zero(r):
    r["contactPoint"][0]["address"] = [{"@type": "Address", "postal-code": 0}]


def _address_replaced_with_non_string_postal_code_empty_list(r):
    r["contactPoint"][0]["address"] = [{"@type": "Address", "postal-code": []}]


def _spatial_resolution_in_meters_non_empty_list(r):
    r["spatialResolutionInMeters"] = ["bad"]


def _temporal_resolution_non_empty_dict(r):
    r["temporalResolution"] = {"a": 1}


def _nested_container_type_error_under_anyof(r):
    r["contactPoint"][0]["@type"] = ["Kind"]


def _bbox_wrong_type(r):
    # bbox objects must be type "Polygon"; anything else is a schema const error.
    r["spatial"][0]["bbox"] = {
        "type": "NoSuch",
        "coordinates": [[[-77.1, 38.7], [-76.9, 38.7], [-76.9, 38.9], [-77.1, 38.7]]],
    }


def _padded_date(r):
    # Whitespace-padded dates are schema-invalid; must not be stripped and warned on.
    r["created"] = " 2025-06-01 "  # later than modified and issued, if parsed


def _created_all_scalar_anyof_branches(r):
    r["created"] = ["2025"]


def _language_all_scalar_anyof_branches(r):
    r["language"] = {"a": 1}


def _spatial_bbox_all_scalar_anyof_branches(r):
    r["spatial"][0]["bbox"] = ["x"]


def _access_rights_all_scalar_anyof_branches(r):
    r["accessRights"] = ["x"]


def _centroid_coordinates_maxitems_numeric(r):
    r["spatial"][0]["centroid"] = {"type": "Point", "coordinates": [-77, 38, 1]}


def _centroid_coordinates_minitems_numeric(r):
    r["spatial"][0]["centroid"] = {"type": "Point", "coordinates": [-77]}


SCHEMA_VIOLATION_CASES = [
    pytest.param(_bad_dataset_id, id="bad_dataset_id"),
    pytest.param(_bad_relation_iri, id="bad_relation_iri"),
    pytest.param(_bad_is_referenced_by_iri, id="bad_is_referenced_by_iri"),
    pytest.param(_bad_image_iri, id="bad_image_iri"),
    pytest.param(_string_entry_in_conforms_to, id="string_entry_in_conforms_to"),
    pytest.param(
        _language_array_of_three_char_codes,
        id="language_array_of_three_char_codes",
    ),
    pytest.param(
        _non_string_spatial_resolution_in_meters,
        id="non_string_spatial_resolution_in_meters",
    ),
    pytest.param(_non_string_temporal_resolution, id="non_string_temporal_resolution"),
    pytest.param(_non_empty_list_byte_size, id="non_empty_list_byte_size"),
    pytest.param(_bare_string_character_encoding, id="bare_string_character_encoding"),
    pytest.param(_blank_keywords, id="blank_keywords"),
    pytest.param(_distribution_type_dict, id="distribution_type_dict"),
    pytest.param(_distribution_type_list, id="distribution_type_list"),
    pytest.param(
        _spatial_geometry_non_string_non_dict,
        id="spatial_geometry_non_string_non_dict",
    ),
    pytest.param(
        _spatial_geometry_dict_missing_required_keys,
        id="spatial_geometry_dict_missing_required_keys",
    ),
    pytest.param(
        _spatial_bbox_non_string_non_dict, id="spatial_bbox_non_string_non_dict"
    ),
    pytest.param(
        _created_date_time_without_utc_offset,
        id="created_date_time_without_utc_offset",
    ),
    pytest.param(
        _address_replaced_with_non_string_postal_code_zero,
        id="address_replaced_with_non_string_postal_code_zero",
    ),
    pytest.param(
        _address_replaced_with_non_string_postal_code_empty_list,
        id="address_replaced_with_non_string_postal_code_empty_list",
    ),
    pytest.param(
        _spatial_resolution_in_meters_non_empty_list,
        id="spatial_resolution_in_meters_non_empty_list",
    ),
    pytest.param(
        _temporal_resolution_non_empty_dict,
        id="temporal_resolution_non_empty_dict",
    ),
    pytest.param(
        _nested_container_type_error_under_anyof,
        id="nested_container_type_error_under_anyof",
    ),
    pytest.param(_bbox_wrong_type, id="bbox_wrong_type"),
    pytest.param(_padded_date, id="padded_date"),
    pytest.param(
        _created_all_scalar_anyof_branches,
        id="created_all_scalar_anyof_branches",
    ),
    pytest.param(
        _language_all_scalar_anyof_branches,
        id="language_all_scalar_anyof_branches",
    ),
    pytest.param(
        _spatial_bbox_all_scalar_anyof_branches,
        id="spatial_bbox_all_scalar_anyof_branches",
    ),
    pytest.param(
        _access_rights_all_scalar_anyof_branches,
        id="access_rights_all_scalar_anyof_branches",
    ),
    pytest.param(
        _centroid_coordinates_maxitems_numeric,
        id="centroid_coordinates_maxitems_numeric",
    ),
    pytest.param(
        _centroid_coordinates_minitems_numeric,
        id="centroid_coordinates_minitems_numeric",
    ),
]


class TestSchemaViolationsProduceNoWarning:
    @pytest.mark.parametrize("mutate", SCHEMA_VIOLATION_CASES)
    def test_schema_violation_has_no_matching_warning(self, mutate):
        mutated = _record()
        mutate(mutated)

        assert _schema_errors(mutated), "mutation should be a schema violation"

        assert _new_warning_types(mutated) == []
