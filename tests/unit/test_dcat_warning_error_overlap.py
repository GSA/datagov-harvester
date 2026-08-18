"""Lock in the schema/warning boundary from GSA/data.gov#6243.

GSA/data.gov#6243 reported a single dataset-quality defect twice: once as a
schema `ValidationError` (severity=error) and again as a DCAT content
`warning` (severity=warning), inflating both `records_errored` and
`records_warned`. The fix removed the warning rules that restated schema
constraints (the `invalid_iri` family, plus guards that now defer to the
schema's type/format/length/enum checks).

The invariant this module locks in place: **any value that trips a DCAT
warning must be schema-valid, and any value that trips a schema error must
produce no warning.** Warnings are for content the schema accepts; the
validator owns type/format/length/enum.

This is checked against the *real* `Draft202012Validator` built from
`schemas/dcatus3.0/definitions/`, not by inspecting `dcat_warnings.py` source,
because the whole point is to catch a *future* warning rule that
(re)duplicates a schema constraint without anyone noticing the overlap by
reading the code. Only a rule and a validator disagreeing at runtime proves
the boundary holds.
"""

import copy
from pathlib import Path

import pytest

from harvester.utils.dcat_warnings import detect_dcat_warnings
from harvester.utils.general_utils import (
    assemble_validation_errors,
    build_dcatus3_validator,
    open_json,
)

ROOT_DIR = Path(__file__).parents[2]
DCATUS3_DEFINITIONS = ROOT_DIR / "schemas" / "dcatus3.0" / "definitions"
COMPLETE_EXAMPLE = (
    ROOT_DIR
    / "schemas"
    / "dcatus3.0"
    / "examples"
    / "Dataset"
    / "good"
    / "complete_example.json"
)
DATASET_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataset"

# Building the validator compiles every DCAT-US 3 definition; build it once
# per module rather than once per test.
VALIDATOR = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)

# The upstream complete example is schema-valid but not warning-free: its
# vanity tel number ("+1-555-CLIMATE") and its WKT `geometry` each already
# trip a warning (invalid_tel, unresolvable_spatial_value). Those are a
# separate, pre-existing false-positive question and out of scope here, so
# every test below diffs against this baseline instead of assuming a record
# starts with zero warnings.
#
# Baseline warnings are deliberately *not* precomputed into a module-level
# constant here (unlike VALIDATOR above). Detecting the unresolvable-spatial
# baseline warning exercises `translate_spatial`'s DB-backed lookup fallback,
# which tests/conftest.py's autouse `default_function_fixture` only points at
# a safe, per-test, rolled-back session once a test is actually running.
# Computing it at import time (module collection, before any test/fixture
# runs) would instead reach the real module-level `harvester.db_interface`
# and leak an uncommitted transaction. So `_new_warning_types` recomputes the
# baseline fresh on every call, inside the test.
BASE_RECORD = open_json(COMPLETE_EXAMPLE)


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
#
# One mutation per warning type still emitted by dcat_warnings.py (see
# _TYPE_RULES and the rule functions). Each mutation is applied to a fresh
# deep copy of the schema-valid base record and must itself remain
# schema-valid; if it doesn't, assertion (a) below is the one that would
# catch a rule that restates a schema constraint.


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
    # A different letters-containing value than the baseline's, so the
    # resulting warning message differs from the baseline one and shows up
    # as new rather than being diffed away.
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
    # A different unresolvable value than the baseline's WKT polygon, so the
    # resulting warning message is new rather than being diffed away.
    r["spatial"][0]["geometry"] = "somewhere over there"


def _date_out_of_order_date_time_with_utc_offset(r):
    # Positive counterpart to fix (c): a `date-time` value *with* a UTC
    # offset ("Z") is schema-valid, so `_parse_dcat_date` must still parse it
    # and still warn -- the guard must not have been over-tightened into
    # rejecting every date-time string.
    r["created"] = "2025-01-01T00:00:00Z"  # later than modified and issued


def _date_out_of_order_lowercase_rfc3339(r):
    # Third review round, fix 3: a lowercase RFC 3339 date-time ("t"/"z") is
    # still schema-valid (FormatChecker's `date-time` format is
    # case-insensitive on both), but `_parse_dcat_date` only normalized an
    # uppercase "Z" before handing the value to `datetime.fromisoformat`, so
    # a lowercase one raised inside the `try`, was swallowed by the `except`,
    # and produced neither a schema error nor a warning -- the defect was
    # reported nowhere.
    r["created"] = "2025-01-01t00:00:00z"  # later than modified and issued


def _empty_address_explicit_empty_strings(r):
    # Positive counterpart to fix (d): every field explicitly set to "" (not
    # just omitted) is still schema-valid and must still warn -- the guard
    # must not have been over-tightened into only recognizing an absent/None
    # field as unpopulated.
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

        # (a) the mutation must be schema-clean: this is the assertion that
        # would fail if a warning rule restated a schema constraint.
        assert _schema_errors(mutated) == []

        # (b) the mutation must still produce the warning it was meant to.
        assert expected_type in _new_warning_types(mutated)


# --- Case table 2: schema violations produce no warning ---------------------
#
# Mutations that are pure schema violations of fields dcat_warnings.py also
# touches. Each must (a) actually be reported as a schema error and (b)
# produce no new warning, confirming the defect is reported exactly once.


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
    # pins fix (a): before it, `detect_dcat_warnings` raised
    # `TypeError: unhashable type` on a non-string, non-hashable `@type`
    # instead of returning cleanly.
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
    # rescued from silent loss by fix (e): before it, `assemble_validation_errors`
    # reported zero errors for this list value even though the record is invalid.
    r["spatialResolutionInMeters"] = ["bad"]


def _temporal_resolution_non_empty_dict(r):
    # same silent-loss defect as above, for a dict-valued offender.
    r["temporalResolution"] = {"a": 1}


def _nested_container_type_error_under_anyof(r):
    # Third review round, fix 1: a genuine leaf `type` error reached through
    # one branch of a decomposed `anyOf` (here, `contactPoint`'s array
    # branch) used to be dropped entirely -- `found_simple_message` only
    # recognized a container `type` error as simple when it had no parent,
    # which excluded this one -- so `assemble_validation_errors` returned no
    # error at all for a schema-invalid record.
    r["contactPoint"][0]["@type"] = ["Kind"]


def _bbox_wrong_type(r):
    # Third review round, fix 2: bbox's object variant pins "type" to the
    # constant "Polygon" (Location.json); a dict with any other "type" is
    # already a schema `const` error, but `_warn_spatial_unresolved` used to
    # only check for the presence of "type"/"coordinates", so it also fired
    # `unresolvable_spatial_value` on this.
    r["spatial"][0]["bbox"] = {
        "type": "NoSuch",
        "coordinates": [[[-77.1, 38.7], [-76.9, 38.7], [-76.9, 38.9], [-77.1, 38.7]]],
    }


def _padded_date(r):
    # Third review round, fix 3: `_parse_dcat_date` used to strip whitespace
    # before format-checking, so a padded value the schema rejects still
    # parsed here and could produce `date_out_of_order`.
    r["created"] = " 2025-06-01 "  # later than modified and issued, if parsed


def _created_all_scalar_anyof_branches(r):
    # Remaining bug from review: `created` is `anyOf[null, {format:
    # date-time}, {format: date}, {pattern: "^[0-9]{4}$"}, {pattern:
    # "^[0-9]{4}-[0-9]{2}$"}]` -- every alternative is scalar/null, so a list
    # value decomposes into same-path `type` errors with no sibling cause to
    # defer to. Before the forced fallback in `assemble_validation_errors`,
    # this was reported as zero errors for a schema-invalid record.
    r["created"] = ["2025"]


def _language_all_scalar_anyof_branches(r):
    # Same defect as above: `language` is `anyOf[null, string, array of
    # string]`, all scalar/null, so a dict value used to vanish entirely.
    r["language"] = {"a": 1}


def _spatial_bbox_all_scalar_anyof_branches(r):
    # Same defect, one level deeper: `bbox` is `anyOf[null, string, object]`,
    # reached through `spatial`'s own `anyOf[null, array of Location]`. A
    # list value here used to vanish entirely too.
    r["spatial"][0]["bbox"] = ["x"]


def _access_rights_all_scalar_anyof_branches(r):
    # Same defect, smallest case: `accessRights` is `anyOf[null, string]`.
    r["accessRights"] = ["x"]


def _centroid_coordinates_maxitems_numeric(r):
    # Third review round, fix 1: `centroid.coordinates` is `{"type":
    # "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 2}`
    # (Location.json). A 3-element numeric array used to crash
    # `finalize_validation_messages` -- the raw message "[-77, 38, 1] is too
    # long [maxItems=2]" has no quoted run and no literal `[]`, so the regex
    # that extracts the invalid value found nothing and `.group(0)` raised
    # `AttributeError`, aborting validation mid-harvest.
    r["spatial"][0]["centroid"] = {"type": "Point", "coordinates": [-77, 38, 1]}


def _centroid_coordinates_minitems_numeric(r):
    # Third review round, fix 2: a 1-element `coordinates` array is too
    # short (`minItems: 2`). Before the fix, `found_simple_message` only
    # whitelisted `maxItems` by name for container instances, so this
    # `minItems` violation was suppressed and `assemble_validation_errors`
    # fell back to the vague "$.spatial[0].centroid, object value does not
    # match any of the acceptable formats: 'null', 'string'" branch-type
    # message instead of naming the real cause on `coordinates`.
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
