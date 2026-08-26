"""Unit tests for DCAT-US 3 warning detection (GSA/data.gov#6127).

One test class per rule, each with a passing case and a warning case. Tests are
isolated and DB-free, mirroring tests/unit/test_dcatus3_validator.py.

Assertions check the stable `warning_type` slug plus a message substring rather
than the full templated string, so message wording can change without churning
every test.
"""

from harvester.utils.dcat_warnings import detect_dcat_warnings
from harvester.utils.general_utils import open_json
from harvester.utils.schema_paths import DCATUS3_COMPLETE_EXAMPLE


def types(warnings):
    return [w.warning_type for w in warnings]


class TestDuplicateKeywords:
    def test_unique_keywords_pass(self):
        data = {"@type": "Dataset", "keyword": ["climate", "weather"]}
        assert detect_dcat_warnings(data) == []

    def test_duplicate_keyword_warns_once(self):
        data = {"@type": "Dataset", "keyword": ["climate", "climate", "weather"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["duplicate_keyword"]
        assert "climate" in warnings[0].message

    def test_duplicate_blank_keywords_produce_no_warning(self):
        # Empty strings are schema `minLength: 1`; must not also warn.
        data = {"@type": "Dataset", "keyword": ["", ""]}
        assert detect_dcat_warnings(data) == []


class TestSpatialResolutionInMeters:
    def test_valid_number_passes(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "1000"}
        assert detect_dcat_warnings(data) == []

    def test_decimal_value_passes(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "0.5"}
        assert detect_dcat_warnings(data) == []

    def test_non_numeric_warns(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "about 1km"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_spatial_resolution"]
        assert "does not appear to be a valid number" in warnings[0].message

    def test_scientific_notation_is_rejected_as_non_numeric(self):
        # is_number would accept "1e3"; the spec wants digits + optional decimal.
        data = {"@type": "Dataset", "spatialResolutionInMeters": "1e3"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_spatial_resolution"]
        assert "does not appear to be a valid number" in warnings[0].message

    def test_zero_or_negative_warns(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "0"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_spatial_resolution"]
        assert "must be greater than zero" in warnings[0].message

    def test_negative_value_reaches_greater_than_zero_check(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "-5"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_spatial_resolution"]
        assert "must be greater than zero" in warnings[0].message

    def test_applies_to_distribution_and_dataservice(self):
        for dcat_type in ("Distribution", "DataService"):
            data = {"@type": dcat_type, "spatialResolutionInMeters": "-5"}
            warnings = detect_dcat_warnings(data)
            assert types(warnings) == ["invalid_spatial_resolution"]

    def test_non_string_value_produces_no_warning(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": 5}
        assert detect_dcat_warnings(data) == []


class TestTemporalResolution:
    def test_valid_iso8601_duration_passes(self):
        data = {"@type": "Dataset", "temporalResolution": "P1D"}
        assert detect_dcat_warnings(data) == []

    def test_dataset_invalid_duration_warns_iso8601_message(self):
        data = {"@type": "Dataset", "temporalResolution": "daily"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_temporal_resolution"]
        assert "ISO 8601 duration" in warnings[0].message

    def test_distribution_invalid_duration_warns_xsd_message(self):
        data = {"@type": "Distribution", "temporalResolution": "daily"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_temporal_resolution"]
        assert "xsd:duration" in warnings[0].message

    def test_bare_p_is_not_a_valid_duration(self):
        data = {"@type": "Dataset", "temporalResolution": "P"}
        assert types(detect_dcat_warnings(data)) == ["invalid_temporal_resolution"]

    def test_non_string_value_produces_no_warning(self):
        data = {"@type": "Dataset", "temporalResolution": 5}
        assert detect_dcat_warnings(data) == []


class TestByteSize:
    def test_numeric_byte_size_passes(self):
        data = {"@type": "Distribution", "byteSize": "524288000"}
        assert detect_dcat_warnings(data) == []

    def test_non_numeric_byte_size_warns(self):
        data = {"@type": "Distribution", "byteSize": "big"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_byte_size"]
        assert "does not appear to be a valid number" in warnings[0].message

    def test_non_string_value_produces_no_warning(self):
        data = {"@type": "Distribution", "byteSize": 524288000}
        assert detect_dcat_warnings(data) == []

    def test_list_value_produces_no_warning_and_does_not_crash(self):
        # is_number(["big"]) used to raise TypeError out of detect_dcat_warnings.
        data = {"@type": "Distribution", "byteSize": []}
        assert detect_dcat_warnings(data) == []


class TestLegacyAccessRights:
    def test_v3_access_rights_passes(self):
        data = {"@type": "Dataset", "accessRights": "public"}
        assert detect_dcat_warnings(data) == []

    def test_legacy_term_warns(self):
        data = {"@type": "Dataset", "accessRights": "non-public"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["legacy_access_rights"]
        assert "legacy DCAT-US 1.1" in warnings[0].message


class TestDateOrdering:
    def test_created_before_modified_passes(self):
        data = {
            "@type": "Dataset",
            "created": "2024-01-01",
            "modified": "2024-06-01",
            "issued": "2024-01-15",
        }
        assert detect_dcat_warnings(data) == []

    def test_created_after_modified_and_issued_warns_for_both(self):
        data = {
            "@type": "Dataset",
            "created": "2025-01-01",
            "modified": "2024-06-01",
            "issued": "2024-01-15",
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["date_out_of_order", "date_out_of_order"]
        assert any("`modified` value" in w.message for w in warnings)
        assert any("`issued` value" in w.message for w in warnings)

    def test_issued_after_modified_warns(self):
        data = {
            "@type": "Dataset",
            "issued": "2024-07-01",
            "modified": "2024-06-01",
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["date_out_of_order"]
        assert "`issued` value" in warnings[0].message

    def test_partial_dates_compare(self):
        data = {"@type": "Dataset", "created": "2025", "issued": "2024"}
        assert types(detect_dcat_warnings(data)) == ["date_out_of_order"]

    def test_yyyy_mm_dd_dates_compare(self):
        data = {"@type": "Dataset", "created": "2025-06-01", "issued": "2024-01-01"}
        assert types(detect_dcat_warnings(data)) == ["date_out_of_order"]

    def test_date_time_with_offset_compares(self):
        # `format: date-time` requires a UTC offset; "Z" is valid RFC 3339.
        data = {
            "@type": "Dataset",
            "created": "2025-01-01T00:00:00Z",
            "issued": "2024-01-01T00:00:00Z",
        }
        assert types(detect_dcat_warnings(data)) == ["date_out_of_order"]

    def test_lowercase_rfc3339_date_time_warns_and_does_not_crash(self):
        # Lowercase "t"/"z" is still RFC 3339; used to raise inside the parser
        # and silently produce no warning.
        data = {
            "@type": "Dataset",
            "created": "2025-01-01t00:00:00z",
            "issued": "2024-01-01t00:00:00z",
        }
        assert types(detect_dcat_warnings(data)) == ["date_out_of_order"]

    def test_date_time_without_offset_produces_no_warning(self):
        # fromisoformat accepts a date-time with no offset; the schema does not.
        data = {
            "@type": "Dataset",
            "created": "2025-01-01T00:00:00",
            "issued": "2024-01-01T00:00:00",
        }
        assert detect_dcat_warnings(data) == []

    def test_whitespace_padded_date_produces_no_warning(self):
        # Padded strings are schema-invalid; must not be stripped and warned on.
        data = {
            "@type": "Dataset",
            "created": " 2025-06-01 ",
            "issued": "2024-01-01",
        }
        assert detect_dcat_warnings(data) == []

    def test_unpadded_equivalent_of_padded_date_still_warns(self):
        # Same date without padding is schema-valid and must still warn.
        data = {
            "@type": "Dataset",
            "created": "2025-06-01",
            "issued": "2024-01-01",
        }
        assert types(detect_dcat_warnings(data)) == ["date_out_of_order"]


class TestPeriodOfTime:
    def test_start_before_end_passes(self):
        data = {
            "@type": "PeriodOfTime",
            "startDate": "2024-01-01",
            "endDate": "2024-12-31",
        }
        assert detect_dcat_warnings(data) == []

    def test_start_after_end_warns(self):
        data = {
            "@type": "PeriodOfTime",
            "startDate": "2024-12-31",
            "endDate": "2024-01-01",
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["date_out_of_order"]
        assert "cannot be after its end" in warnings[0].message


class TestTel:
    def test_numeric_tel_passes(self):
        data = {"@type": "Kind", "tel": "+1-555-123-4567"}
        assert detect_dcat_warnings(data) == []

    def test_tel_with_letters_warns(self):
        data = {"@type": "Kind", "tel": "+1-555-CLIMATE"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_tel"]
        assert "contains letters" in warnings[0].message


class TestExpectedDataType:
    def test_xsd_prefixed_type_passes(self):
        data = {"@type": "Metric", "expectedDataType": "xsd:decimal"}
        assert detect_dcat_warnings(data) == []

    def test_missing_xsd_prefix_warns(self):
        data = {"@type": "Metric", "expectedDataType": "decimal"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_expected_data_type"]
        assert "xsd:" in warnings[0].message

    def test_non_string_value_produces_no_warning(self):
        data = {"@type": "Metric", "expectedDataType": ["xsd:decimal"]}
        assert detect_dcat_warnings(data) == []


class TestAddress:
    def test_populated_us_address_passes(self):
        data = {
            "@type": "Address",
            "postal-code": "28801",
            "country-name": "United States",
        }
        assert detect_dcat_warnings(data) == []

    def test_us_postal_code_with_letters_warns(self):
        data = {
            "@type": "Address",
            "postal-code": "K1A0B1",
            "country-name": "US",
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_postal_code"]
        assert "not valid for a US address" in warnings[0].message

    def test_non_us_postal_code_with_letters_passes(self):
        data = {
            "@type": "Address",
            "postal-code": "K1A0B1",
            "country-name": "Canada",
        }
        assert detect_dcat_warnings(data) == []

    def test_empty_address_warns(self):
        data = {"@type": "Address", "postal-code": "", "country-name": None}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["empty_address"]

    def test_wrong_typed_postal_code_zero_produces_no_empty_address_warning(self):
        # `0` is a type error, not an unpopulated field, so this is not empty.
        data = {"@type": "Address", "postal-code": 0}
        assert detect_dcat_warnings(data) == []

    def test_wrong_typed_postal_code_empty_list_produces_no_empty_address_warning(self):
        data = {"@type": "Address", "postal-code": []}
        assert detect_dcat_warnings(data) == []

    def test_all_fields_missing_still_warns(self):
        # Missing fields (as opposed to explicit None/"") is still unpopulated.
        data = {"@type": "Address"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["empty_address"]


class TestCuiBannerMarking:
    def test_valid_marking_passes(self):
        data = {"@type": "CUIRestriction", "cuiBannerMarking": "CUI//SP-CTI"}
        assert detect_dcat_warnings(data) == []

    def test_missing_prefix_warns(self):
        data = {"@type": "CUIRestriction", "cuiBannerMarking": "SP-CTI"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_cui_banner_marking"]
        assert "CUI//" in warnings[0].message


class TestLocationSpatial:
    def test_resolvable_point_passes(self):
        # A simple "x, y" pair resolves via munge_spatial without any DB lookup.
        data = {"@type": "Location", "geometry": "-92.1, 15.1"}
        assert detect_dcat_warnings(data) == []

    def test_unresolvable_geometry_warns(self):
        data = {"@type": "Location", "geometry": "somewhere over there"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["unresolvable_spatial_value"]
        assert "could not be resolved" in warnings[0].message

    def test_resolvable_polygon_object_passes(self):
        data = {
            "@type": "Location",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-77.04, 38.79], [-76.9, 38.89], [-76.91, 38.93], [-77.04, 38.79]]
                ],
            },
        }
        assert detect_dcat_warnings(data) == []

    def test_non_string_non_object_geometry_produces_no_warning(self):
        data = {"@type": "Location", "geometry": 5}
        assert detect_dcat_warnings(data) == []

    def test_geometry_dict_missing_required_keys_produces_no_warning(self):
        data = {"@type": "Location", "geometry": {"foo": 1}}
        assert detect_dcat_warnings(data) == []

    def test_non_string_non_object_bbox_produces_no_warning(self):
        data = {"@type": "Location", "bbox": 5}
        assert detect_dcat_warnings(data) == []

    def test_bbox_dict_missing_required_keys_produces_no_warning(self):
        data = {"@type": "Location", "bbox": {"foo": 1}}
        assert detect_dcat_warnings(data) == []

    def test_bbox_dict_wrong_type_produces_no_warning(self):
        # bbox objects must be type "Polygon"; anything else is a schema const error.
        data = {
            "@type": "Location",
            "bbox": {
                "type": "NotAPolygon",
                "coordinates": [[[-77.1, 38.7], [-76.9, 38.7], [-77.1, 38.7]]],
            },
        }
        assert detect_dcat_warnings(data) == []

    def test_geometry_dict_arbitrary_type_still_warns_when_unresolvable(self):
        # geometry has no type const, so an unresolvable dict is still a warning.
        data = {
            "@type": "Location",
            "geometry": {"type": "NotAPolygon", "coordinates": [1, 2]},
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["unresolvable_spatial_value"]


class TestLanguage:
    def test_recognized_code_passes(self):
        data = {"@type": "Dataset", "language": "en"}
        assert detect_dcat_warnings(data) == []

    def test_array_of_codes_passes(self):
        data = {"@type": "Dataset", "language": ["en", "es"]}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_code_warns(self):
        data = {"@type": "Dataset", "language": "zz"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_language"]
        assert "not a recognized ISO 639-1" in warnings[0].message

    def test_too_short_code_warns(self):
        data = {"@type": "Dataset", "language": "e"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_language"]
        assert "too short" in warnings[0].message

    def test_applies_per_entry_in_array(self):
        data = {"@type": "Distribution", "language": ["en", "zz"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_language"]
        assert '"zz"' in warnings[0].message

    def test_entry_longer_than_two_chars_produces_no_warning(self):
        # Entries longer than 2 characters are schema `maxLength` errors.
        assert detect_dcat_warnings({"@type": "Dataset", "language": "eng"}) == []
        assert detect_dcat_warnings({"@type": "Dataset", "language": ["eng"]}) == []


class TestCharacterEncoding:
    def test_recognized_charset_passes(self):
        data = {"@type": "Distribution", "characterEncoding": ["UTF-8"]}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_charset_warns(self):
        data = {"@type": "Distribution", "characterEncoding": ["bogus"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_character_encoding"]
        assert "IANA character set" in warnings[0].message

    def test_bare_string_value_produces_no_warning(self):
        # characterEncoding is array-of-string only; a bare string is a type error.
        data = {"@type": "Distribution", "characterEncoding": "bogus"}
        assert detect_dcat_warnings(data) == []


class TestMediaType:
    def test_recognized_media_type_passes(self):
        data = {"@type": "Distribution", "mediaType": "text/csv"}
        assert detect_dcat_warnings(data) == []

    def test_document_recognized_media_type_passes(self):
        data = {"@type": "Document", "mediaType": "application/pdf"}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_media_type_warns(self):
        data = {"@type": "Distribution", "mediaType": "application/notreal"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_media_type"]
        assert "IANA media type" in warnings[0].message

    def test_media_type_with_parameter_passes(self):
        data = {"@type": "Distribution", "mediaType": "text/csv; charset=UTF-8"}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_media_type_with_parameter_still_warns(self):
        data = {"@type": "Distribution", "mediaType": "application/notreal; v=1"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_media_type"]
        # The message echoes the full original value, parameter included.
        assert "application/notreal; v=1" in warnings[0].message


class TestRestrictions:
    def test_recognized_access_restriction_status_passes(self):
        data = {"@type": "AccessRestriction", "restrictionStatus": "Unrestricted"}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_access_restriction_status_warns(self):
        data = {"@type": "AccessRestriction", "restrictionStatus": "Bogus"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_restriction_status"]
        assert "NARA access restriction status" in warnings[0].message

    def test_concept_object_preflabel_is_checked(self):
        data = {
            "@type": "UseRestriction",
            "restrictionStatus": {"@type": "Concept", "prefLabel": "Unrestricted"},
        }
        assert detect_dcat_warnings(data) == []

    def test_concept_object_unrecognized_preflabel_warns(self):
        data = {
            "@type": "UseRestriction",
            "restrictionStatus": {"@type": "Concept", "prefLabel": "Bogus"},
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_restriction_status"]
        assert "NARA use restriction status" in warnings[0].message

    def test_specific_restriction_uses_its_own_authority_list(self):
        # "Copyright" is valid for specific *use* restriction but is not an
        # access-restriction term.
        ok = {"@type": "UseRestriction", "specificRestriction": "Copyright"}
        assert detect_dcat_warnings(ok) == []

        bad = {"@type": "AccessRestriction", "specificRestriction": "Copyright"}
        warnings = detect_dcat_warnings(bad)
        assert types(warnings) == ["invalid_specific_restriction"]
        assert "NARA specific access restriction" in warnings[0].message


class TestTraversalAndCleanRecord:
    def test_clean_minimal_record_has_no_warnings(self):
        data = {
            "@id": "https://example.gov/datasets/one",
            "@type": "Dataset",
            "keyword": ["climate", "weather"],
            "spatialResolutionInMeters": "1000",
            "temporalResolution": "P1D",
            "created": "2024-01-01",
            "issued": "2024-01-15",
            "modified": "2024-06-01",
        }
        assert detect_dcat_warnings(data) == []

    def test_complete_example_reference_data_fields_do_not_warn(self):
        # The upstream complete example uses valid language ("en"), media types
        # (text/csv, application/json, application/pdf), etc. Guard against the
        # reference-data rules producing false positives on it. The vanity tel
        # and WKT geometry warnings are expected and unrelated.
        dataset = open_json(DCATUS3_COMPLETE_EXAMPLE)
        reference_data_types = {
            "invalid_language",
            "invalid_character_encoding",
            "invalid_media_type",
            "invalid_restriction_status",
            "invalid_specific_restriction",
        }
        produced = set(types(detect_dcat_warnings(dataset)))
        assert produced.isdisjoint(reference_data_types)

    def test_warnings_collected_from_multiple_nested_objects(self):
        data = {
            "@type": "Dataset",
            "@id": "https://example.gov/one",
            "keyword": ["a", "a"],
            "distribution": [{"@type": "Distribution", "byteSize": "big"}],
            "contactPoint": [{"@type": "Kind", "tel": "CALL-ME"}],
        }
        warnings = detect_dcat_warnings(data)
        assert set(types(warnings)) == {
            "duplicate_keyword",
            "invalid_byte_size",
            "invalid_tel",
        }


class TestNonStringTypeDispatch:
    # Non-string @type is unhashable; dispatching via `_TYPE_RULES.get` used
    # to raise TypeError and abort warning detection for the whole record.

    def test_list_type_is_skipped_without_raising(self):
        data = {
            "@type": "Dataset",
            "distribution": [{"@type": [], "byteSize": "big"}],
        }
        assert detect_dcat_warnings(data) == []

    def test_dict_type_is_skipped_without_raising(self):
        data = {
            "@type": "Dataset",
            "distribution": [{"@type": {}, "byteSize": "big"}],
        }
        assert detect_dcat_warnings(data) == []

    def test_other_objects_still_processed_when_one_has_a_bad_type(self):
        # A malformed @type on one object must not swallow sibling warnings.
        data = {
            "@type": "Dataset",
            "keyword": ["a", "a"],
            "distribution": [{"@type": [], "byteSize": "big"}],
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["duplicate_keyword"]
