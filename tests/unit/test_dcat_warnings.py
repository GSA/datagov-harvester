"""Unit tests for DCAT-US 3 warning detection (GSA/data.gov#6127).

One test class per rule, each with a passing case and a warning case. Tests are
isolated and DB-free, mirroring tests/unit/test_dcatus3_validator.py.

Assertions check the stable `warning_type` slug plus a message substring rather
than the full templated string, so message wording can change without churning
every test.
"""

from pathlib import Path

from harvester.utils.dcat_warnings import DcatWarning, detect_dcat_warnings
from harvester.utils.general_utils import open_json

COMPLETE_EXAMPLE = (
    Path(__file__).parents[2]
    / "schemas"
    / "dcatus3.0"
    / "examples"
    / "Dataset"
    / "good"
    / "complete_example.json"
)


def types(warnings):
    return [w.warning_type for w in warnings]


class TestIdIri:
    def test_valid_iri_produces_no_warning(self):
        data = {"@id": "https://example.gov/datasets/one", "@type": "Dataset"}
        assert detect_dcat_warnings(data) == []

    def test_invalid_iri_warns(self):
        data = {"@id": "not a valid iri", "@type": "Dataset"}
        warnings = detect_dcat_warnings(data)
        assert warnings == [
            DcatWarning(
                "invalid_iri", '`@id` value "not a valid iri" is not a valid IRI.'
            )
        ]

    def test_missing_id_is_ignored(self):
        assert detect_dcat_warnings({"@type": "Dataset"}) == []

    def test_nested_object_id_is_checked(self):
        data = {
            "@type": "Dataset",
            "distribution": [{"@type": "Distribution", "@id": "bad iri"}],
        }
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_iri"]
        assert '`@id` value "bad iri"' in warnings[0].message


class TestDuplicateKeywords:
    def test_unique_keywords_pass(self):
        data = {"@type": "Dataset", "keyword": ["climate", "weather"]}
        assert detect_dcat_warnings(data) == []

    def test_duplicate_keyword_warns_once(self):
        data = {"@type": "Dataset", "keyword": ["climate", "climate", "weather"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["duplicate_keyword"]
        assert "climate" in warnings[0].message


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


class TestByteSize:
    def test_numeric_byte_size_passes(self):
        data = {"@type": "Distribution", "byteSize": "524288000"}
        assert detect_dcat_warnings(data) == []

    def test_non_numeric_byte_size_warns(self):
        data = {"@type": "Distribution", "byteSize": "big"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_byte_size"]
        assert "does not appear to be a valid number" in warnings[0].message


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


class TestIriArrays:
    def test_valid_iri_arrays_pass(self):
        data = {
            "@type": "Dataset",
            "relation": ["https://example.gov/a"],
            "isReferencedBy": ["https://example.gov/b"],
        }
        assert detect_dcat_warnings(data) == []

    def test_invalid_relation_iri_warns(self):
        data = {"@type": "Dataset", "relation": ["not an iri"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_iri"]
        assert "`relation` value" in warnings[0].message

    def test_conformsTo_object_entries_are_skipped(self):
        # Standard objects on conformsTo are covered by the universal @id walk,
        # not by the string-IRI check.
        data = {
            "@type": "Dataset",
            "conformsTo": [
                {"@type": "Standard", "@id": "https://example.gov/std", "title": "x"}
            ],
        }
        assert detect_dcat_warnings(data) == []

    def test_image_invalid_iri_warns(self):
        data = {"@type": "Dataset", "image": "not an iri"}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_iri"]
        assert "`image` value" in warnings[0].message


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


class TestCharacterEncoding:
    def test_recognized_charset_passes(self):
        data = {"@type": "Distribution", "characterEncoding": ["UTF-8"]}
        assert detect_dcat_warnings(data) == []

    def test_unrecognized_charset_warns(self):
        data = {"@type": "Distribution", "characterEncoding": ["bogus"]}
        warnings = detect_dcat_warnings(data)
        assert types(warnings) == ["invalid_character_encoding"]
        assert "IANA character set" in warnings[0].message


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
        dataset = open_json(COMPLETE_EXAMPLE)
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
