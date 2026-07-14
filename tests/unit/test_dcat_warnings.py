"""Unit tests for DCAT-US 3 warning detection (GSA/data.gov#6127).

One test class per rule, each with a passing case and a warning case. Tests are
isolated and DB-free, mirroring tests/unit/test_dcatus3_validator.py.
"""

from harvester.utils.dcat_warnings import detect_dcat_warnings


class TestIdIri:
    def test_valid_iri_produces_no_warning(self):
        data = {"@id": "https://example.gov/datasets/one", "@type": "Dataset"}
        assert detect_dcat_warnings(data) == []

    def test_invalid_iri_warns(self):
        data = {"@id": "not a valid iri", "@type": "Dataset"}
        warnings = detect_dcat_warnings(data)
        assert warnings == ['`@id` value "not a valid iri" is not a valid IRI.']

    def test_missing_id_is_ignored(self):
        assert detect_dcat_warnings({"@type": "Dataset"}) == []

    def test_nested_object_id_is_checked(self):
        data = {
            "@type": "Dataset",
            "distribution": [{"@type": "Distribution", "@id": "bad iri"}],
        }
        assert '`@id` value "bad iri" is not a valid IRI.' in detect_dcat_warnings(data)


class TestDuplicateKeywords:
    def test_unique_keywords_pass(self):
        data = {"@type": "Dataset", "keyword": ["climate", "weather"]}
        assert detect_dcat_warnings(data) == []

    def test_duplicate_keyword_warns_once(self):
        data = {"@type": "Dataset", "keyword": ["climate", "climate", "weather"]}
        warnings = detect_dcat_warnings(data)
        assert warnings == [
            'Duplicate keyword detected: "climate". Keywords should be unique.'
        ]


class TestSpatialResolutionInMeters:
    def test_valid_number_passes(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "1000"}
        assert detect_dcat_warnings(data) == []

    def test_non_numeric_warns(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "about 1km"}
        assert detect_dcat_warnings(data) == [
            '`spatialResolutionInMeters` value "about 1km" '
            "does not appear to be a valid number."
        ]

    def test_zero_or_negative_warns(self):
        data = {"@type": "Dataset", "spatialResolutionInMeters": "0"}
        assert detect_dcat_warnings(data) == [
            '`spatialResolutionInMeters` value "0" must be greater than zero.'
        ]

    def test_applies_to_distribution_and_dataservice(self):
        for dcat_type in ("Distribution", "DataService"):
            data = {"@type": dcat_type, "spatialResolutionInMeters": "-5"}
            assert detect_dcat_warnings(data) == [
                '`spatialResolutionInMeters` value "-5" must be greater than zero.'
            ]


class TestTemporalResolution:
    def test_valid_iso8601_duration_passes(self):
        data = {"@type": "Dataset", "temporalResolution": "P1D"}
        assert detect_dcat_warnings(data) == []

    def test_dataset_invalid_duration_warns_iso8601_message(self):
        data = {"@type": "Dataset", "temporalResolution": "daily"}
        assert detect_dcat_warnings(data) == [
            '`temporalResolution` value "daily" does not appear to be a valid '
            'ISO 8601 duration (e.g., "P1Y", "P6M", "P1D").'
        ]

    def test_distribution_invalid_duration_warns_xsd_message(self):
        data = {"@type": "Distribution", "temporalResolution": "daily"}
        assert detect_dcat_warnings(data) == [
            '`temporalResolution` value "daily" '
            "does not appear to be a valid xsd:duration."
        ]

    def test_bare_p_is_not_a_valid_duration(self):
        data = {"@type": "Dataset", "temporalResolution": "P"}
        assert len(detect_dcat_warnings(data)) == 1


class TestByteSize:
    def test_numeric_byte_size_passes(self):
        data = {"@type": "Distribution", "byteSize": "524288000"}
        assert detect_dcat_warnings(data) == []

    def test_non_numeric_byte_size_warns(self):
        data = {"@type": "Distribution", "byteSize": "big"}
        assert detect_dcat_warnings(data) == [
            '`byteSize` value "big" does not appear to be a valid number.'
        ]


class TestLegacyAccessRights:
    def test_v3_access_rights_passes(self):
        data = {"@type": "Dataset", "accessRights": "public"}
        assert detect_dcat_warnings(data) == []

    def test_legacy_term_warns(self):
        data = {"@type": "Dataset", "accessRights": "non-public"}
        assert detect_dcat_warnings(data) == [
            '`accessRights` value "non-public" appears to be a legacy DCAT-US 1.1 '
            "`accessLevel` term. Migrate to an appropriate DCAT-US 3 access "
            "rights value."
        ]


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
        assert len(warnings) == 2
        assert any("`modified` value" in w for w in warnings)
        assert any("`issued` value" in w for w in warnings)

    def test_issued_after_modified_warns(self):
        data = {
            "@type": "Dataset",
            "issued": "2024-07-01",
            "modified": "2024-06-01",
        }
        assert detect_dcat_warnings(data) == [
            '`issued` value "2024-07-01" is later than `modified` value '
            '"2024-06-01". A dataset cannot be issued after it was last modified.'
        ]

    def test_partial_dates_compare(self):
        data = {"@type": "Dataset", "created": "2025", "issued": "2024"}
        assert len(detect_dcat_warnings(data)) == 1


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
        assert detect_dcat_warnings(data) == [
            '`startDate` value "2024-12-31" is later than `endDate` value '
            '"2024-01-01". The start of a time period cannot be after its end.'
        ]


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
        assert detect_dcat_warnings(data) == [
            '`relation` value "not an iri" is not a valid IRI.'
        ]

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
        assert detect_dcat_warnings(data) == [
            '`image` value "not an iri" is not a valid IRI.'
        ]


class TestTel:
    def test_numeric_tel_passes(self):
        data = {"@type": "Kind", "tel": "+1-555-123-4567"}
        assert detect_dcat_warnings(data) == []

    def test_tel_with_letters_warns(self):
        data = {"@type": "Kind", "tel": "+1-555-CLIMATE"}
        assert detect_dcat_warnings(data) == [
            '`tel` value "+1-555-CLIMATE" contains letters and may not be a valid '
            "telephone number."
        ]


class TestExpectedDataType:
    def test_xsd_prefixed_type_passes(self):
        data = {"@type": "Metric", "expectedDataType": "xsd:decimal"}
        assert detect_dcat_warnings(data) == []

    def test_missing_xsd_prefix_warns(self):
        data = {"@type": "Metric", "expectedDataType": "decimal"}
        assert detect_dcat_warnings(data) == [
            '`expectedDataType` value "decimal" does not appear to be a valid '
            'XSD type. Expected format begins with "xsd:" (e.g., "xsd:string", '
            '"xsd:decimal").'
        ]


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
        assert detect_dcat_warnings(data) == [
            'Postal code "K1A0B1" contains letters, which is not valid '
            "for a US address."
        ]

    def test_non_us_postal_code_with_letters_passes(self):
        data = {
            "@type": "Address",
            "postal-code": "K1A0B1",
            "country-name": "Canada",
        }
        assert detect_dcat_warnings(data) == []

    def test_empty_address_warns(self):
        data = {"@type": "Address", "postal-code": "", "country-name": None}
        assert detect_dcat_warnings(data) == ["Address object has no populated fields."]


class TestCuiBannerMarking:
    def test_valid_marking_passes(self):
        data = {"@type": "CUIRestriction", "cuiBannerMarking": "CUI//SP-CTI"}
        assert detect_dcat_warnings(data) == []

    def test_missing_prefix_warns(self):
        data = {"@type": "CUIRestriction", "cuiBannerMarking": "SP-CTI"}
        assert detect_dcat_warnings(data) == [
            '`cuiBannerMarking` value "SP-CTI" does not appear to follow the CUI '
            'banner marking format. Expected format begins with "CUI//".'
        ]


class TestLocationSpatial:
    def test_resolvable_point_passes(self):
        # A simple "x, y" pair resolves via munge_spatial without any DB lookup.
        data = {"@type": "Location", "geometry": "-92.1, 15.1"}
        assert detect_dcat_warnings(data) == []

    def test_unresolvable_geometry_warns(self):
        data = {"@type": "Location", "geometry": "somewhere over there"}
        assert detect_dcat_warnings(data) == [
            'spatial value "somewhere over there" could not be resolved to a '
            "recognized geographic area."
        ]


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

    def test_warnings_collected_from_multiple_nested_objects(self):
        data = {
            "@type": "Dataset",
            "@id": "https://example.gov/one",
            "keyword": ["a", "a"],
            "distribution": [{"@type": "Distribution", "byteSize": "big"}],
            "contactPoint": [{"@type": "Kind", "tel": "CALL-ME"}],
        }
        warnings = detect_dcat_warnings(data)
        assert len(warnings) == 3
        assert any("Duplicate keyword" in w for w in warnings)
        assert any("byteSize" in w for w in warnings)
        assert any("tel" in w for w in warnings)
