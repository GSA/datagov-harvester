"""Unit tests for CatalogRecord utility functions."""

import pytest

from harvester.utils.general_utils import (
    normalize_catalog_record_identifier,
    validate_catalog_record_has_id,
)


class TestNormalizeCatalogRecordIdentifier:
    """Tests for normalize_catalog_record_identifier function."""

    def test_extracts_string_from_at_id_field(self):
        """Should extract the string from @id field."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "https://example.gov/catalog-records/record-001",
            "modified": "2024-06-15",
            "primaryTopic": "https://example.gov/datasets/climate-data",
        }
        result = normalize_catalog_record_identifier(catalog_record)
        assert result == "https://example.gov/catalog-records/record-001"

    def test_returns_none_when_at_id_missing(self):
        """Should return None when @id field is missing."""
        catalog_record = {
            "@type": "CatalogRecord",
            "modified": "2024-06-15",
            "primaryTopic": "https://example.gov/datasets/climate-data",
        }
        result = normalize_catalog_record_identifier(catalog_record)
        assert result is None

    def test_returns_none_when_at_id_is_empty_string(self):
        """Should return None when @id is an empty string."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "",
            "modified": "2024-06-15",
        }
        result = normalize_catalog_record_identifier(catalog_record)
        assert result is None

    def test_returns_none_when_at_id_is_whitespace(self):
        """Should return None when @id is only whitespace."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "   ",
            "modified": "2024-06-15",
        }
        result = normalize_catalog_record_identifier(catalog_record)
        assert result is None

    def test_returns_none_when_catalog_record_is_none(self):
        """Should return None when catalog_record is None."""
        result = normalize_catalog_record_identifier(None)
        assert result is None

    def test_returns_none_when_catalog_record_is_not_dict(self):
        """Should return None when catalog_record is not a dict."""
        result = normalize_catalog_record_identifier("not-a-dict")
        assert result is None

    def test_strips_whitespace_from_at_id(self):
        """Should strip leading/trailing whitespace from @id."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "  https://example.gov/catalog-records/record-001  ",
            "modified": "2024-06-15",
        }
        result = normalize_catalog_record_identifier(catalog_record)
        assert result == "https://example.gov/catalog-records/record-001"


class TestValidateCatalogRecordHasId:
    """Tests for validate_catalog_record_has_id function."""

    def test_returns_true_when_at_id_present_and_valid(self):
        """Should return True when @id is present and valid."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "https://example.gov/catalog-records/record-001",
            "modified": "2024-06-15",
        }
        result = validate_catalog_record_has_id(catalog_record)
        assert result is True

    def test_returns_false_when_at_id_missing(self):
        """Should return False when @id field is missing."""
        catalog_record = {
            "@type": "CatalogRecord",
            "modified": "2024-06-15",
        }
        result = validate_catalog_record_has_id(catalog_record)
        assert result is False

    def test_returns_false_when_at_id_is_empty_string(self):
        """Should return False when @id is an empty string."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "",
        }
        result = validate_catalog_record_has_id(catalog_record)
        assert result is False

    def test_returns_false_when_at_id_is_whitespace(self):
        """Should return False when @id is only whitespace."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "   ",
        }
        result = validate_catalog_record_has_id(catalog_record)
        assert result is False

    def test_returns_false_when_catalog_record_is_none(self):
        """Should return False when catalog_record is None."""
        result = validate_catalog_record_has_id(None)
        assert result is False

    def test_returns_false_when_catalog_record_is_not_dict(self):
        """Should return False when catalog_record is not a dict."""
        result = validate_catalog_record_has_id("not-a-dict")
        assert result is False

    def test_returns_true_with_whitespace_at_id(self):
        """Should return True even with whitespace around valid @id."""
        catalog_record = {
            "@type": "CatalogRecord",
            "@id": "  https://example.gov/catalog-records/record-001  ",
        }
        result = validate_catalog_record_has_id(catalog_record)
        assert result is True
