"""Unit tests for CatalogRecord extraction from DCAT-US 3.0 catalogs."""

import json
from pathlib import Path

import pytest

from harvester.utils.general_utils import (
    normalize_catalog_record_identifier,
    validate_catalog_record_has_id,
)


class TestCatalogFixture:
    """Tests for the DCAT-US 3.0 catalog fixture."""

    @pytest.fixture
    def sample_catalog_with_records(self):
        """Load sample DCAT-US 3.0 catalog with records array."""
        fixture_path = (
            Path(__file__).parents[1] / "fixtures" / "dcatus3_catalog_with_records.json"
        )
        with open(fixture_path) as f:
            return json.load(f)

    def test_catalog_has_record_array(self, sample_catalog_with_records):
        """Verify test fixture has record array."""
        assert "record" in sample_catalog_with_records
        assert isinstance(sample_catalog_with_records["record"], list)
        assert len(sample_catalog_with_records["record"]) == 2

    def test_catalog_has_dataset_array(self, sample_catalog_with_records):
        """Verify test fixture has dataset array."""
        assert "dataset" in sample_catalog_with_records
        assert isinstance(sample_catalog_with_records["dataset"], list)
        assert len(sample_catalog_with_records["dataset"]) == 1

    def test_catalog_records_have_at_id(self, sample_catalog_with_records):
        """Verify CatalogRecords in fixture have @id fields."""
        for record in sample_catalog_with_records["record"]:
            assert "@id" in record
            assert isinstance(record["@id"], str)
            assert record["@id"].startswith("https://")

    def test_catalog_records_have_required_fields(self, sample_catalog_with_records):
        """Verify CatalogRecords have required fields."""
        for record in sample_catalog_with_records["record"]:
            assert "modified" in record
            assert "primaryTopic" in record
            assert "@id" in record

    def test_catalog_records_can_be_normalized(self, sample_catalog_with_records):
        """Verify CatalogRecords can be processed by utility functions."""
        for record in sample_catalog_with_records["record"]:
            identifier = normalize_catalog_record_identifier(record)
            assert identifier is not None
            assert identifier == record["@id"]

            has_id = validate_catalog_record_has_id(record)
            assert has_id is True
