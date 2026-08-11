"""Unit tests for record type constants."""

from shared.constants import RECORD_TYPE_VALUES


def test_record_type_values_defined():
    """Test that RECORD_TYPE_VALUES is defined with expected metadata types."""
    assert RECORD_TYPE_VALUES is not None
    assert isinstance(RECORD_TYPE_VALUES, list)


def test_record_type_values_contains_dataset():
    """Test that dataset is in the record type values for backward compatibility."""
    assert "dataset" in RECORD_TYPE_VALUES


def test_record_type_values_contains_catalog_record():
    """Test that catalog_record is in the record type values."""
    assert "catalog_record" in RECORD_TYPE_VALUES


def test_record_type_values_contains_all_dcat3_types():
    """Test that all DCAT-US 3.0 metadata types are present."""
    expected_types = [
        "dataset",
        "catalog_record",
        "data_service",
        "dataset_series",
        "catalog",
    ]
    for record_type in expected_types:
        assert record_type in RECORD_TYPE_VALUES


def test_record_type_values_count():
    """Test that we have exactly 5 record types."""
    assert len(RECORD_TYPE_VALUES) == 5
