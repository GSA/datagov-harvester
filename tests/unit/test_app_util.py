from unittest.mock import Mock, patch

import pytest

from app.util import detect_catalog_warnings, fetch_json_from_url


class TestFetchJsonFromUrl:
    """Tests for fetch_json_from_url function"""

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_exceeds_size_limit(self, mock_get):
        """Test that fetch_json_from_url raises ValueError when content exceeds 10MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()

        large_content = b"x" * (11 * 1024 * 1024)
        mock_response.iter_content = Mock(return_value=[large_content])
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="JSON payload too large - must be 10MB or less."
        ):
            fetch_json_from_url("https://example.com/large-file.json")

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_within_size_limit(self, mock_get):
        """Test that fetch_json_from_url succeeds when content is within 10MB limit"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()

        small_json = b'{"test": "data"}'
        mock_response.iter_content = Mock(return_value=[small_json])
        mock_get.return_value = mock_response

        result = fetch_json_from_url("https://example.com/small-file.json")
        assert result == {"test": "data"}

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_content_length_exceeds_limit(self, mock_get):
        """Test that fetch_json_from_url raises ValueError when Content-Length
        header exceeds 10MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(11 * 1024 * 1024),
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="JSON payload too large - must be 10MB or less."
        ):
            fetch_json_from_url("https://example.com/large-file.json")

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_stops_streaming_when_limit_exceeded(self, mock_get):
        """Test that fetch_json_from_url stops downloading chunks when size
        exceeds 10MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()

        chunk_size = 1024 * 1024  # 1MB
        chunks_generated = []

        def generate_chunks():
            for i in range(15):
                chunks_generated.append(i)
                yield b"x" * chunk_size

        mock_response.iter_content = Mock(
            side_effect=lambda chunk_size: generate_chunks()
        )
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="JSON payload too large - must be 10MB or less."
        ):
            fetch_json_from_url("https://example.com/large-file.json")

        # Verify we stopped after ~10 chunks (10MB), not all 15
        assert len(chunks_generated) <= 11, (
            f"Downloaded {len(chunks_generated)} chunks, should have stopped around "
            f"10-11"
        )
        mock_response.close.assert_called_once()


class TestDetectCatalogWarnings:
    """Tests for detect_catalog_warnings (GSA/data.gov#6127).

    Schema validation only owns structure; this is the second, semantic layer
    that a real harvest also runs, surfaced here so /validate/ can show the
    same signal without a full harvest job.
    """

    def test_non_dcatus3_schema_returns_no_warnings(self):
        """Warning detection only exists for DCAT-US 3.0 today, matching the
        harvest pipeline's own `schema_type == "dcatus3.0"` gate."""
        catalog = {
            "dataset": [
                {"identifier": "a", "keyword": ["x", "x"]},
                {"identifier": "a", "keyword": ["y"]},
            ]
        }
        assert detect_catalog_warnings(catalog, "dcatus1.1: federal dataset") == []

    def test_clean_catalog_has_no_warnings(self):
        catalog = {
            "dataset": [
                {"@type": "Dataset", "identifier": "a", "keyword": ["climate"]},
                {"@type": "Dataset", "identifier": "b", "keyword": ["weather"]},
            ]
        }
        assert detect_catalog_warnings(catalog, "dcatus3.0 catalog") == []

    def test_duplicate_identifier_across_records_warns(self):
        catalog = {
            "dataset": [
                {"@type": "Dataset", "identifier": "dup-id"},
                {"@type": "Dataset", "identifier": "dup-id"},
            ]
        }
        warnings = detect_catalog_warnings(catalog, "dcatus3.0 catalog")

        assert len(warnings) == 1
        identifier, warning = warnings[0]
        assert identifier == "dup-id"
        assert warning.warning_type == "duplicate_identifier"
        assert "dup-id" in warning.message

    def test_missing_identifier_falls_back_to_dataset_position(self):
        catalog = {"dataset": [{"@type": "Dataset", "keyword": ["climate", "climate"]}]}
        warnings = detect_catalog_warnings(catalog, "dcatus3.0 catalog")

        assert len(warnings) == 1
        identifier, warning = warnings[0]
        assert identifier == 0
        assert warning.warning_type == "duplicate_keyword"

    def test_per_record_content_quality_warning_is_surfaced(self):
        """Delegates to detect_dcat_warnings; this just checks the wiring, not
        every individual rule (those are covered in test_dcat_warnings.py)."""
        catalog = {
            "dataset": [
                {
                    "@type": "Dataset",
                    "identifier": "record-1",
                    "language": ["us"],
                }
            ]
        }
        warnings = detect_catalog_warnings(catalog, "dcatus3.0 catalog")

        assert len(warnings) == 1
        identifier, warning = warnings[0]
        assert identifier == "record-1"
        assert warning.warning_type == "invalid_language"

    def test_non_dict_catalog_returns_no_warnings_without_raising(self):
        assert detect_catalog_warnings([], "dcatus3.0 catalog") == []
        assert detect_catalog_warnings("not a catalog", "dcatus3.0 catalog") == []

    def test_non_list_dataset_field_returns_no_warnings_without_raising(self):
        assert (
            detect_catalog_warnings({"dataset": "not a list"}, "dcatus3.0 catalog")
            == []
        )

    def test_non_dict_dataset_entries_are_skipped_without_raising(self):
        """A non-dict dataset entry is already a schema error from
        validate_records; warning detection should skip it, not crash, and
        should still process the valid entries around it."""
        catalog = {
            "dataset": [
                "not a dataset object",
                {"@type": "Dataset", "identifier": "ok", "language": ["us"]},
            ]
        }
        warnings = detect_catalog_warnings(catalog, "dcatus3.0 catalog")

        assert len(warnings) == 1
        identifier, warning = warnings[0]
        assert identifier == "ok"
        assert warning.warning_type == "invalid_language"

    def test_missing_dataset_field_returns_no_warnings(self):
        assert detect_catalog_warnings({}, "dcatus3.0 catalog") == []
