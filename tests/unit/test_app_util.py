from unittest.mock import Mock, patch

import pytest

from app.util import fetch_json_from_url


class TestFetchJsonFromUrl:
    """Tests for fetch_json_from_url function"""

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_exceeds_size_limit(self, mock_get):
        """Test that fetch_json_from_url raises ValueError when content exceeds 10MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}

        large_content = b"x" * (11 * 1024 * 1024)
        mock_response.content = large_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="JSON payload too large - must be 10MB or less."):
            fetch_json_from_url("https://example.com/large-file.json")

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_within_size_limit(self, mock_get):
        """Test that fetch_json_from_url succeeds when content is within 10MB limit"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}

        small_json = b'{"test": "data"}'
        mock_response.content = small_json
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = fetch_json_from_url("https://example.com/small-file.json")
        assert result == {"test": "data"}

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_content_length_exceeds_limit(self, mock_get):
        """Test that fetch_json_from_url raises ValueError when Content-Length header exceeds 10MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(11 * 1024 * 1024)
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="JSON payload too large - must be 10MB or less."):
            fetch_json_from_url("https://example.com/large-file.json")
