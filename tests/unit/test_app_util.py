import itertools
from unittest.mock import Mock, patch

import pytest
import requests

from app.util import (
    FETCH_CONNECT_TIMEOUT_SECONDS,
    FETCH_TIMEOUT_SECONDS,
    InvalidCatalogSource,
    fetch_json_from_url,
)


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

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_times_out(self, mock_get, caplog):
        """A hung/unresponsive target raises a clear, logged ValueError instead
        of hanging the worker indefinitely."""
        mock_get.side_effect = requests.exceptions.Timeout("timed out")

        with pytest.raises(ValueError, match="took longer than"):
            fetch_json_from_url("https://example.com/slow.json")

        assert any(
            "timed out" in record.message and "example.com/slow.json" in record.message
            for record in caplog.records
        )

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_passes_timeout_to_requests(self, mock_get):
        """requests.get is bounded by FETCH_TIMEOUT_SECONDS, not unbounded."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()
        mock_response.iter_content = Mock(return_value=[b'{"test": "data"}'])
        mock_get.return_value = mock_response

        fetch_json_from_url("https://example.com/small-file.json")

        # The deadline is set once and the remaining budget shrinks as time
        # passes, so these are bounded by (not exactly equal to) the constants.
        connect_timeout, read_timeout = mock_get.call_args.kwargs["timeout"]
        assert 0 < connect_timeout <= FETCH_CONNECT_TIMEOUT_SECONDS
        assert 0 < read_timeout <= FETCH_TIMEOUT_SECONDS
        assert connect_timeout < read_timeout
        assert mock_get.call_args.kwargs["allow_redirects"] is False

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_follows_redirect_to_public_url(self, mock_get):
        """A single redirect to another public URL is followed and validated."""
        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "https://example.com/final.json"}
        redirect_response.close = Mock()

        final_response = Mock()
        final_response.status_code = 200
        final_response.headers = {"Content-Type": "application/json"}
        final_response.raise_for_status = Mock()
        final_response.close = Mock()
        final_response.iter_content = Mock(return_value=[b'{"test": "data"}'])

        mock_get.side_effect = [redirect_response, final_response]

        result = fetch_json_from_url("https://example.com/redirect-me")

        assert result == {"test": "data"}
        assert mock_get.call_count == 2
        assert mock_get.call_args_list[1].args[0] == "https://example.com/final.json"

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_redirect_budget_shrinks_over_time(
        self, mock_get, monkeypatch
    ):
        """The timeout budget is shared across the whole fetch, not reset on
        every redirect hop - otherwise a chain of slow redirects could run for
        MAX_FETCH_REDIRECTS x FETCH_TIMEOUT_SECONDS in total."""

        # deadline set at t=100; hop 1's request issued at t=100 (full budget
        # left); 4s "pass" before hop 2's request is issued.
        clock = itertools.chain([100.0, 100.0, 104.0], itertools.repeat(104.0))
        monkeypatch.setattr("app.util.time.monotonic", lambda: next(clock))

        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "https://example.com/final.json"}
        redirect_response.close = Mock()

        final_response = Mock()
        final_response.status_code = 200
        final_response.headers = {"Content-Type": "application/json"}
        final_response.raise_for_status = Mock()
        final_response.close = Mock()
        final_response.iter_content = Mock(return_value=[b'{"test": "data"}'])

        mock_get.side_effect = [redirect_response, final_response]

        fetch_json_from_url("https://example.com/redirect-me")

        _, first_read = mock_get.call_args_list[0].kwargs["timeout"]
        _, second_read = mock_get.call_args_list[1].kwargs["timeout"]
        assert first_read == pytest.approx(FETCH_TIMEOUT_SECONDS)
        assert second_read == pytest.approx(FETCH_TIMEOUT_SECONDS - 4)
        assert second_read < first_read

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_redirect_chain_exceeding_budget_times_out(
        self, mock_get, monkeypatch
    ):
        """If the budget is exhausted partway through a redirect chain, the
        next hop never gets a fresh budget - it times out instead."""

        # deadline at t=100 + FETCH_TIMEOUT_SECONDS; by the second hop more
        # than the whole budget has elapsed, so there is nothing left to spend.
        exhausted = 100.0 + FETCH_TIMEOUT_SECONDS + 1
        clock = itertools.chain([100.0, 100.0, exhausted], itertools.repeat(exhausted))
        monkeypatch.setattr("app.util.time.monotonic", lambda: next(clock))

        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "https://example.com/final.json"}
        redirect_response.close = Mock()
        mock_get.return_value = redirect_response

        with pytest.raises(ValueError, match="took longer than"):
            fetch_json_from_url("https://example.com/redirect-me")

        # never attempted a second request once the shared budget was gone
        assert mock_get.call_count == 1

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_rejects_redirect_to_private_address_in_prod(
        self, mock_get, monkeypatch
    ):
        """A redirect target is re-validated like any other URL - a public URL
        that redirects to a private/internal address is refused in prod, and
        the redirect is never followed."""
        monkeypatch.setattr("app.util.IS_PROD", True)

        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "http://127.0.0.1/secret"}
        redirect_response.close = Mock()
        mock_get.return_value = redirect_response

        with pytest.raises(
            ValueError, match="Access to private/internal addresses is not allowed."
        ):
            fetch_json_from_url("https://example.com/redirect-me")

        # never followed the redirect to the private address
        mock_get.assert_called_once()

    @patch("app.util.requests.get")
    def test_fetch_json_from_url_caps_redirect_chain(self, mock_get):
        """A redirect loop/chain longer than MAX_FETCH_REDIRECTS is rejected
        rather than followed indefinitely."""
        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "https://example.com/next"}
        redirect_response.close = Mock()
        mock_get.return_value = redirect_response

        with pytest.raises(ValueError, match="Too many redirects."):
            fetch_json_from_url("https://example.com/redirect-me")

    @patch("app.util.requests.get")
    def test_rejections_raise_disclosable_exception(self, mock_get, monkeypatch):
        """Every submitter-actionable refusal raises InvalidCatalogSource, which
        is what lets both callers show the reason - the API answers 400 with
        str(e) only for this type, and falls back to a generic 500 otherwise."""
        monkeypatch.setattr("app.util.IS_PROD", True)

        not_json = Mock()
        not_json.status_code = 200
        not_json.headers = {"Content-Type": "text/html"}
        not_json.raise_for_status = Mock()
        not_json.close = Mock()

        cases = {
            "ftp://example.com/data.json": "Only HTTP/HTTPS URLs are allowed.",
            "http://127.0.0.1/data.json": "private/internal addresses",
        }
        for url, expected in cases.items():
            with pytest.raises(InvalidCatalogSource, match=expected):
                fetch_json_from_url(url)

        mock_get.return_value = not_json
        with pytest.raises(InvalidCatalogSource, match="did not return JSON"):
            fetch_json_from_url("https://example.com/page.html")

    @patch("app.util.requests.get")
    def test_unparseable_json_is_reported_by_position_only(self, mock_get):
        """A fetched document that won't parse is described by line/column, with
        neither the decoder's own text nor the document itself in the message -
        the document may be something the submitter could not otherwise read."""
        body = b'{"internal_secret": "hunter2",, "b": 2}'
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()
        mock_response.iter_content = Mock(return_value=[body])
        mock_get.return_value = mock_response

        with pytest.raises(InvalidCatalogSource) as excinfo:
            fetch_json_from_url("https://example.com/broken.json")

        message = str(excinfo.value)
        assert message == "Invalid JSON at line 1, column 31."
        assert "hunter2" not in message
        assert "internal_secret" not in message
        # the decoder's own phrasing is not reused
        assert "Expecting" not in message

    @patch("app.util.requests.get")
    def test_unexpected_transport_error_is_not_leaked(self, mock_get, caplog):
        """A connection-level failure is still reported as a refusal the
        submitter can act on, but with a fixed message - requests' own error
        text can carry internals, so it goes to the log, not the response."""
        mock_get.side_effect = requests.exceptions.SSLError(
            "certificate verify failed: /internal/path/to/ca-bundle.crt"
        )

        with pytest.raises(InvalidCatalogSource) as excinfo:
            fetch_json_from_url("https://example.com/data.json")

        assert "ca-bundle" not in str(excinfo.value)
        assert "Could not retrieve the catalog" in str(excinfo.value)
        # the detail is still recoverable by an operator
        assert any("ca-bundle" in record.message for record in caplog.records)
