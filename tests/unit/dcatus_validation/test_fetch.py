import itertools
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

import pytest
import requests

import dcatus_validation.fetch as fetch
from dcatus_validation.errors import CatalogTooDeeplyNested
from dcatus_validation.fetch import (
    FETCH_CONNECT_ATTEMPTS,
    FETCH_CONNECT_TIMEOUT_SECONDS,
    FETCH_TIMEOUT_SECONDS,
    INVALID_JSON_ENCODING_MESSAGE,
    INVALID_JSON_NUMBER_MESSAGE,
    InvalidCatalogSource,
    _is_json_media_type,
    fetch_json_from_url,
    is_public_ip,
    parse_json_document,
)
from dcatus_validation.limits import MAX_DOCUMENT_NESTING_DEPTH


def _json_response(body=b'{"dataset": []}'):
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.raise_for_status = Mock()
    response.close = Mock()
    response.iter_content = Mock(return_value=[body])
    return response


class TestFetchJsonFromUrl:
    """Tests for fetch_json_from_url function"""

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
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

    @pytest.mark.parametrize(
        "content_type",
        [
            "application/json",
            "Application/JSON; charset=UTF-8",
            "application/ld+json",
            "application/vnd.api+json; profile=example",
        ],
    )
    def test_json_media_types(self, content_type):
        assert _is_json_media_type(content_type) is True

    @pytest.mark.parametrize(
        "content_type",
        ["", "text/json", "text/html", "application/jsonp"],
    )
    def test_non_json_media_types(self, content_type):
        assert _is_json_media_type(content_type) is False

    @pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
    @patch("dcatus_validation.fetch.json.loads")
    def test_deep_json_is_rejected_before_parsing(self, mock_loads, encoding):
        depth = MAX_DOCUMENT_NESTING_DEPTH + 1
        document = ("[" * depth + "0" + "]" * depth).encode(encoding)

        with pytest.raises(CatalogTooDeeplyNested):
            parse_json_document(document)

        mock_loads.assert_not_called()

    def test_nesting_characters_inside_strings_are_ignored(self):
        brackets = "[" * (MAX_DOCUMENT_NESTING_DEPTH + 1)
        document = f'{{"value": "{brackets}"}}'

        assert parse_json_document(document) == {"value": brackets}

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_times_out(self, mock_get, caplog, monkeypatch):
        """A hung/unresponsive target raises a clear, logged ValueError instead
        of hanging the worker indefinitely."""
        # deadline set at t=100; the timeout surfaces once it has passed
        exhausted = 100.0 + FETCH_TIMEOUT_SECONDS
        clock = itertools.chain([100.0, 100.0], itertools.repeat(exhausted))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )
        mock_get.side_effect = requests.exceptions.ReadTimeout("timed out")

        with pytest.raises(
            ValueError, match=f"took longer than {FETCH_TIMEOUT_SECONDS} seconds"
        ):
            fetch_json_from_url("https://example.com/slow.json")

        timeout_record = next(
            record for record in caplog.records if "timed out" in record.message
        )
        origin, url_hash = timeout_record.args[-1].split()
        assert origin == "https://example.com"
        assert url_hash.startswith("url_hash=")
        assert len(url_hash) == len("url_hash=") + 12

    @pytest.mark.parametrize(
        "error",
        [
            requests.exceptions.ConnectTimeout("syn lost"),
            # how requests reports a TLS handshake stalled past the connect timeout
            requests.exceptions.ReadTimeout("handshake stalled"),
        ],
    )
    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_connect_phase_timeout(
        self, mock_get, caplog, monkeypatch, error
    ):
        """A connect-phase timeout is retried on a fresh connection, and once
        the attempts run out it's reported as a connect failure, not as the
        whole budget running out."""
        early = 100.0 + FETCH_CONNECT_TIMEOUT_SECONDS
        clock = itertools.chain([100.0, 100.0], itertools.repeat(early))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )
        mock_get.side_effect = error

        with pytest.raises(InvalidCatalogSource, match="Could not connect"):
            fetch_json_from_url("https://example.com/flaky.json")

        assert mock_get.call_count == FETCH_CONNECT_ATTEMPTS
        failure_record = next(
            record for record in caplog.records if "could not connect" in record.message
        )
        origin, url_hash = failure_record.args[-1].split()
        assert origin == "https://example.com"
        assert url_hash.startswith("url_hash=")
        assert len(url_hash) == len("url_hash=") + 12

    @pytest.mark.parametrize(
        "error",
        [
            requests.exceptions.ConnectTimeout("syn lost"),
            requests.exceptions.ReadTimeout("handshake stalled"),
        ],
    )
    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_retries_a_stalled_connect(self, mock_get, error):
        """The data.nola.gov case: one connection's handshake stalls, the next
        connects straight away."""
        mock_get.side_effect = [error, _json_response()]

        assert fetch_json_from_url("https://example.com/flaky.json") == {"dataset": []}
        assert mock_get.call_count == 2

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_retries_with_another_approved_address(
        self, mock_get, monkeypatch
    ):
        monkeypatch.setattr(
            "dcatus_validation.fetch._resolve_addresses",
            lambda hostname, port=None: ["2001:4860:4860::8888", "8.8.8.8"],
        )
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("network unreachable"),
            _json_response(),
        ]

        assert fetch_json_from_url("https://example.com/catalog.json") == {
            "dataset": []
        }
        assert [call.args[1] for call in mock_get.call_args_list] == [
            "2001:4860:4860::8888",
            "8.8.8.8",
        ]

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_does_not_retry_at_the_deadline(
        self, mock_get, monkeypatch
    ):
        """A server that connected but never answered used the whole budget;
        retrying can't help, and it's reported as the budget running out."""
        exhausted = 100.0 + FETCH_TIMEOUT_SECONDS
        clock = itertools.chain([100.0, 100.0], itertools.repeat(exhausted))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )
        mock_get.side_effect = requests.exceptions.ReadTimeout("no response")

        with pytest.raises(InvalidCatalogSource, match="took longer than"):
            fetch_json_from_url("https://example.com/slow.json")

        assert mock_get.call_count == 1

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_body_timeout_after_a_retry(
        self, mock_get, monkeypatch
    ):
        """Once connected, running out of budget mid-download is a slow
        response, even if an earlier connect attempt was retried."""
        # deadline, attempt 1, its timeout check, attempt 2 - then the budget
        # is gone by the first downloaded chunk
        clock = itertools.chain([100.0] * 4, itertools.repeat(200.0))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )
        mock_get.side_effect = [
            requests.exceptions.ConnectTimeout("syn lost"),
            _json_response(),
        ]

        with pytest.raises(InvalidCatalogSource, match="took longer than"):
            fetch_json_from_url("https://example.com/slow-body.json")

    def test_connect_attempts_fit_inside_the_fetch_budget(self):
        """Every connect attempt fits inside the one deadline, each allows for
        a quick SYN retransmit (~1s, then ~3s), and validation (up to ~12s at
        10MB) has to fit after the fetch inside CloudFront's 30s, with
        padding."""
        assert FETCH_CONNECT_TIMEOUT_SECONDS >= 3.5
        assert FETCH_CONNECT_TIMEOUT_SECONDS * FETCH_CONNECT_ATTEMPTS <= (
            FETCH_TIMEOUT_SECONDS
        )
        assert FETCH_TIMEOUT_SECONDS + 12 <= 25

    @patch("dcatus_validation.fetch._pinned_get")
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
        stream_timeouts = mock_response.raw._connection.sock.settimeout.call_args_list
        assert stream_timeouts
        assert all(
            0 < call.args[0] <= FETCH_TIMEOUT_SECONDS for call in stream_timeouts
        )

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_redirect_budget_shrinks_over_time(
        self, mock_get, monkeypatch
    ):
        """The timeout budget is shared across the whole fetch, not reset on
        every redirect hop - otherwise a chain of slow redirects could run for
        MAX_FETCH_REDIRECTS x FETCH_TIMEOUT_SECONDS in total."""

        # deadline set at t=100; hop 1's request issued at t=100 (full budget
        # left); 4s "pass" before hop 2's request is issued.
        clock = itertools.chain([100.0, 100.0, 104.0], itertools.repeat(104.0))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )

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

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_redirect_chain_exceeding_budget_times_out(
        self, mock_get, monkeypatch
    ):
        """If the budget is exhausted partway through a redirect chain, the
        next hop never gets a fresh budget - it times out instead."""

        # deadline at t=100 + FETCH_TIMEOUT_SECONDS; by the second hop more
        # than the whole budget has elapsed, so there is nothing left to spend.
        exhausted = 100.0 + FETCH_TIMEOUT_SECONDS + 1
        clock = itertools.chain([100.0, 100.0, exhausted], itertools.repeat(exhausted))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )

        redirect_response = Mock()
        redirect_response.status_code = 302
        redirect_response.headers = {"Location": "https://example.com/final.json"}
        redirect_response.close = Mock()
        mock_get.return_value = redirect_response

        with pytest.raises(ValueError, match="took longer than"):
            fetch_json_from_url("https://example.com/redirect-me")

        # never attempted a second request once the shared budget was gone
        assert mock_get.call_count == 1

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_rejects_redirect_to_private_address_in_prod(
        self, mock_get, monkeypatch
    ):
        """A redirect target is re-validated like any other URL - a public URL
        that redirects to a private/internal address is refused in prod, and
        the redirect is never followed."""
        monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", False)

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

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
    def test_rejections_raise_disclosable_exception(self, mock_get, monkeypatch):
        """Every submitter-actionable refusal raises InvalidCatalogSource, which
        is what lets both callers show the reason - the API answers 400 with
        str(e) only for this type, and falls back to a generic 500 otherwise."""
        monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", False)

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

    @patch("dcatus_validation.fetch._pinned_get")
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

    @patch("dcatus_validation.fetch._pinned_get")
    def test_invalid_json_encoding_is_a_submission_error(self, mock_get):
        mock_get.return_value = _json_response(b'{"secret":"\xff"}')

        with pytest.raises(InvalidCatalogSource, match=INVALID_JSON_ENCODING_MESSAGE):
            fetch_json_from_url("https://example.com/broken.json")

    @pytest.mark.parametrize(
        "body",
        [
            b'{"value": NaN}',
            b'{"value": Infinity}',
            b'{"value": 1e9999}',
            b'{"value": ' + b"9" * 5000 + b"}",
        ],
    )
    @patch("dcatus_validation.fetch._pinned_get")
    def test_unsupported_json_numbers_are_submission_errors(self, mock_get, body):
        mock_get.return_value = _json_response(body)

        with pytest.raises(InvalidCatalogSource, match=INVALID_JSON_NUMBER_MESSAGE):
            fetch_json_from_url("https://example.com/broken.json")

    @patch("dcatus_validation.fetch._pinned_get")
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
        assert any("SSLError" in record.message for record in caplog.records)
        assert not any("ca-bundle" in record.message for record in caplog.records)

    def test_pinned_get_ignores_ambient_credentials_and_proxies(self, monkeypatch):
        monkeypatch.setenv("https_proxy", "http://proxy.internal:8080")
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/tmp/test-ca.pem")

        with patch("requests.Session.get") as session_get:
            session_get.return_value = Mock()
            response = fetch._pinned_get(
                "https://example.com/data.json",
                "93.184.216.34",
                (1, 2),
            )

        assert response._validator_session.trust_env is False
        assert session_get.call_args.kwargs["allow_redirects"] is False
        assert session_get.call_args.kwargs["verify"] == "/tmp/test-ca.pem"
        response._validator_session.close()

    def test_hostname_resolution_is_pinned_for_the_connection(self, monkeypatch):
        class CatalogHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'{"dataset": []}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), CatalogHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        original_getaddrinfo = socket.getaddrinfo
        hostname_resolutions = 0

        def rebinding_getaddrinfo(host, port, *args, **kwargs):
            nonlocal hostname_resolutions
            if host == "rebind.test":
                hostname_resolutions += 1
                if hostname_resolutions > 1:
                    raise AssertionError("hostname was resolved more than once")
                return [
                    (
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP,
                        "",
                        ("127.0.0.1", port),
                    )
                ]
            return original_getaddrinfo(host, port, *args, **kwargs)

        monkeypatch.setattr(fetch, "ALLOW_PRIVATE_ADDRESSES", True)
        monkeypatch.setattr(socket, "getaddrinfo", rebinding_getaddrinfo)
        try:
            result = fetch_json_from_url(
                f"http://rebind.test:{server.server_port}/catalog.json"
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

        assert result == {"dataset": []}
        assert hostname_resolutions == 1

    def test_url_credentials_are_rejected_without_being_logged(
        self, caplog, monkeypatch
    ):
        monkeypatch.setattr(fetch, "ALLOW_PRIVATE_ADDRESSES", True)

        with pytest.raises(InvalidCatalogSource, match="credentials"):
            fetch_json_from_url("https://user:hunter2@example.com/catalog.json")

        assert "hunter2" not in caplog.text

    @patch("dcatus_validation.fetch._pinned_get")
    def test_signed_query_is_not_logged(self, mock_get, caplog):
        mock_get.return_value = _json_response()

        fetch_json_from_url("https://example.com/catalog.json?access_token=hunter2")

        assert "hunter2" not in caplog.text
        assert "catalog.json" not in caplog.text
        assert "url_hash=" in caplog.text


class TestPrivateAddressDefault:
    def test_shared_address_space_is_not_public(self, monkeypatch):
        """RFC 6598 addresses are not `is_private`, but can still be internal."""
        monkeypatch.setattr(
            "dcatus_validation.fetch._resolve_addresses",
            lambda hostname, port=None: ["100.64.0.1"],
        )

        assert is_public_ip("shared.example") is False

    @pytest.mark.parametrize(
        "address",
        [
            "::ffff:100.64.0.1",
            "::ffff:192.0.0.8",
            "::ffff:192.88.99.1",
            "::127.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::c000:8",
            "64:ff9b::c058:6301",
            "64:ff9b::e000:1",
            "64:ff9b:1::7f00:1",
            "2002:7f00:1::",
            "2002:0808:0808::",
            "192.0.0.8",
            "192.88.99.1",
            "224.0.0.1",
            "3fff::1",
            "fec0::1",
            "ff0e::1",
        ],
    )
    def test_embedded_non_public_ipv4_is_not_public(self, address, monkeypatch):
        monkeypatch.setattr(
            "dcatus_validation.fetch._resolve_addresses",
            lambda hostname, port=None: [address],
        )

        assert is_public_ip("embedded.example") is False

    @pytest.mark.parametrize(
        "address",
        [
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "8.8.8.8",
            "2001:4860:4860::8888",
        ],
    )
    def test_embedded_public_ipv4_remains_public(self, address, monkeypatch):
        monkeypatch.setattr(
            "dcatus_validation.fetch._resolve_addresses",
            lambda hostname, port=None: [address],
        )

        assert is_public_ip("embedded.example") is True

    def test_private_addresses_are_refused_unless_explicitly_allowed(self, monkeypatch):
        """Fail closed: an unset ALLOW_PRIVATE_ADDRESSES must mean refuse."""
        import importlib

        import dcatus_validation.fetch

        monkeypatch.delenv("ALLOW_PRIVATE_ADDRESSES", raising=False)
        try:
            module = importlib.reload(dcatus_validation.fetch)
            assert module.ALLOW_PRIVATE_ADDRESSES is False
            with pytest.raises(
                module.InvalidCatalogSource, match="private/internal addresses"
            ):
                module.fetch_json_from_url("http://127.0.0.1/secret.json")
        finally:
            monkeypatch.undo()
            importlib.reload(dcatus_validation.fetch)
