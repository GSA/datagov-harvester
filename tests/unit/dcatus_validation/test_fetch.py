import itertools
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

import pytest
import requests

import dcatus_validation.fetch as fetch
from dcatus_validation.errors import CatalogTooDeeplyNested
from dcatus_validation.fetch import (
    BLOCKED_VALIDATOR_DOMAIN_MESSAGE,
    FETCH_CONNECT_ATTEMPTS,
    FETCH_CONNECT_TIMEOUT_SECONDS,
    FETCH_TIMEOUT_SECONDS,
    INVALID_JSON_ENCODING_MESSAGE,
    INVALID_JSON_NUMBER_MESSAGE,
    InvalidCatalogSource,
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

    @pytest.mark.parametrize("domain", ["nip.io", "rbndr.us", "sslip.io", "traefik.me"])
    @pytest.mark.parametrize(
        "host", ["{domain}", "sub.{domain}", "SUB.{domain}.", "sub。{domain}"]
    )
    def test_blocked_domain_is_rejected_before_dns_or_fetch(
        self, domain, host, monkeypatch
    ):
        resolve = Mock()
        get = Mock()
        monkeypatch.setattr(fetch, "_resolve_addresses", resolve)
        monkeypatch.setattr(fetch, "_http_get", get)

        with pytest.raises(
            InvalidCatalogSource, match=BLOCKED_VALIDATOR_DOMAIN_MESSAGE
        ):
            fetch_json_from_url(f"https://{host.format(domain=domain)}/data.json")

        resolve.assert_not_called()
        get.assert_not_called()

    @pytest.mark.parametrize(
        "host",
        [
            "notnip.io",
            "nip.io.example.com",
            "rbndr.us.evil.org",
            "example.gov",
            "www.yahoo.com",
        ],
    )
    def test_other_domains_use_existing_fetch_rules(self, host, monkeypatch):
        resolve = Mock(return_value=["8.8.8.8"])
        get = Mock(return_value=_json_response())
        monkeypatch.setattr(fetch, "_resolve_addresses", resolve)
        monkeypatch.setattr(fetch, "_http_get", get)

        assert fetch_json_from_url(f"https://{host}/data.json") == {"dataset": []}
        resolve.assert_called_once_with(host, None)
        get.assert_called_once()

    def test_redirect_to_blocked_domain_is_rejected_before_dns_or_fetch(
        self, monkeypatch
    ):
        redirect = Mock(
            status_code=302, headers={"Location": "https://sub.nip.io/data.json"}
        )
        redirect.close = Mock()
        resolve = Mock(return_value=["8.8.8.8"])
        get = Mock(return_value=redirect)
        monkeypatch.setattr(fetch, "_resolve_addresses", resolve)
        monkeypatch.setattr(fetch, "_http_get", get)

        with pytest.raises(
            InvalidCatalogSource, match=BLOCKED_VALIDATOR_DOMAIN_MESSAGE
        ):
            fetch_json_from_url("https://example.gov/redirect")

        resolve.assert_called_once_with("example.gov", None)
        get.assert_called_once()
        redirect.close.assert_called()

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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
            "text/plain; charset=utf-8",
            "application/octet-stream",
            None,
        ],
    )
    @patch("dcatus_validation.fetch._http_get")
    def test_content_type_is_not_required_to_be_json(self, mock_get, content_type):
        """Like the harvester, the body decides whether it is JSON - hosts such
        as raw.githubusercontent.com and S3 serve data.json as text/plain or
        octet-stream, or omit the header entirely."""
        response = _json_response(b'{"test": "data"}')
        response.headers = (
            {} if content_type is None else {"Content-Type": content_type}
        )
        mock_get.return_value = response

        assert fetch_json_from_url("https://example.com/data.json") == {"test": "data"}

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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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
    @patch("dcatus_validation.fetch._http_get")
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
    @patch("dcatus_validation.fetch._http_get")
    def test_fetch_json_from_url_retries_a_stalled_connect(self, mock_get, error):
        """The data.nola.gov case: one connection's handshake stalls, the next
        connects straight away."""
        mock_get.side_effect = [error, _json_response()]

        assert fetch_json_from_url("https://example.com/flaky.json") == {"dataset": []}
        assert mock_get.call_count == 2

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
    def test_rejections_raise_disclosable_exception(self, mock_get, monkeypatch):
        """Every submitter-actionable refusal raises InvalidCatalogSource, which
        is what lets both callers show the reason - the API answers 400 with
        str(e) only for this type, and falls back to a generic 500 otherwise."""
        monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", False)

        not_json = _json_response(b"<html><body>Not a catalog</body></html>")
        not_json.headers = {"Content-Type": "text/html"}

        cases = {
            "ftp://example.com/data.json": "Only HTTP/HTTPS URLs are allowed.",
            "http://127.0.0.1/data.json": "private/internal addresses",
        }
        for url, expected in cases.items():
            with pytest.raises(InvalidCatalogSource, match=expected):
                fetch_json_from_url(url)

        mock_get.return_value = not_json
        with pytest.raises(InvalidCatalogSource, match="Invalid JSON at line 1"):
            fetch_json_from_url("https://example.com/page.html")

    @patch("dcatus_validation.fetch._http_get")
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

    @patch("dcatus_validation.fetch._http_get")
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
    @patch("dcatus_validation.fetch._http_get")
    def test_unsupported_json_numbers_are_submission_errors(self, mock_get, body):
        mock_get.return_value = _json_response(body)

        with pytest.raises(InvalidCatalogSource, match=INVALID_JSON_NUMBER_MESSAGE):
            fetch_json_from_url("https://example.com/broken.json")

    @patch("dcatus_validation.fetch._http_get")
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

    def test_url_credentials_are_rejected_without_being_logged(
        self, caplog, monkeypatch
    ):
        monkeypatch.setattr(fetch, "ALLOW_PRIVATE_ADDRESSES", True)

        with pytest.raises(InvalidCatalogSource, match="credentials"):
            fetch_json_from_url("https://user:hunter2@example.com/catalog.json")

        assert "hunter2" not in caplog.text

    @patch("dcatus_validation.fetch._http_get")
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


class _ForwardProxy:
    """A minimal forward proxy on localhost that records what it was asked for.

    Plain-HTTP targets arrive in absolute form and are answered directly with a
    small catalog. CONNECT requests are refused with 403, the way the egress
    proxy refuses a denied host or port.
    """

    def __init__(self):
        self.requests = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                proxy.requests.append((self.command, self.path))
                body = b'{"dataset": []}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_CONNECT(self):
                proxy.requests.append((self.command, self.path))
                self.send_response(403)
                self.end_headers()

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class TestEgressProxy:
    """On cloud.gov, `.profile` sets http_proxy/https_proxy from the egress
    proxy's proxy_url, and fetches go through it like the harvester's do."""

    # Through the module, not the names imported above:
    # TestPrivateAddressDefault reloads it, replacing its classes.

    @pytest.fixture(autouse=True)
    def public_target(self, monkeypatch):
        """The targets resolve to a public address that isn't serving them, so
        a fetch can only return the proxy's answer if it went through it."""
        monkeypatch.setattr(fetch, "ALLOW_PRIVATE_ADDRESSES", False)
        monkeypatch.setattr(
            fetch, "_resolve_addresses", lambda hostname, port=None: ["8.8.8.8"]
        )
        monkeypatch.delenv("no_proxy", raising=False)
        monkeypatch.delenv("NO_PROXY", raising=False)

    def test_fetch_uses_the_proxy_from_the_environment(self, monkeypatch):
        with _ForwardProxy() as proxy:
            monkeypatch.setenv("http_proxy", proxy.url)
            monkeypatch.setenv("https_proxy", proxy.url)

            assert fetch.fetch_json_from_url("http://catalog.test/data.json") == {
                "dataset": []
            }
            with pytest.raises(fetch.InvalidCatalogSource, match="Could not retrieve"):
                fetch.fetch_json_from_url("https://catalog.test:8443/data.json")

        assert proxy.requests == [
            ("GET", "http://catalog.test/data.json"),
            ("CONNECT", "catalog.test:8443"),
        ]

    def test_private_targets_are_refused_before_reaching_the_proxy(self, monkeypatch):
        monkeypatch.setattr(
            fetch, "_resolve_addresses", lambda hostname, port=None: ["10.0.0.5"]
        )
        with _ForwardProxy() as proxy:
            monkeypatch.setenv("http_proxy", proxy.url)
            with pytest.raises(fetch.InvalidCatalogSource, match="private/internal"):
                fetch.fetch_json_from_url("http://internal.test/data.json")

        assert proxy.requests == []
