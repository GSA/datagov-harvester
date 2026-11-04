import codecs
import itertools
import json
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from unittest.mock import Mock, patch

import pytest
import requests
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import dcatus_validation.fetch as fetch
from dcatus_validation.errors import CatalogTooDeeplyNested
from dcatus_validation.fetch import (
    BLOCKED_VALIDATOR_DOMAIN_MESSAGE,
    FETCH_CONNECT_ATTEMPTS,
    FETCH_CONNECT_TIMEOUT_SECONDS,
    FETCH_TIMEOUT_SECONDS,
    INVALID_JSON_ENCODING_MESSAGE,
    INVALID_JSON_NUMBER_MESSAGE,
    PAYLOAD_TOO_LARGE_MESSAGE,
    InvalidCatalogSource,
    fetch_json_from_url,
    is_public_ip,
    parse_json_document,
)
from dcatus_validation.limits import (
    MAX_DOCUMENT_NESTING_DEPTH,
    MAX_REQUEST_BYTES,
    MAX_REQUEST_OVERHEAD_BYTES,
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_MB,
)
from dcatus_validation.validate import validate_records
from tests.unit.dcatus_validation.test_equivalence import use_fast_path as fast_path


def _json_body_of_size(size: int) -> bytes:
    """A valid JSON catalog of exactly `size` bytes."""
    prefix, suffix = b'{"dataset": [], "padding": "', b'"}'
    body = prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix
    assert len(body) == size
    return body


def _json_response(body=b'{"dataset": []}'):
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.raise_for_status = Mock()
    response.close = Mock()
    response.iter_content = Mock(return_value=[body])
    response._validator_timeout_controller = Mock()
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
        monkeypatch.setattr(fetch, "_pinned_get", get)

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
        monkeypatch.setattr(fetch, "_pinned_get", get)

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
        monkeypatch.setattr(fetch, "_pinned_get", get)

        with pytest.raises(
            InvalidCatalogSource, match=BLOCKED_VALIDATOR_DOMAIN_MESSAGE
        ):
            fetch_json_from_url("https://example.gov/redirect")

        resolve.assert_called_once_with("example.gov", None)
        get.assert_called_once()
        redirect.close.assert_called()

    @pytest.mark.parametrize(
        "size_offset, accepted",
        [(-1, True), (0, True), (1, False)],
        ids=["just-under", "at", "just-over"],
    )
    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_size_limit_boundary(
        self, mock_get, monkeypatch, size_offset, accepted
    ):
        """The limit is inclusive: a body of exactly MAX_UPLOAD_BYTES is
        fetched, one byte more is refused. The limit is patched down so the
        boundary is cheap to build; test_size_limit_is_150mb pins the real
        value."""
        limit = 4096
        monkeypatch.setattr(fetch, "MAX_UPLOAD_BYTES", limit)
        body = _json_body_of_size(limit + size_offset)
        mock_get.return_value = _json_response(body)

        if accepted:
            assert fetch_json_from_url("https://example.com/data.json") == (
                json.loads(body)
            )
        else:
            with pytest.raises(InvalidCatalogSource) as exc_info:
                fetch_json_from_url("https://example.com/data.json")
            assert exc_info.value.public_message == PAYLOAD_TOO_LARGE_MESSAGE

    @pytest.mark.parametrize(
        "size_offset, accepted",
        [(-1, True), (0, True), (1, False)],
        ids=["just-under", "at", "just-over"],
    )
    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_content_length_boundary(
        self, mock_get, monkeypatch, size_offset, accepted
    ):
        """A Content-Length over the limit is refused before any of the body
        is read; one at the limit is allowed through to the streamed count."""
        limit = 4096
        monkeypatch.setattr(fetch, "MAX_UPLOAD_BYTES", limit)
        response = _json_response(b'{"dataset": []}')
        response.headers["Content-Length"] = str(limit + size_offset)
        mock_get.return_value = response

        if accepted:
            assert fetch_json_from_url("https://example.com/data.json") == {
                "dataset": []
            }
        else:
            with pytest.raises(InvalidCatalogSource, match=PAYLOAD_TOO_LARGE_MESSAGE):
                fetch_json_from_url("https://example.com/data.json")
            response.iter_content.assert_not_called()

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_exceeds_size_limit(self, mock_get):
        """At the real limit: a body one byte over 150MB is refused, built
        from a reused chunk rather than one 150MB allocation."""
        chunk = b"x" * (1024 * 1024)
        chunk_count = MAX_UPLOAD_MB
        mock_get.return_value = _json_response()
        mock_get.return_value.iter_content = Mock(
            side_effect=lambda chunk_size: itertools.chain(
                itertools.repeat(chunk, chunk_count), [b"x"]
            )
        )

        with pytest.raises(
            ValueError, match="JSON payload too large - must be 150MB or less."
        ):
            fetch_json_from_url("https://example.com/large-file.json")

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_within_size_limit(self, mock_get):
        """Test that fetch_json_from_url succeeds when content is within the limit"""
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
    def test_content_type_is_not_required_to_be_json(self, content_type, monkeypatch):
        """Like the harvester, the body decides whether it is JSON - hosts such
        as raw.githubusercontent.com and S3 serve data.json as text/plain or
        octet-stream, or omit the header entirely."""
        response = _json_response(b'{"test": "data"}')
        response.headers = (
            {} if content_type is None else {"Content-Type": content_type}
        )
        monkeypatch.setattr(fetch, "_pinned_get", Mock(return_value=response))

        assert fetch_json_from_url("https://example.com/data.json") == {"test": "data"}

    @pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
    @patch("dcatus_validation.fetch.json.loads")
    def test_deep_json_is_rejected_before_parsing(
        self, mock_loads, encoding, monkeypatch
    ):
        """The pure-Python parser, which parses whatever orjson refuses, scans
        for depth before json.loads can recurse."""
        depth = MAX_DOCUMENT_NESTING_DEPTH + 1
        document = ("[" * depth + "0" + "]" * depth).encode(encoding)

        with fast_path(False), pytest.raises(CatalogTooDeeplyNested):
            parse_json_document(document)

        mock_loads.assert_not_called()

    @pytest.mark.parametrize("encoding", ["utf-16", "utf-32"])
    @patch("dcatus_validation.fetch.json.loads")
    def test_deep_json_orjson_refuses_is_rejected_before_parsing(
        self, mock_loads, encoding
    ):
        """orjson takes UTF-8 only; anything else gets the scan as before."""
        depth = MAX_DOCUMENT_NESTING_DEPTH + 1
        document = ("[" * depth + "0" + "]" * depth).encode(encoding)

        with pytest.raises(CatalogTooDeeplyNested):
            parse_json_document(document)

        mock_loads.assert_not_called()

    @pytest.mark.parametrize("depth", [MAX_DOCUMENT_NESTING_DEPTH + 1, 1024, 100_000])
    @patch("dcatus_validation.fetch.json.loads")
    def test_deep_json_never_reaches_json_loads(self, mock_loads, depth):
        """The fast path: orjson parses up to 1024 levels without recursing
        (the post-parse walk in validate.py then refuses anything past
        MAX_DOCUMENT_NESTING_DEPTH); deeper than that, the scan refuses it."""
        document = ("[" * depth + "0" + "]" * depth).encode()

        try:
            parse_json_document(document)
        except CatalogTooDeeplyNested:
            assert depth > 1024
        else:
            assert depth <= 1024

        mock_loads.assert_not_called()

    def test_nesting_characters_inside_strings_are_ignored(self):
        brackets = "[" * (MAX_DOCUMENT_NESTING_DEPTH + 1)
        document = f'{{"value": "{brackets}"}}'

        assert parse_json_document(document) == {"value": brackets}

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_content_length_exceeds_limit(self, mock_get):
        """Test that fetch_json_from_url raises ValueError when Content-Length
        header exceeds 150MB"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Type": "application/json",
            "Content-Length": str(MAX_UPLOAD_BYTES + 1),
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(
            ValueError, match="JSON payload too large - must be 150MB or less."
        ):
            fetch_json_from_url("https://example.com/large-file.json")

    @patch("dcatus_validation.fetch._pinned_get")
    def test_fetch_json_from_url_stops_streaming_when_limit_exceeded(
        self, mock_get, monkeypatch
    ):
        """Test that fetch_json_from_url stops downloading chunks once the
        size exceeds the limit (patched down to 10 chunks' worth)"""
        chunk_size = 1024
        monkeypatch.setattr(fetch, "MAX_UPLOAD_BYTES", 10 * chunk_size)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.raise_for_status = Mock()
        mock_response.close = Mock()

        chunks_generated = []

        def generate_chunks():
            for i in range(15):
                chunks_generated.append(i)
                yield b"x" * chunk_size

        mock_response.iter_content = Mock(
            side_effect=lambda chunk_size: generate_chunks()
        )
        mock_get.return_value = mock_response

        with pytest.raises(InvalidCatalogSource, match=PAYLOAD_TOO_LARGE_MESSAGE):
            fetch_json_from_url("https://example.com/large-file.json")

        # Verify we stopped after the 11th chunk crossed the limit, not all 15
        assert len(chunks_generated) == 11
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
        a quick SYN retransmit (~1s, then ~3s), and validation has to fit
        after the fetch inside CloudFront's 30s, with padding. The fast path
        validates even a 150MB catalog in ~2s on a fast laptop
        (GSA/data.gov#6460); 12s leaves room for slower production CPUs."""
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
        stream_timeouts = (
            mock_response._validator_timeout_controller.settimeout.call_args_list
        )
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


class TestFetchedDocumentsParseTheSameOnBothPaths:
    """The URL path through fetch_json_from_url, with the fast path
    (orjson) on and off: the same document or the same refusal
    (GSA/data.gov#6460; the parse-equivalence tests below
    cover parse_json_document itself)."""

    BODIES = {
        "catalog": b'{"dataset": [{"title": "x"}]}',
        "UTF-8 BOM": b'\xef\xbb\xbf{"dataset": []}',
        "UTF-16": '{"dataset": []}'.encode("utf-16"),
        "invalid UTF-8": b'{"secret":"\xff"}',
        "NaN": b'{"value": NaN}',
        "infinite float": b'{"value": 1e9999}',
        "huge integer": b'{"value": ' + b"9" * 5000 + b"}",
        "integer beyond 64 bits": b'{"value": 18446744073709551616}',
        "escaped lone surrogate": b'{"value": "\\ud800"}',
        "duplicate keys": b'{"a": 1, "a": 2}',
        "syntax error": b'{"internal_secret": "hunter2",, "b": 2}',
        "HTML": b"<html><body>Not JSON</body></html>",
        "empty": b"",
        "101 deep": b"[" * 101 + b"]" * 101,
        "100,000 deep": b"[" * 100_000 + b"]" * 100_000,
    }

    @staticmethod
    def _fetch(monkeypatch, body, fast):
        # Through the module: a test above reloads it, which replaces
        # InvalidCatalogSource and fetch_json_from_url.
        try:
            with fast_path(fast):
                document = fetch.fetch_json_from_url("https://example.com/d.json")
            return ("document", repr(document))
        except fetch.InvalidCatalogSource as error:
            return ("refused", error.public_message)
        except CatalogTooDeeplyNested as error:
            return ("too deep", str(error))

    @pytest.mark.parametrize("body", BODIES.values(), ids=list(BODIES))
    @patch("dcatus_validation.fetch._pinned_get")
    def test_same_result(self, mock_get, monkeypatch, body):
        mock_get.side_effect = lambda *args, **kwargs: _json_response(body)

        old = self._fetch(monkeypatch, body, fast=False)
        new = self._fetch(monkeypatch, body, fast=True)

        if body.startswith(b"[" * 101) and len(body) < 2050:
            # Refused while parsing on the old path; parsed on the fast path
            # and refused by validation's post-parse walk, with the same
            # error (see the parse-equivalence tests below).
            assert old[0] == "too deep"
            assert new[0] == "document"
        else:
            assert new == old


NGINX_SIZE_UNITS = {"": 1, "k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}


def _validate_location_body_limit(config_path: str) -> int:
    """client_max_body_size inside the validate API location block, in bytes."""
    config = Path(config_path).read_text()
    location = re.search(
        r"location ~ \^/api\(/v1\)\?/validate\(/document\)\?\$ \{(?P<body>[^}]*)\}",
        config,
    )
    assert location, f"no validate API location in {config_path}"
    size = re.search(
        r"client_max_body_size\s+(\d+)([kKmMgG]?);", location.group("body")
    )
    assert size, f"no client_max_body_size in {config_path}'s validate location"
    return int(size.group(1)) * NGINX_SIZE_UNITS[size.group(2).lower()]


def test_size_limit_is_150mb():
    """GSA/data.gov#6460: 150MB, not the 600MB one Census file asked for."""
    assert MAX_UPLOAD_MB == 150
    assert MAX_UPLOAD_BYTES == 150 * 1024 * 1024
    assert PAYLOAD_TOO_LARGE_MESSAGE == (
        "JSON payload too large - must be 150MB or less."
    )


def test_request_limit_fits_a_json_escaped_document_at_the_limit():
    """1.5x the document limit, for JSON escaping, plus the other API fields:
    225MB + 1KB, so nginx's 226M sits just above it (GSA/data.gov#6460)."""
    assert MAX_REQUEST_BYTES == MAX_UPLOAD_BYTES * 3 // 2 + MAX_REQUEST_OVERHEAD_BYTES
    assert MAX_REQUEST_BYTES == 225 * 1024 * 1024 + 1024


@pytest.mark.parametrize(
    "config_path", ["proxy/nginx-common.conf", "nginx-local-proxy.conf"]
)
def test_proxy_body_limit_sits_just_above_the_validator_limit(config_path):
    """Above MAX_REQUEST_BYTES, so the validator's own JSON 413 is what callers
    see rather than nginx's bare HTML one; within a few MB of it, so nginx
    still refuses anything far larger before buffering it to disk."""
    proxy_limit = _validate_location_body_limit(config_path)

    assert MAX_REQUEST_BYTES < proxy_limit <= MAX_REQUEST_BYTES + 2 * 1024 * 1024


# orjson against the stdlib parser (GSA/data.gov#6460): every document parses
# to the same value, or fails with the same error, on both paths. Where orjson
# differs from json.loads, parse_json_document re-parses with the stdlib path,
# so each difference below is pinned with a test that it ends the same.  The
# one deliberate change: a document nested 101 to 1024 levels deep now gets
# past the parser, since orjson never recurses, and the post-parse depth check
# in validate.py rejects it with the same CatalogTooDeeplyNested. The API's
# response is unchanged (tests/integration/validator_api).


EXAMPLE_COUNT = int(os.getenv("DCATUS_EQUIVALENCE_EXAMPLES", "300"))
HYPOTHESIS_SETTINGS = settings(
    max_examples=EXAMPLE_COUNT,
    derandomize=True,
    deadline=None,
    suppress_health_check=list(HealthCheck),
)


def parse_outcome(document):
    """What a caller can observe: the value (repr keeps 1, 1.0 and True, and
    key order, apart) or the error and everything it carries."""
    # Through the module: tests/unit/dcatus_validation/test_fetch.py reloads
    # it, which replaces InvalidCatalogSource and parse_json_document.
    try:
        return ("value", repr(fetch.parse_json_document(document)))
    except json.JSONDecodeError as error:
        return ("JSONDecodeError", error.msg, error.lineno, error.colno, error.pos)
    except fetch.InvalidCatalogSource as error:
        return ("InvalidCatalogSource", error.public_message)
    except Exception as error:
        return (type(error).__name__, str(error))


def both_parsers(document):
    with fast_path(False):
        old = parse_outcome(document)
    with fast_path(True):
        new = parse_outcome(document)
    return old, new


def orjson_accepts(document) -> bool:
    return fetch._parse_json_document_orjson(document) is not fetch._ORJSON_REFUSED


def nested(depth, encoding=None):
    document = "[" * depth + "0" + "]" * depth
    return document.encode(encoding) if encoding else document


# (document, whether orjson accepts it): each difference between orjson and
# json.loads, and the cases around it.
CASES = {
    # NaN and Infinity: orjson refuses them as syntax; the stdlib path refuses
    # them as unsupported numbers.
    "NaN": (b'{"v": NaN}', False),
    "Infinity": (b'{"v": Infinity}', False),
    "-Infinity": (b'{"v": -Infinity}', False),
    "overflowing float": (b'{"v": 1e9999}', False),
    "overflowing negative float": (b'{"v": -1e9999}', False),
    "underflowing float": (b'{"v": 1e-400}', True),
    "smallest subnormal": (b'{"v": 5e-324}', True),
    # Any float of 2**63 or more is re-parsed: it could have been an integer.
    "largest float": (b'{"v": 1.7976931348623157e308}', False),
    "1e19": (b'{"v": 1e19}', False),
    "negative zero": (b'{"v": -0.0}', True),
    "integer negative zero": (b'{"v": -0}', True),
    # Integers: exact to 64 bits in orjson; beyond that it makes floats, which
    # the stdlib re-parses to exact ints.
    "u64 max": (b'{"v": 18446744073709551615}', True),
    "u64 max + 1": (b'{"v": 18446744073709551616}', False),
    "i64 min": (b'{"v": -9223372036854775808}', True),
    "i64 min - 1": (b'{"v": -9223372036854775809}', False),
    "100 digits": (b'{"v": ' + b"9" * 100 + b"}", False),
    "4300 digits": (b'{"v": ' + b"9" * 4300 + b"}", False),
    "4301 digits": (b'{"v": ' + b"9" * 4301 + b"}", False),
    "big int in a list": (b"[1, 2, " + b"7" * 30 + b"]", False),
    "float at 2**63": (b'{"v": 9223372036854775808.0}', False),
    "float just under 2**63": (b'{"v": 9223372036854774784.0}', True),
    # Duplicate keys: the last value wins, in the first key's position.
    "duplicate keys": (b'{"a": 1, "b": 2, "a": 3}', True),
    "duplicate nested keys": (b'{"a": {"x": 1, "x": [2]}}', True),
    # Byte order marks and encodings: orjson takes UTF-8 alone.
    "UTF-8 BOM bytes": (codecs.BOM_UTF8 + b'{"a": 1}', False),
    "UTF-8 BOM str": ('﻿{"a": 1}', False),
    "UTF-16 BOM": ('{"a": "é"}'.encode("utf-16"), False),
    "UTF-16-LE": ('{"a": 1}'.encode("utf-16-le"), False),
    "UTF-16-BE": ('{"a": 1}'.encode("utf-16-be"), False),
    "UTF-32 BOM": ('{"a": 1}'.encode("utf-32"), False),
    "UTF-32-LE": ('{"a": 1}'.encode("utf-32-le"), False),
    # Invalid UTF-8 and surrogates.
    "invalid UTF-8": (b'{"a": "\xff"}', False),
    "overlong UTF-8": (b'{"a": "\xc0\xaf"}', False),
    "UTF-8 encoded surrogate": (b'{"a": "\xed\xa0\x80"}', False),
    "truncated UTF-8": (b'{"a": "\xe2\x82"}', False),
    "escaped lone high surrogate": (b'{"a": "\\ud800"}', False),
    "escaped lone low surrogate": (b'{"a": "\\udc00"}', False),
    "escaped surrogate pair": (b'{"a": "\\ud83d\\ude00"}', True),
    "lone surrogate in pasted str": ('{"a": "\ud800"}', False),
    "non-ASCII": ('{"a": "é中\U0001f600"}'.encode(), True),
    "non-ASCII str": ('{"a": "é中\U0001f600"}', True),
    "escaped NUL": (b'{"a": "\\u0000"}', True),
    # Syntax errors: both refuse, and the stdlib's position is reported.
    "raw control character": (b'{"a": "\x01"}', False),
    "raw tab": (b'{"a": "\t"}', False),
    "raw NUL": (b'{"a": "\x00"}', False),
    "DEL": (b'{"a": "\x7f"}', True),
    "trailing comma": (b'{"a": 1,}', False),
    "comment": (b'{"a": 1 /* x */}', False),
    "single quotes": (b"{'a': 1}", False),
    "leading zero": (b'{"a": 01}', False),
    "bare decimal point": (b'{"a": .5}', False),
    "trailing decimal point": (b'{"a": 1.}', False),
    "plus sign": (b'{"a": +1}', False),
    "hex": (b'{"a": 0x10}', False),
    "bad escape": (b'{"a": "\\x41"}', False),
    "unterminated string": (b'{"a": "x', False),
    "missing value": (b'{"a": }', False),
    "extra data": (b"{} x", False),
    "NUL after document": (b"{}\x00", False),
    "vertical tab as whitespace": (b'{\x0b"a": 1}', False),
    "no-break space as whitespace": (b'{\xc2\xa0"a": 1}', False),
    "empty": (b"", False),
    "empty str": ("", False),
    "whitespace only": (b"  \n ", False),
    "scalar": (b"1", True),
    "string scalar": (b'"x"', True),
    "null": (b"null", True),
    "error on line 3": (b'{\n  "a": 1,\n  "b": ]\n}', False),
    "secret in a broken document": (b'{"internal_secret": "hunter2",, "b": 2}', False),
    # Nesting: orjson refuses past 1024 levels; the stdlib's scan refuses past
    # 100. Up to 100 both parse.
    "depth 100": (nested(MAX_DOCUMENT_NESTING_DEPTH, "utf-8"), True),
    "depth 1025": (nested(1025, "utf-8"), False),
    "depth 100,000": (nested(100_000, "utf-8"), False),
    "depth 100,000 str": (nested(100_000), False),
    "depth 100,000 objects": (b'{"a":' * 100_000 + b"1" + b"}" * 100_000, False),
    "deep but unbalanced": (b"[" * 100_000, False),
    "brackets inside strings": (('{"v": "' + "[" * 2000 + '"}').encode(), True),
}


@pytest.mark.parametrize(
    "document, accepted_by_orjson", CASES.values(), ids=list(CASES)
)
def test_every_difference_ends_the_same(document, accepted_by_orjson):
    assert orjson_accepts(document) is accepted_by_orjson
    old, new = both_parsers(document)

    assert new == old


@pytest.mark.parametrize(
    "document, expected",
    [
        (b'{"v": NaN}', ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE)),
        (b'{"v": 1e9999}', ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE)),
        (
            b'{"v": ' + b"9" * 5000 + b"}",
            ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE),
        ),
        (b'{"a": "\xff"}', ("InvalidCatalogSource", INVALID_JSON_ENCODING_MESSAGE)),
        (b'{"v": 18446744073709551616}', ("value", "{'v': 18446744073709551616}")),
        (b'{"a": 1, "b": 2, "a": 3}', ("value", "{'a': 3, 'b': 2}")),
        (codecs.BOM_UTF8 + b'{"a": 1}', ("value", "{'a': 1}")),
        (
            '﻿{"a": 1}',
            (
                "JSONDecodeError",
                "Unexpected UTF-8 BOM (decode using utf-8-sig)",
                1,
                1,
                0,
            ),
        ),
        (b'{"a": "\\ud800"}', ("value", "{'a': '\\ud800'}")),
        (
            b'{"a": 1,}',
            ("JSONDecodeError", "Illegal trailing comma before end of object", 1, 8, 7),
        ),
        (
            nested(100_000, "utf-8"),
            ("CatalogTooDeeplyNested", fetch.NESTING_TOO_DEEP_MESSAGE),
        ),
    ],
    ids=[
        "NaN",
        "infinite float",
        "5000 digits",
        "invalid UTF-8",
        "64-bit overflow stays an int",
        "duplicate keys",
        "BOM bytes",
        "BOM str",
        "lone surrogate",
        "trailing comma",
        "100,000 deep",
    ],
)
def test_the_outcomes_are_the_old_ones(document, expected):
    """Pinned rather than only compared, so both paths can't drift together."""
    with fast_path(True):
        assert parse_outcome(document) == expected


@pytest.mark.parametrize("depth", [101, 500, 1024])
@pytest.mark.parametrize("encoding", [None, "utf-8"])
def test_nesting_between_101_and_1024_is_rejected_after_parsing(depth, encoding):
    """The one change: orjson parses these (without recursing) and the
    post-parse walk rejects them, with the same error the old pre-parse
    scan raised."""
    document = nested(depth, encoding)
    with fast_path(False):
        with pytest.raises(CatalogTooDeeplyNested):
            fetch.parse_json_document(document)
    with fast_path(True):
        parsed = fetch.parse_json_document(document)
        with pytest.raises(CatalogTooDeeplyNested):
            validate_records(parsed, "dcatus1.1: federal dataset")


def test_a_100000_deep_document_never_recurses(monkeypatch):
    """orjson stops at 1024 levels; the stdlib path's character scan then
    rejects it before json.loads. Nothing recurses 100,000 deep."""
    monkeypatch.setattr("sys.getrecursionlimit", lambda: 1000)
    with (
        fast_path(True),
        mock.patch("json.loads", side_effect=AssertionError("json.loads used")),
    ):
        with pytest.raises(CatalogTooDeeplyNested):
            fetch.parse_json_document(nested(100_000, "utf-8"))


def test_orjson_parses_what_it_accepts_without_the_character_scan():
    with (
        fast_path(True),
        mock.patch.object(
            fetch, "_check_json_text_nesting", side_effect=AssertionError("scan")
        ),
    ):
        assert fetch.parse_json_document(b'{"dataset": [{"title": "x"}]}') == {
            "dataset": [{"title": "x"}]
        }


@pytest.mark.parametrize(
    "value, beyond",
    [
        (1.0, False),
        (9223372036854774784.0, False),
        (-9223372036854774784.0, False),
        (9223372036854775808.0, True),
        (-9223372036854775808.0, True),
        (1e300, True),
        ({"a": [{"b": 1e19}]}, True),
        ([[[[-1e19]]]], True),
        (2**80, False),  # an int, which orjson never produces this large
        ({"a": "1e300"}, False),
        ([], False),
    ],
)
def test_floats_beyond_exact_integers(value, beyond):
    assert fetch._has_float_beyond_exact_integers(value) is beyond


# --- random documents (Hypothesis) -------------------------------------------

JSON_VALUES = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.integers(min_value=2**62, max_value=2**66)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children, max_size=4)
    | st.dictionaries(st.text(max_size=6), children, max_size=4),
    max_leaves=12,
)


def _serialize(value, ensure_ascii, indent):
    return json.dumps(value, ensure_ascii=ensure_ascii, indent=indent)


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES,
    st.booleans(),
    st.sampled_from([None, 2]),
    st.sampled_from(["str", "utf-8", "utf-8-sig", "utf-16", "utf-32"]),
)
def test_random_documents_parse_the_same(value, ensure_ascii, indent, encoding):
    text = _serialize(value, ensure_ascii, indent)
    document = text if encoding == "str" else text.encode(encoding, "surrogatepass")
    old, new = both_parsers(document)

    assert new == old


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES,
    st.lists(
        st.tuples(
            st.sampled_from(["insert", "delete", "replace"]),
            st.integers(min_value=0),
            st.binary(min_size=1, max_size=3)
            | st.sampled_from(
                [
                    b"[",
                    b"]",
                    b"{",
                    b"}",
                    b'"',
                    b"\\",
                    b",",
                    b":",
                    b"\xff",
                    b"\x00",
                    b"NaN",
                    b"1e999",
                    b"\xef\xbb\xbf",
                ]
            ),
        ),
        min_size=1,
        max_size=4,
    ),
)
def test_corrupted_documents_fail_the_same(value, edits):
    """Random byte damage to a valid document: the same value, or the same
    error at the same line and column."""
    document = bytearray(
        _serialize(value, False, None).encode("utf-8", "surrogatepass")
    )
    for action, position, data in edits:
        position %= len(document) + 1
        if action == "insert":
            document[position:position] = data
        elif action == "delete":
            del document[position : position + len(data)]
        else:
            document[position : position + len(data)] = data
    old, new = both_parsers(bytes(document))

    assert new == old


@HYPOTHESIS_SETTINGS
@given(st.text(max_size=60) | st.binary(max_size=60))
def test_arbitrary_input_fails_the_same(document):
    old, new = both_parsers(document)

    assert new == old


def pasted_outcome(text):
    """parse_outcome for the validator's paste path, which parses the UTF-8
    encoding of the pasted text (parse_pasted_json_document)."""
    try:
        return ("value", repr(fetch.parse_pasted_json_document(text.encode("utf-8"))))
    except json.JSONDecodeError as error:
        return ("JSONDecodeError", error.msg, error.lineno, error.colno, error.pos)
    except fetch.InvalidCatalogSource as error:
        return ("InvalidCatalogSource", error.public_message)
    except Exception as error:
        return (type(error).__name__, str(error))


PASTED_TEXTS = {
    "BOM": '﻿{"dataset": []}',
    "leading NUL (bytes would guess UTF-16)": '\x00{"dataset": []}',
    "NUL pairs (bytes would guess UTF-32)": '\x00\x00\x00{"dataset": []}',
    "integer beyond 64 bits": '{"dataset": [{"title": 18446744073709551616}]}',
    "NaN": '{"dataset": [NaN]}',
    "syntax error on line 3": '{\n  "dataset": [\n    {"title": "x",}\n  ]\n}',
    "non-ASCII": '{"dataset": [{"title": "Données ’ \U0001f600"}]}',
    "nested 101 deep": "[" * 101 + "]" * 101,
    "nested 1025 deep": "[" * 1025 + "]" * 1025,
    "empty": "",
    "escaped lone surrogate": '{"dataset": [{"title": "\\ud800"}]}',
}


@pytest.mark.parametrize("enabled", [True, False], ids=["fast path", "pure Python"])
@pytest.mark.parametrize("text", PASTED_TEXTS.values(), ids=list(PASTED_TEXTS))
def test_pasted_bytes_parse_like_the_pasted_text(text, enabled):
    """The paste route parses the text's UTF-8 bytes so it can drop the text
    first (GSA/data.gov#6460). That must end exactly as parsing the text did,
    including the refusals bytes would otherwise change: a leading BOM, and
    NULs that bytes parsing reads as UTF-16 or UTF-32."""
    with fast_path(enabled):
        assert pasted_outcome(text) == parse_outcome(text)


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES.map(lambda value: _serialize(value, False, None))
    | st.text(max_size=60),
    st.booleans(),
)
def test_random_pasted_texts_parse_like_the_text(text, enabled):
    with fast_path(enabled):
        assert pasted_outcome(text) == parse_outcome(text)
