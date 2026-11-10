import gc
import itertools
import json
import threading
import tracemalloc
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from unittest.mock import Mock
from urllib.parse import urlencode

import pytest
import requests

from dcatus_validation import fetch
from dcatus_validation.errors import NESTING_TOO_DEEP_MESSAGE
from dcatus_validation.fetch import (
    BLOCKED_VALIDATOR_DOMAIN_MESSAGE,
    FETCH_TIMEOUT_SECONDS,
    INVALID_JSON_ENCODING_MESSAGE,
    INVALID_JSON_NUMBER_MESSAGE,
    PAYLOAD_TOO_LARGE_MESSAGE,
    UNEXPECTED_FETCH_ERROR_MESSAGE,
)
from dcatus_validation.limits import (
    MAX_REQUEST_BYTES,
    MAX_REQUEST_OVERHEAD_BYTES,
    MAX_RESULT_IDENTIFIER_CHARS,
    MAX_RESULT_MESSAGE_CHARS,
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_MB,
    MAX_VALIDATION_ERRORS,
)
from tests.unit.dcatus_validation.test_equivalence import use_fast_path as fast_path
from tests.utils.large_catalog import (
    FEDERAL,
    INVALID_ACCESS_LEVEL,
    write_catalog,
)
from validator_api import create_app

URL = "/api/v1/validate"


def _json_response(body: str):
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.raise_for_status = Mock()
    response.close = Mock()
    response.iter_content = Mock(return_value=[body.encode()])
    return response


def _deeply_nested_catalog(depth=200):
    """3.0: Catalog.catalog is `items: {"$ref": "#"}`. 150 validates, 200 does not."""
    catalog = {"@type": "Catalog", "title": "t", "description": "d", "dataset": []}
    for _ in range(depth):
        catalog = {
            "@type": "Catalog",
            "title": "t",
            "description": "d",
            "dataset": [],
            "catalog": [catalog],
        }
    return json.dumps(catalog)


class TestValidate:
    def test_paste(self, validator_client, dcatus_bad_license_uri_json):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: non-federal dataset",
                "fetch_method": "paste",
                "json_text": dcatus_bad_license_uri_json,
            },
        )

        # ruff: noqa: E501
        assert res.status_code == 200
        assert res.get_json() == {
            "validation_errors": [
                [
                    "https://www.arcgis.com/home/item.html?id=99731bb0369848169d98f31ce83fb0e2",
                    "$.license, 'center' does not match any of the acceptable formats: 'uri', 'null', '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",
                ]
            ]
        }

    def test_url(self, validator_client, monkeypatch, dcatus_no_identifier_json):
        get = Mock(return_value=_json_response(dcatus_no_identifier_json))
        monkeypatch.setattr("dcatus_validation.fetch._http_get", get)

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: non-federal dataset",
                "fetch_method": "url",
                "url": "https://example.com/data.json",
            },
        )

        assert res.status_code == 200
        # dataset position, not a string, when the dataset has no identifier
        assert res.get_json() == {
            "validation_errors": [[0, "$, 'identifier' is a required property"]]
        }
        assert get.call_args.args[0] == "https://example.com/data.json"

    def test_url_with_many_errors(
        self, validator_client, monkeypatch, dcatus_multiple_invalid_json
    ):
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(return_value=_json_response(dcatus_multiple_invalid_json)),
        )

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "url",
                "url": "https://example.com/data.json",
            },
        )

        assert res.status_code == 200
        assert len(res.get_json()["validation_errors"]) == 5

    def test_dcatus3_catalog(
        self, validator_client, dcatus_3_catalog_missing_identifier
    ):
        """The harvester's API only accepted 1.1; its validator page took 3.0 too."""
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus3.0 catalog",
                "fetch_method": "paste",
                "json_text": dcatus_3_catalog_missing_identifier,
            },
        )

        assert res.status_code == 200
        assert res.get_json() == {
            "validation_errors": [
                ["", "$.dataset[0], 'identifier' is a required property"]
            ]
        }

    def test_valid_catalog_has_no_errors(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": json.dumps({"dataset": []}),
            },
        )

        assert res.status_code == 200
        assert res.get_json() == {"validation_errors": []}

    def test_unversioned_path_redirects_to_latest(self, validator_client):
        res = validator_client.post("/api/validate", json={})

        assert res.status_code == 308
        assert res.location == URL

    @pytest.mark.parametrize(
        ("path", "expected_location"),
        [
            ("/api/%5C%5Cevil.example", "/api/v1///evil.example"),
            ("/api/http:%2F%2Fevil.example", "/api/v1/http://evil.example"),
        ],
    )
    def test_unversioned_redirect_cannot_become_external(
        self, validator_client, path, expected_location
    ):
        res = validator_client.get(path)

        assert res.status_code == 308
        assert res.location == expected_location

    def test_unversioned_redirect_rejects_protocol_relative_path(
        self, validator_client
    ):
        res = validator_client.get("/api//evil.example")

        assert res.status_code == 404
        assert "Location" not in res.headers

    def test_responses_are_not_cached_and_name_the_app(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": json.dumps({"dataset": []}),
            },
        )

        assert res.headers["Cache-Control"] == "private, no-store, max-age=0"
        assert res.headers["X-Served-By"] == "datagov-harvest-validator"


class TestRequestValidation:
    def test_bad_url(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: non-federal dataset",
                "fetch_method": "url",
                "url": "nonsense",
            },
        )

        assert res.status_code == 422
        assert res.get_json() == {
            "detail": {"json": {"url": ["Not a valid URL."]}},
            "message": "Validation error",
        }

    def test_unknown_schema(self, validator_client):
        res = validator_client.post(
            URL,
            json={"schema": "dcatus2", "fetch_method": "paste", "json_text": "{}"},
        )

        assert res.status_code == 422
        assert "schema" in res.get_json()["detail"]["json"]

    def test_missing_url(self, validator_client):
        res = validator_client.post(
            URL, json={"schema": "dcatus1.1: federal dataset", "fetch_method": "url"}
        )

        assert res.status_code == 422

    def test_missing_json_text(self, validator_client):
        res = validator_client.post(
            URL,
            json={"schema": "dcatus1.1: federal dataset", "fetch_method": "paste"},
        )

        assert res.status_code == 422

    def test_unparseable_json_is_reported_by_position(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": '{"dataset": [}',
            },
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": "Invalid JSON at line 1, column 14."}

    def test_nonfinite_json_number_is_refused(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": '{"dataset": [], "value": NaN}',
            },
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": INVALID_JSON_NUMBER_MESSAGE}

    def test_malformed_catalog_is_a_validation_result(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": "{}",
            },
        )

        assert res.status_code == 200
        assert res.get_json() == {
            "validation_errors": [["", "$, 'dataset' is a required property"]]
        }

    def test_url_credentials_are_refused(self, validator_client):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "url",
                "url": "https://user:secret@example.com/catalog.json",
            },
        )

        assert res.status_code == 400
        assert res.get_json() == {
            "error": "URLs containing credentials are not allowed."
        }


# Small stand-ins for the 150MB document / 225MB request limits, so the
# boundary tests don't build bodies that size. tests/unit/dcatus_validation/test_fetch.py pins the real
# values and how they relate.
SMALL_UPLOAD_BYTES = 64 * 1024
SMALL_REQUEST_BYTES = SMALL_UPLOAD_BYTES * 3 // 2 + MAX_REQUEST_OVERHEAD_BYTES


@pytest.fixture
def small_limits(monkeypatch, validator_app):
    """Scale the document and request limits down together, the way
    validator_api applies them: the route's document check and Flask's
    MAX_CONTENT_LENGTH."""
    monkeypatch.setattr("validator_api.api.MAX_UPLOAD_BYTES", SMALL_UPLOAD_BYTES)
    monkeypatch.setitem(validator_app.config, "MAX_CONTENT_LENGTH", SMALL_REQUEST_BYTES)


def _paste_body(document: str) -> str:
    return json.dumps(
        {
            "schema": "dcatus1.1: federal dataset",
            "fetch_method": "paste",
            "json_text": document,
        }
    )


def _document_of_size(size: int, padding_char: str = "x") -> str:
    """A valid, empty DCAT-US 1.1 catalog of exactly `size` UTF-8 bytes."""
    prefix = '{"dataset":[],"padding":"'
    suffix = '"}'
    room = size - len(prefix) - len(suffix)
    pad = padding_char * (room // len(padding_char))
    document = prefix + pad + "x" * (room - len(pad)) + suffix
    assert len(document.encode("utf-8")) == size
    return document


class TestLimits:
    def test_app_uses_the_150mb_limits(self, validator_app):
        """The boundary tests below patch both limits down; this pins what
        they stand in for."""
        # Imported here, not at module level, like create_app does: a test in
        # test_fetch.py reloads dcatus_validation.fetch, and an early import
        # would keep the old InvalidCatalogSource class.
        import validator_api.api

        assert MAX_UPLOAD_MB == 150
        assert validator_api.api.MAX_UPLOAD_BYTES == MAX_UPLOAD_BYTES
        assert validator_app.config["MAX_CONTENT_LENGTH"] == MAX_REQUEST_BYTES

    def test_request_body_over_the_limit_returns_json(
        self, validator_client, small_limits
    ):
        res = validator_client.post(
            URL,
            data=b'{"json_text":"' + b"x" * (SMALL_REQUEST_BYTES + 1024) + b'"}',
            content_type="application/json",
        )

        assert res.status_code == 413
        assert res.get_json() == {
            "error": "Submission too large - must be 150MB or less."
        }

    @pytest.mark.parametrize(
        "size_offset, accepted",
        [(-1, True), (0, True), (1, False)],
        ids=["just-under", "at", "just-over"],
    )
    def test_pasted_document_size_boundary(
        self, validator_client, small_limits, size_offset, accepted
    ):
        """The document limit is inclusive and counted in UTF-8 bytes."""
        document = _document_of_size(SMALL_UPLOAD_BYTES + size_offset)

        res = validator_client.post(
            URL, data=_paste_body(document), content_type="application/json"
        )

        if accepted:
            assert res.status_code == 200
            assert res.get_json() == {"validation_errors": []}
        else:
            assert res.status_code == 400
            assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}

    def test_pasted_document_at_the_limit_fits_despite_escaping(
        self, validator_client, small_limits
    ):
        """
        JSON-encoding a document makes it bigger (1.01-1.23x for real
        catalogs). One at the limit growing 1.4x still fits, with room for the
        surrounding API fields.
        """
        # every 5 characters, 2 backslashes: 5 bytes become 7 when escaped
        document = _document_of_size(SMALL_UPLOAD_BYTES, padding_char="\\\\xxx")
        body = _paste_body(document)
        assert 1.39 * SMALL_UPLOAD_BYTES < len(body) < SMALL_REQUEST_BYTES

        res = validator_client.post(URL, data=body, content_type="application/json")

        assert res.status_code == 200
        assert res.get_json() == {"validation_errors": []}

    def test_escape_heavy_document_at_the_limit_gets_a_json_413(
        self, validator_client, small_limits
    ):
        """
        A document made of backslashes doubles when JSON-encoded, past the
        1.5x request limit. The validator, not nginx, refuses it, so the caller
        still gets the {"error": ...} JSON answer.
        """
        document = _document_of_size(SMALL_UPLOAD_BYTES, padding_char="\\\\")
        body = _paste_body(document)
        assert len(body) > SMALL_REQUEST_BYTES

        res = validator_client.post(URL, data=body, content_type="application/json")

        assert res.status_code == 413
        assert res.get_json() == {
            "error": "Submission too large - must be 150MB or less."
        }

    def test_multibyte_document_over_the_limit_in_bytes_is_refused(
        self, validator_client, small_limits
    ):
        """At the limit in characters but over it in UTF-8 bytes is over."""
        document = _document_of_size(SMALL_UPLOAD_BYTES).replace("x", "é", 1)
        assert len(document) == SMALL_UPLOAD_BYTES < len(document.encode("utf-8"))

        res = validator_client.post(
            URL, data=_paste_body(document), content_type="application/json"
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}

    def test_deeply_nested_catalog_returns_422_naming_the_reason(
        self, validator_client
    ):
        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus3.0 catalog",
                "fetch_method": "paste",
                "json_text": _deeply_nested_catalog(),
            },
        )

        assert res.status_code == 422
        assert res.get_json() == {"error": NESTING_TOO_DEEP_MESSAGE}

    def test_json_nested_too_deeply_to_parse_returns_422(self, validator_client):
        depth = 2000
        document = "[" * depth + "0" + "]" * depth

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": document,
            },
        )

        assert res.status_code == 422
        assert res.get_json() == {"error": NESTING_TOO_DEEP_MESSAGE}

    def test_validation_error_amplification_is_capped(self, validator_client):
        catalog = {"dataset": [{} for _ in range(101)]}

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": json.dumps(catalog),
            },
        )

        assert res.status_code == 200
        assert len(res.get_json()["validation_errors"]) == MAX_VALIDATION_ERRORS
        assert res.get_json()["validation_incomplete"] is True

    def test_dcatus3_validation_error_amplification_is_capped(self, validator_client):
        catalog = {"dataset": [{} for _ in range(251)]}

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus3.0 catalog",
                "fetch_method": "paste",
                "json_text": json.dumps(catalog),
            },
        )

        assert res.status_code == 200
        assert len(res.get_json()["validation_errors"]) == MAX_VALIDATION_ERRORS
        assert res.get_json()["validation_incomplete"] is True

    def test_result_values_are_bounded(self, validator_client, monkeypatch):
        monkeypatch.setattr(
            "validator_api.api.validate_records_limited",
            Mock(
                return_value=(
                    [
                        (
                            "i" * (MAX_RESULT_IDENTIFIER_CHARS + 1),
                            "m" * (MAX_RESULT_MESSAGE_CHARS + 1),
                        )
                    ],
                    False,
                )
            ),
        )

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": '{"dataset": []}',
            },
        )

        identifier, message = res.get_json()["validation_errors"][0]
        assert identifier.endswith("... [truncated]")
        assert message.endswith("... [truncated]")
        assert len(identifier) == MAX_RESULT_IDENTIFIER_CHARS
        assert len(message) == MAX_RESULT_MESSAGE_CHARS


class TestRefusedUrlIsExplained:
    """
    A URL we decline to fetch has to say why: 400 (bad submission, not a server
    fault) with the reason, rather than an opaque 500. GSA/data.gov#6293.
    """

    def _url_form(self, url):
        return {
            "schema": "dcatus1.1: federal dataset",
            "fetch_method": "url",
            "url": url,
        }

    def test_blocked_domain(self, validator_client, monkeypatch):
        resolve = Mock()
        get = Mock()
        monkeypatch.setattr("dcatus_validation.fetch._resolve_addresses", resolve)
        monkeypatch.setattr("dcatus_validation.fetch._http_get", get)

        res = validator_client.post(
            URL, json=self._url_form("https://sub.rbndr.us/catalog.json")
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": BLOCKED_VALIDATOR_DOMAIN_MESSAGE}
        resolve.assert_not_called()
        get.assert_not_called()

    def test_timeout(self, validator_client, monkeypatch):
        clock = itertools.chain([100.0, 100.0], itertools.repeat(200.0))
        monkeypatch.setattr(
            "dcatus_validation.fetch.time.monotonic", lambda: next(clock)
        )
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(side_effect=requests.exceptions.Timeout("timed out")),
        )

        res = validator_client.post(
            URL, json=self._url_form("https://example.com/slow.json")
        )

        assert res.status_code == 400
        assert f"longer than {FETCH_TIMEOUT_SECONDS} seconds" in res.get_json()["error"]

    def test_connect_timeout(self, validator_client, monkeypatch):
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(side_effect=requests.exceptions.ConnectTimeout("syn lost")),
        )

        res = validator_client.post(
            URL, json=self._url_form("https://example.com/flaky.json")
        )

        assert res.status_code == 400
        assert "Could not connect to the URL's server" in res.get_json()["error"]

    def test_private_address(self, validator_client, monkeypatch):
        monkeypatch.setattr("dcatus_validation.fetch.ALLOW_PRIVATE_ADDRESSES", False)

        res = validator_client.post(
            URL, json=self._url_form("http://127.0.0.1/secret.json")
        )

        assert res.status_code == 400
        assert "private/internal addresses is not allowed" in res.get_json()["error"]

    def test_unexpected_transport_error_is_not_disclosed(
        self, validator_client, monkeypatch
    ):
        """Still a 400 the submitter can act on, but requests' own text - which
        can name internal paths - must not reach the response body."""
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(
                side_effect=requests.exceptions.SSLError(
                    "verify failed: /internal/path/ca-bundle.crt"
                )
            ),
        )

        res = validator_client.post(
            URL, json=self._url_form("https://example.com/data.json")
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": UNEXPECTED_FETCH_ERROR_MESSAGE}

    def test_unexpected_server_error_is_not_disclosed(
        self, validator_client, monkeypatch
    ):
        log_error = Mock()
        monkeypatch.setattr("validator_api.api.logger.error", log_error)
        monkeypatch.setattr(
            "validator_api.api.validate_records_limited",
            Mock(side_effect=KeyError("/internal/detail")),
        )

        res = validator_client.post(
            URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "paste",
                "json_text": "{}",
            },
        )

        assert res.status_code == 500
        assert res.get_json() == {
            "error": "API Validator error: failed to validate dcatus catalog"
        }
        assert log_error.call_args.args == (
            "API Validator error error_type=%s",
            "KeyError",
        )


class TestService:
    # /health for Cloud Foundry, /validator/health through the proxy.
    @pytest.mark.parametrize("path", ["/health", "/validator/health"])
    def test_health(self, validator_client, path):
        res = validator_client.get(path)

        assert res.status_code == 200
        assert res.get_json() == {"status": "ok"}

    def test_openapi_spec_documents_validate(self, validator_client):
        spec = validator_client.get("/validator/openapi.json").get_json()

        assert set(spec["paths"]) == {URL, f"{URL}/document"}
        assert "post" in spec["paths"][URL]
        assert "post" in spec["paths"][f"{URL}/document"]
        # no auth anywhere in this service
        assert "securitySchemes" not in spec.get("components", {})

    def test_docs(self, validator_client):
        response = validator_client.get(
            "/validator/docs", headers={"X-Forwarded-Proto": "https"}
        )

        assert response.status_code == 200
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["X-Frame-Options"] == "SAMEORIGIN"
        assert "max-age=31536000" in response.headers["Strict-Transport-Security"]
        csp = response.headers["Content-Security-Policy"]
        assert "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net" in csp
        assert "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net" in csp
        assert "img-src 'self' data: https://apiflask.com" in csp
        assert validator_client.get("/").location == "/validator/docs"

    def test_docs_csp_allows_configured_server(self, monkeypatch):
        monkeypatch.setenv("EXTERNAL_ROUTE", "https://validator.example.gov")
        app = create_app()

        response = app.test_client().get("/validator/docs")

        assert (
            "connect-src 'self' https://validator.example.gov"
            in response.headers["Content-Security-Policy"]
        )


# The validator API answers exactly the same on the fast path as on the old
# pure-Python path, through both the paste and the URL fetch paths: status
# codes, errors, `validation_incomplete` and refusals (GSA/data.gov#6460).


EXAMPLES = Path(__file__).resolve().parents[3] / "example_data" / "dcatus"
NON_FEDERAL = "dcatus1.1: non-federal dataset"
DCATUS3 = "dcatus3.0 catalog"


def _response(body: bytes):
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.raise_for_status = Mock()
    response.close = Mock()
    response.iter_content = Mock(return_value=[body])
    return response


def _submit(client, monkeypatch, body: bytes | str, schema, fetch_method, fast):
    """POST a document by paste or by URL on the fast path or the old one;
    returns (status, JSON body)."""
    if fetch_method == "paste":
        text = body if isinstance(body, str) else body.decode("utf-8", "surrogatepass")
        payload = {"schema": schema, "fetch_method": "paste", "json_text": text}
    else:
        raw = body if isinstance(body, bytes) else body.encode("utf-8", "surrogatepass")
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(side_effect=lambda *args, **kwargs: _response(raw)),
        )
        payload = {
            "schema": schema,
            "fetch_method": "url",
            "url": "https://example.com/data.json",
        }
    with fast_path(fast):
        res = client.post(URL, json=payload)
    return res.status_code, res.get_json()


def _both(client, monkeypatch, body, schema, fetch_method):
    old = _submit(client, monkeypatch, body, schema, fetch_method, fast=False)
    new = _submit(client, monkeypatch, body, schema, fetch_method, fast=True)
    return old, new


def _golden():
    cases = []
    for path in sorted(EXAMPLES.glob("*.json")):
        schemas = (DCATUS3,) if "3_0" in path.name else (FEDERAL, NON_FEDERAL)
        for schema in schemas:
            cases.append(pytest.param(path, schema, id=f"{path.name}-{schema}"))
    return cases


@pytest.mark.parametrize("fetch_method", ["paste", "url"])
@pytest.mark.parametrize("path, schema", _golden())
def test_golden_catalogs(validator_client, monkeypatch, path, schema, fetch_method):
    old, new = _both(
        validator_client, monkeypatch, path.read_bytes(), schema, fetch_method
    )

    assert new == old
    assert old[0] == 200


@pytest.mark.parametrize("fetch_method", ["paste", "url"])
@pytest.mark.parametrize("depth", [100, 101, 1024, 1025, 100_000])
@pytest.mark.parametrize("schema", [FEDERAL, DCATUS3])
def test_nesting(validator_client, monkeypatch, depth, schema, fetch_method):
    """Deep documents get the same clean 422 on both paths, including the
    malicious 100,000-deep one, which must not crash or recurse."""
    body = "[" * depth + "0" + "]" * depth
    old, new = _both(validator_client, monkeypatch, body, schema, fetch_method)

    assert new == old
    if depth > 100:
        assert old == (422, {"error": NESTING_TOO_DEEP_MESSAGE})
    else:
        assert old[0] == 200


REFUSED = {
    "syntax error": '{"internal_secret": "hunter2",, "b": 2}',
    "trailing comma": '{"dataset": [],}',
    "NaN": '{"dataset": [], "v": NaN}',
    "Infinity": '{"dataset": [], "v": -Infinity}',
    "infinite float": '{"dataset": [], "v": 1e9999}',
    "5000-digit integer": '{"dataset": [], "v": ' + "9" * 5000 + "}",
    "empty": "",
    "not JSON": "<html></html>",
}


@pytest.mark.parametrize("fetch_method", ["paste", "url"])
@pytest.mark.parametrize("body", REFUSED.values(), ids=list(REFUSED))
def test_refused_documents(validator_client, monkeypatch, body, fetch_method):
    old, new = _both(validator_client, monkeypatch, body, FEDERAL, fetch_method)

    assert new == old
    if fetch_method == "paste" and not body:
        assert old[0] == 422  # json_text is required
    else:
        assert old[0] == 400


def test_numbers_are_still_refused_with_the_number_message(
    validator_client, monkeypatch
):
    old, new = _both(validator_client, monkeypatch, '{"v": NaN}', FEDERAL, "paste")

    assert new == old == (400, {"error": INVALID_JSON_NUMBER_MESSAGE})


ACCEPTED_BY_URL = {
    "UTF-8 BOM": b'\xef\xbb\xbf{"dataset": [{}]}',
    "UTF-16": '{"dataset": [{}]}'.encode("utf-16"),
    "UTF-32": '{"dataset": [{}]}'.encode("utf-32"),
    "invalid UTF-8": b'{"dataset": [{"title": "\xff"}]}',
    "integer beyond 64 bits": b'{"dataset": [{"title": 18446744073709551616}]}',
    "escaped lone surrogate": b'{"dataset": [{"title": "\\ud800"}]}',
    "duplicate keys": b'{"dataset": [{"title": 1, "title": "x"}]}',
}


@pytest.mark.parametrize("body", ACCEPTED_BY_URL.values(), ids=list(ACCEPTED_BY_URL))
def test_url_documents_with_unusual_encodings_or_values(
    validator_client, monkeypatch, body
):
    old, new = _both(validator_client, monkeypatch, body, FEDERAL, "url")

    assert new == old


@pytest.mark.parametrize(
    "text",
    [
        '﻿{"dataset": []}',
        '{"dataset": [{"title": "\ud800"}]}',
        '{"dataset": [{"title": 18446744073709551616}]}',
        '{"dataset": [{"title": "Données \U0001f600"}]}',
    ],
    ids=["BOM", "lone surrogate", "integer beyond 64 bits", "non-ASCII"],
)
def test_pasted_documents_with_unusual_encodings_or_values(
    validator_client, monkeypatch, text
):
    old, new = _both(validator_client, monkeypatch, text, FEDERAL, "paste")

    assert new == old


@pytest.mark.parametrize("fetch_method", ["paste", "url"])
@pytest.mark.parametrize(
    "schema, catalog",
    [
        (FEDERAL, {"dataset": [{} for _ in range(101)]}),
        (NON_FEDERAL, {"dataset": [{} for _ in range(126)]}),
        (DCATUS3, {"dataset": [{} for _ in range(251)]}),
    ],
    ids=["1.1 federal", "1.1 non-federal", "3.0"],
)
def test_error_cap(validator_client, monkeypatch, schema, catalog, fetch_method):
    old, new = _both(
        validator_client, monkeypatch, json.dumps(catalog), schema, fetch_method
    )

    assert new == old
    assert old[1]["validation_incomplete"] is True
    assert len(old[1]["validation_errors"]) == 1000


# Real-size submissions to the validator API (GSA/data.gov#6460): catalogs
# generated at test time at, just over and well over MAX_UPLOAD_MB, sent
# through the real app with nothing patched but the private-address check, by
# paste, as the request body, and by URL fetch from a local HTTP server. Each
# test writes its catalog under tmp_path and deletes it before returning.


MIB = 1024 * 1024
SUBMISSION_TOO_LARGE_MESSAGE = (
    f"Submission too large - must be {MAX_UPLOAD_MB}MB or less."
)


@pytest.fixture(autouse=True)
def release_memory():
    """Free each test's ~2GB before the next one: the app and test client a
    test builds are only freed by the cycle collector."""
    yield
    gc.collect()


@pytest.fixture
def catalog_file(tmp_path):
    """Write a catalog of exactly `size` bytes; every one is deleted at teardown."""
    written = []

    def make(size, **kwargs):
        path = tmp_path / f"catalog-{len(written)}.json"
        written.append(path)
        write_catalog(path, size, **kwargs)
        return path

    yield make
    for path in written:
        path.unlink(missing_ok=True)


def _paste_body_of(path) -> bytes:
    """What the /validate/ page sends: JSON.stringify({schema, fetch_method,
    json_text}), with the catalog JSON-escaped inside json_text."""
    return json.dumps(
        {"schema": FEDERAL, "fetch_method": "paste", "json_text": path.read_text()}
    ).encode()


def _post(client, body: bytes):
    return client.post(URL, data=body, content_type="application/json")


class _CatalogServer(BaseHTTPRequestHandler):
    """Serves one file. `?no-length` leaves out Content-Length, so the body
    ends when the connection closes and only the streamed count can stop it."""

    path_to_serve = None

    def do_GET(self):
        size = self.path_to_serve.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        if not self.path.endswith("?no-length"):
            self.send_header("Content-Length", str(size))
        self.end_headers()
        try:
            with open(self.path_to_serve, "rb") as fp:
                while chunk := fp.read(MIB):
                    self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the validator stopped reading at the limit

    def log_message(self, *args):
        pass


@pytest.fixture
def serve():
    """Serve a file on 127.0.0.1 and return its URL."""
    servers = []

    def start(path):
        handler = type("Handler", (_CatalogServer,), {"path_to_serve": path})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}/catalog.json"

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


class TestPastedCatalog:
    def test_a_valid_catalog_at_the_limit_is_validated(
        self, validator_client, catalog_file
    ):
        path = catalog_file(MAX_UPLOAD_BYTES)
        body = _paste_body_of(path)
        path.unlink()

        res = _post(validator_client, body)

        assert res.status_code == 200
        assert res.get_json() == {"validation_errors": []}

    def test_every_record_is_validated(self, validator_client, catalog_file):
        """The one bad record, last in a catalog at the limit, is reported."""
        path = catalog_file(MAX_UPLOAD_BYTES, invalid_last="last-record")
        body = _paste_body_of(path)
        path.unlink()

        res = _post(validator_client, body)

        assert res.status_code == 200
        [[identifier, message]] = res.get_json()["validation_errors"]
        assert identifier == "last-record"
        assert INVALID_ACCESS_LEVEL in message

    @pytest.mark.parametrize("size", [151 * MIB, 200 * MIB], ids=["151MB", "200MB"])
    def test_a_catalog_over_the_limit_is_refused(
        self, validator_client, catalog_file, size
    ):
        """Its request body fits MAX_REQUEST_BYTES, so the validator itself
        refuses the document, with its JSON message."""
        path = catalog_file(size)
        body = _paste_body_of(path)
        path.unlink()
        assert len(body) < MAX_REQUEST_BYTES

        res = _post(validator_client, body)

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}

    def test_a_request_body_over_the_request_limit_gets_a_json_413(
        self, validator_client, catalog_file
    ):
        path = catalog_file(230 * MIB)
        body = _paste_body_of(path)
        path.unlink()
        assert len(body) > MAX_REQUEST_BYTES

        res = _post(validator_client, body)

        assert res.status_code == 413
        assert res.get_json() == {"error": SUBMISSION_TOO_LARGE_MESSAGE}


class TestFetchedCatalog:
    def test_a_catalog_at_the_limit_is_fetched_and_every_record_validated(
        self, validator_client, catalog_file, serve
    ):
        url = serve(catalog_file(MAX_UPLOAD_BYTES, invalid_last="last-record"))

        res = validator_client.post(
            URL, json={"schema": FEDERAL, "fetch_method": "url", "url": url}
        )

        assert res.status_code == 200
        [[identifier, message]] = res.get_json()["validation_errors"]
        assert identifier == "last-record"
        assert INVALID_ACCESS_LEVEL in message

    @pytest.mark.parametrize(
        "size, query",
        [
            (151 * MIB, ""),
            (200 * MIB, ""),
            (151 * MIB, "?no-length"),
        ],
        ids=["151MB", "200MB", "151MB-without-content-length"],
    )
    def test_a_catalog_over_the_limit_is_refused(
        self, validator_client, catalog_file, serve, size, query
    ):
        """Refused by Content-Length before the download, or by the running
        byte count when the server doesn't send one."""
        url = serve(catalog_file(size)) + query

        res = validator_client.post(
            URL, json={"schema": FEDERAL, "fetch_method": "url", "url": url}
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}


class TestCatalogAsTheBody:
    """/validate/document, how the /validate/ page sends a pasted or uploaded
    catalog: the body is the catalog itself."""

    @staticmethod
    def _post(client, path, fetch_method="upload"):
        body = path.read_bytes()
        path.unlink()
        return client.post(
            f"{URL}/document?schema={FEDERAL}&fetch_method={fetch_method}",
            data=body,
            content_type="application/json",
        )

    @pytest.mark.parametrize("fetch_method", ["paste", "upload"])
    def test_a_catalog_at_the_limit_is_validated_to_its_last_record(
        self, validator_client, catalog_file, fetch_method
    ):
        path = catalog_file(MAX_UPLOAD_BYTES, invalid_last="last-record")

        res = self._post(validator_client, path, fetch_method)

        assert res.status_code == 200
        [[identifier, message]] = res.get_json()["validation_errors"]
        assert identifier == "last-record"
        assert INVALID_ACCESS_LEVEL in message

    @pytest.mark.parametrize("size", [151 * MIB, 200 * MIB], ids=["151MB", "200MB"])
    def test_a_catalog_over_the_limit_is_refused(
        self, validator_client, catalog_file, size
    ):
        res = self._post(validator_client, catalog_file(size))

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}

    def test_a_body_over_the_request_limit_gets_a_json_413(
        self, validator_client, catalog_file
    ):
        path = catalog_file(230 * MIB)
        assert path.stat().st_size > MAX_REQUEST_BYTES

        res = self._post(validator_client, path)

        assert res.status_code == 413
        assert res.get_json() == {"error": SUBMISSION_TOO_LARGE_MESSAGE}


# /validate/document, the catalog as the raw request body (GSA/data.gov#6460).
# The /validate/ page sends pasted text and uploaded files this way, so the
# browser never JSON-escapes a catalog. Every document must get exactly what
# /validate gives it as `json_text`: a paste is the same text, and an upload is
# the text the page used to decode from the file and send as a paste.


TOO_LARGE_413 = {"error": "Submission too large - must be 150MB or less."}

TEXTS = {
    "valid 1.1": (FEDERAL, (EXAMPLES / "dcatus.json").read_text()),
    "invalid 1.1": (FEDERAL, (EXAMPLES / "dcatus_multiple_invalid.json").read_text()),
    "3.0": (DCATUS3, (EXAMPLES / "dcatus3_0_invalid.json").read_text()),
    "syntax error on line 3": (FEDERAL, '{\n  "dataset": [\n    {"title": 1,}\n]}'),
    "NaN": (FEDERAL, '{"dataset": [NaN]}'),
    "integer beyond 64 bits": (
        FEDERAL,
        '{"dataset": [{"title": 18446744073709551616}]}',
    ),
    "nested 200 deep": (FEDERAL, '{"dataset": [' + "[" * 200 + "]" * 200 + "]}"),
    "non-ASCII": (FEDERAL, '{"dataset": [{"title": "Données ’ \U0001f600"}]}'),
    "not a catalog": (FEDERAL, "[1, 2]"),
    "empty": (FEDERAL, ""),
}


def _json_paste(client, schema, text):
    res = client.post(
        "/api/v1/validate",
        json={"schema": schema, "fetch_method": "paste", "json_text": text},
    )
    return res.status_code, res.get_json()


def _document(client, schema, body: bytes, fetch_method="upload", path="/api/v1"):
    query = urlencode({"schema": schema, "fetch_method": fetch_method})
    res = client.post(
        f"{path}/validate/document?{query}",
        data=body,
        content_type="application/json",
    )
    return res.status_code, res.get_json()


@pytest.mark.parametrize("fetch_method", ["paste", "upload"])
@pytest.mark.parametrize("schema, text", TEXTS.values(), ids=list(TEXTS))
def test_the_body_gets_what_json_text_gets(
    validator_client, schema, text, fetch_method
):
    expected = _json_paste(validator_client, schema, text)
    if text == "":
        # /validate refuses an empty json_text in the request schema; the body
        # is just an empty document.
        assert expected[0] == 422
        expected = (400, {"error": "Invalid JSON at line 1, column 1."})

    assert _document(validator_client, schema, text.encode(), fetch_method) == expected


class TestPaste:
    def test_a_byte_order_mark_is_refused_as_in_json_text(self, validator_client):
        text = '﻿{"dataset": []}'
        expected = _json_paste(validator_client, FEDERAL, text)
        assert expected[0] == 400

        assert _document(validator_client, FEDERAL, text.encode(), "paste") == expected

    def test_bytes_that_arent_utf8_are_refused(self, validator_client):
        assert _document(
            validator_client, FEDERAL, b'{"dataset": ["\xff"]}', "paste"
        ) == (
            400,
            {"error": INVALID_JSON_ENCODING_MESSAGE},
        )


class TestUpload:
    """The page used to decode a file in the browser (UTF-16 by BOM or NULs,
    otherwise UTF-8, dropping one BOM, refusing what didn't decode) and send
    the text as a paste. The validator now does the same."""

    CATALOG = '{"dataset": [{"title": "Données"}]}'

    @pytest.mark.parametrize(
        "encoding", ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"]
    )
    def test_decodes_as_the_page_did(self, validator_client, encoding):
        expected = _json_paste(validator_client, FEDERAL, self.CATALOG)

        assert (
            _document(validator_client, FEDERAL, self.CATALOG.encode(encoding))
            == expected
        )

    def test_drops_only_one_byte_order_mark(self, validator_client):
        expected = _json_paste(validator_client, FEDERAL, "﻿" + self.CATALOG)
        assert expected[0] == 400

        body = ("﻿﻿" + self.CATALOG).encode("utf-8")
        assert _document(validator_client, FEDERAL, body) == expected

    @pytest.mark.parametrize(
        "body",
        [
            b'{"dataset": ["\xff"]}',
            '{"dataset": ["\ud800"]}'.encode("utf-16-le", "surrogatepass"),
        ],
        ids=["invalid UTF-8", "lone surrogate in UTF-16"],
    )
    def test_a_file_that_doesnt_decode_is_refused(self, validator_client, body):
        assert _document(validator_client, FEDERAL, body) == (
            400,
            {"error": INVALID_JSON_ENCODING_MESSAGE},
        )

    def test_upload_is_the_default(self, validator_client):
        res = validator_client.post(
            f"/api/v1/validate/document?{urlencode({'schema': FEDERAL})}",
            data=self.CATALOG.encode("utf-16"),
        )

        assert (res.status_code, res.get_json()) == _json_paste(
            validator_client, FEDERAL, self.CATALOG
        )


def _catalog_of_size(size: int) -> bytes:
    prefix, suffix = b'{"dataset":[],"padding":"', b'"}'
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


class TestDocumentLimits:
    @pytest.mark.parametrize("fetch_method", ["paste", "upload"])
    @pytest.mark.parametrize(
        "size_offset, accepted",
        [(-1, True), (0, True), (1, False)],
        ids=["just-under", "at", "just-over"],
    )
    def test_document_size_boundary(
        self, validator_client, small_limits, fetch_method, size_offset, accepted
    ):
        body = _catalog_of_size(SMALL_UPLOAD_BYTES + size_offset)

        result = _document(validator_client, FEDERAL, body, fetch_method)

        if accepted:
            assert result == (200, {"validation_errors": []})
        else:
            assert result == (400, {"error": PAYLOAD_TOO_LARGE_MESSAGE})

    def test_a_utf16_file_counts_its_utf8_size(self, validator_client, small_limits):
        """Like the page, which checked the file's size and then sent its text:
        within the limit as a UTF-16 file, over it once decoded, is over. A
        euro sign is 2 bytes in UTF-16 and 3 in UTF-8."""
        prefix, suffix = '{"dataset":[],"padding":"', '"}'
        text = prefix + "€" * (SMALL_UPLOAD_BYTES // 2 - 40) + suffix
        body = text.encode("utf-16-le")
        assert len(body) <= SMALL_UPLOAD_BYTES < len(text.encode("utf-8"))

        result = _document(validator_client, FEDERAL, body)

        assert result == (400, {"error": PAYLOAD_TOO_LARGE_MESSAGE})

    def test_a_body_over_the_request_limit_gets_a_json_413(
        self, validator_client, small_limits
    ):
        body = _catalog_of_size(SMALL_REQUEST_BYTES + 1)

        assert _document(validator_client, FEDERAL, body) == (413, TOO_LARGE_413)


class TestQuery:
    @pytest.mark.parametrize(
        "query",
        [{}, {"schema": "no such schema"}, {"schema": FEDERAL, "fetch_method": "url"}],
        ids=["no schema", "unknown schema", "url"],
    )
    def test_invalid_query_is_refused(self, validator_client, query):
        res = validator_client.post(
            f"/api/v1/validate/document?{urlencode(query)}", data=b'{"dataset": []}'
        )

        assert res.status_code == 422
        assert res.get_json()["message"] == "Validation error"

    def test_unversioned_path_redirects_with_its_query(self, validator_client):
        query = urlencode({"schema": FEDERAL, "fetch_method": "paste"})
        res = validator_client.post(
            f"/api/validate/document?{query}", data=b'{"dataset": []}'
        )

        assert res.status_code == 308
        assert res.headers["Location"] == f"/api/v1/validate/document?{query}"


# The paste route holds a pasted catalog once while it's parsed
# (GSA/data.gov#6460). Before, the decoded text, werkzeug's cached request body
# and cached JSON, and a UTF-8 copy for parsing were all alive at once: ~1.1GB for a
# 150MB paste on Linux, ~1.6GB if the text wasn't pure ASCII.  A small catalog
# is enough: tracemalloc counts Python's allocations exactly, so what's alive
# when parsing starts is the same multiple of the document's size at any size.


def _catalog_text(tmp_path) -> str:
    path = tmp_path / "catalog.json"
    write_catalog(path, 2 * 1024 * 1024)
    # Not pure ASCII, as real catalogs often aren't: CPython then stores the
    # text at 2 bytes a character, the costliest case.
    text = path.read_text().replace('"title": "', '"title": "\u2019')
    path.unlink()
    return text


def _alive_when_parsing_starts(client, url, body: bytes) -> int:
    """How much more Python memory is allocated when the route starts
    parsing than before the request: the copies of the catalog it holds."""
    alive_at_parse = []
    real_parse = fetch.parse_pasted_json_document

    def measure_then_parse(document):
        gc.collect()
        alive_at_parse.append(tracemalloc.get_traced_memory()[0] - baseline)
        return real_parse(document)

    gc.collect()
    tracemalloc.start()
    try:
        baseline = tracemalloc.get_traced_memory()[0]
        with mock.patch(
            "validator_api.api.parse_pasted_json_document",
            side_effect=measure_then_parse,
        ):
            res = client.post(url, data=body, content_type="application/json")
    finally:
        tracemalloc.stop()

    assert res.status_code == 200
    assert res.get_json() == {"validation_errors": []}
    [alive] = alive_at_parse
    return alive


def test_a_paste_is_held_once_while_it_is_parsed(validator_client, tmp_path):
    text = _catalog_text(tmp_path)
    document_size = len(text.encode("utf-8"))
    body = json.dumps(
        {"schema": FEDERAL, "fetch_method": "paste", "json_text": text}
    ).encode()
    del text

    alive = _alive_when_parsing_starts(validator_client, URL, body)

    # The document's UTF-8 bytes, plus a little: not the text (here 2 bytes a
    # character) and the request body again on top.
    assert alive < 1.3 * document_size, (alive, document_size)


@pytest.mark.parametrize("fetch_method", ["paste", "upload"])
def test_a_document_body_is_held_once_while_it_is_parsed(
    validator_client, tmp_path, fetch_method
):
    """/validate/document, how the page sends catalogs: the body it reads,
    once, is the document it parses."""
    body = _catalog_text(tmp_path).encode("utf-8")
    query = urlencode({"schema": FEDERAL, "fetch_method": fetch_method})

    alive = _alive_when_parsing_starts(
        validator_client, f"/api/v1/validate/document?{query}", body
    )

    assert alive < 1.3 * len(body), (alive, len(body))


# One document over LARGE_DOCUMENT_BYTES at a time per instance
# (GSA/data.gov#6460): two at once could exceed validator_memory_quota.

LARGE = 1024  # stands in for LARGE_DOCUMENT_BYTES
BUSY = {
    "error": "The validator is busy with another large catalog. "
    "Please try again in a minute."
}


@pytest.fixture
def large_document_lock(monkeypatch, tmp_path):
    monkeypatch.setattr("validator_api.api.LARGE_DOCUMENT_BYTES", LARGE)
    path = tmp_path / "large-document.lock"
    monkeypatch.setattr("validator_api.api.LARGE_DOCUMENT_LOCK_PATH", str(path))
    return path


class TestOneLargeDocumentAtATime:
    def test_a_second_large_document_is_turned_away_while_one_runs(
        self, validator_app, large_document_lock, monkeypatch
    ):
        import validator_api.api as api_module

        started, release, calls = threading.Event(), threading.Event(), []
        validate = api_module.validate_records_limited

        def first_call_waits(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                started.set()
                release.wait(10)
            return validate(*args, **kwargs)

        monkeypatch.setattr(
            "validator_api.api.validate_records_limited", first_call_waits
        )
        large = _paste_body(_document_of_size(LARGE + 1))
        small = _paste_body(_document_of_size(LARGE))
        first = {}

        def submit_first():
            res = validator_app.test_client().post(
                URL, data=large, content_type="application/json"
            )
            first["result"] = (res.status_code, res.get_json())

        thread = threading.Thread(target=submit_first)
        thread.start()
        assert started.wait(10)
        client = validator_app.test_client()
        busy = client.post(URL, data=large, content_type="application/json")
        allowed = client.post(URL, data=small, content_type="application/json")
        release.set()
        thread.join(10)
        after = client.post(URL, data=large, content_type="application/json")

        assert (busy.status_code, busy.get_json()) == (503, BUSY)
        assert busy.headers["Retry-After"] == "60"
        assert (allowed.status_code, allowed.get_json()) == (
            200,
            {"validation_errors": []},
        )
        assert first["result"] == (200, {"validation_errors": []})
        assert after.status_code == 200

    def test_every_way_in_takes_the_slot(
        self, validator_client, large_document_lock, monkeypatch
    ):
        """With the slot held (here by another open file, as another gunicorn
        worker would hold it), a large paste, body or URL fetch gets the 503;
        a small one doesn't."""
        import fcntl

        document = _document_of_size(LARGE + 1)
        monkeypatch.setattr(
            "dcatus_validation.fetch._http_get",
            Mock(side_effect=lambda *a, **k: _json_response(document)),
        )
        query = urlencode({"schema": FEDERAL, "fetch_method": "paste"})
        url_body = {"schema": FEDERAL, "fetch_method": "url"}
        url_body["url"] = "https://example.com/data.json"

        with open(large_document_lock, "a") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            results = [
                validator_client.post(
                    URL, data=_paste_body(document), content_type="application/json"
                ),
                validator_client.post(f"{URL}/document?{query}", data=document),
                validator_client.post(
                    f"{URL}/document?schema={FEDERAL}", data=document.encode()
                ),
                validator_client.post(URL, json=url_body),
            ]
            small = validator_client.post(
                URL,
                data=_paste_body(_document_of_size(LARGE)),
                content_type="application/json",
            )

        assert [(r.status_code, r.get_json()) for r in results] == [(503, BUSY)] * 4
        assert small.status_code == 200
