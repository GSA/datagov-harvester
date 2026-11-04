"""/validate/document, the catalog as the raw request body (GSA/data.gov#6460).

The /validate/ page sends pasted text and uploaded files this way, so the
browser never JSON-escapes a catalog. Every document must get exactly what
/validate gives it as `json_text`: a paste is the same text, and an upload is
the text the page used to decode from the file and send as a paste.
"""

from pathlib import Path
from urllib.parse import urlencode

import pytest

from dcatus_validation.fetch import (
    INVALID_JSON_ENCODING_MESSAGE,
    PAYLOAD_TOO_LARGE_MESSAGE,
)
from dcatus_validation.limits import MAX_REQUEST_OVERHEAD_BYTES

FEDERAL = "dcatus1.1: federal dataset"
DCATUS3 = "dcatus3.0 catalog"
EXAMPLES = Path(__file__).resolve().parents[3] / "example_data" / "dcatus"
SMALL_UPLOAD_BYTES = 64 * 1024
SMALL_REQUEST_BYTES = SMALL_UPLOAD_BYTES * 3 // 2 + MAX_REQUEST_OVERHEAD_BYTES
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


@pytest.fixture(params=["true", "false"], ids=["fast path", "kill switch"])
def fast_path_setting(request, monkeypatch):
    monkeypatch.setenv("DCATUS_VALIDATION_FAST_PATH", request.param)


@pytest.mark.parametrize("fetch_method", ["paste", "upload"])
@pytest.mark.parametrize("schema, text", TEXTS.values(), ids=list(TEXTS))
def test_the_body_gets_what_json_text_gets(
    validator_client, fast_path_setting, schema, text, fetch_method
):
    expected = _json_paste(validator_client, schema, text)
    if text == "":
        # /validate refuses an empty json_text in the request schema; the body
        # is just an empty document.
        assert expected[0] == 422
        expected = (400, {"error": "Invalid JSON at line 1, column 1."})

    assert _document(validator_client, schema, text.encode(), fetch_method) == expected


class TestPaste:
    def test_a_byte_order_mark_is_refused_as_in_json_text(
        self, validator_client, fast_path_setting
    ):
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
    def test_decodes_as_the_page_did(
        self, validator_client, fast_path_setting, encoding
    ):
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


@pytest.fixture
def small_limits(monkeypatch, validator_app):
    """Scale the document and request limits down together, as in
    test_validate_api.py."""
    monkeypatch.setattr("validator_api.api.MAX_UPLOAD_BYTES", SMALL_UPLOAD_BYTES)
    monkeypatch.setitem(validator_app.config, "MAX_CONTENT_LENGTH", SMALL_REQUEST_BYTES)


def _catalog_of_size(size: int) -> bytes:
    prefix, suffix = b'{"dataset":[],"padding":"', b'"}'
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


class TestLimits:
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
