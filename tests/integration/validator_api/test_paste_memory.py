"""The paste route holds a pasted catalog once while it's parsed
(GSA/data.gov#6460). Before, the decoded text, werkzeug's cached request body
and cached JSON, and orjson's UTF-8 copy were all alive at once: ~1.1GB for a
150MB paste on Linux, ~1.6GB if the text wasn't pure ASCII.

A small catalog is enough: tracemalloc counts Python's allocations exactly,
so what's alive when parsing starts is the same multiple of the document's
size at any size.
"""

import gc
import json
import tracemalloc
from unittest import mock
from urllib.parse import urlencode

import pytest

from dcatus_validation import fetch
from tests.utils.large_catalog import FEDERAL, write_catalog

URL = "/api/v1/validate"


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


@pytest.mark.parametrize("enabled", ["true", "false"], ids=["fast path", "kill switch"])
def test_a_paste_is_held_once_while_it_is_parsed(
    validator_client, tmp_path, monkeypatch, enabled
):
    monkeypatch.setenv("DCATUS_VALIDATION_FAST_PATH", enabled)
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
