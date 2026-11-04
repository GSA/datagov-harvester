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

import pytest

from dcatus_validation import fetch
from tests.utils.large_catalog import FEDERAL, write_catalog

URL = "/api/v1/validate"


@pytest.mark.parametrize("enabled", ["true", "false"], ids=["fast path", "kill switch"])
def test_a_paste_is_held_once_while_it_is_parsed(
    validator_client, tmp_path, monkeypatch, enabled
):
    monkeypatch.setenv("DCATUS_VALIDATION_FAST_PATH", enabled)
    path = tmp_path / "catalog.json"
    write_catalog(path, 2 * 1024 * 1024)
    # Not pure ASCII, as real catalogs often aren't: CPython then stores the
    # text at 2 bytes a character, the costliest case.
    text = path.read_text().replace('"title": "', '"title": "’')
    path.unlink()
    document_size = len(text.encode("utf-8"))
    body = json.dumps(
        {"schema": FEDERAL, "fetch_method": "paste", "json_text": text}
    ).encode()
    del text

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
            res = validator_client.post(URL, data=body, content_type="application/json")
    finally:
        tracemalloc.stop()

    assert res.status_code == 200
    assert res.get_json() == {"validation_errors": []}
    # The document's UTF-8 bytes, plus a little: not the text (here 2 bytes a
    # character) and the request body again on top.
    [alive] = alive_at_parse
    assert alive < 1.3 * document_size, (alive, document_size)
