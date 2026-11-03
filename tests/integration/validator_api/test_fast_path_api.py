"""The validator API answers exactly the same with the fast path on and off
(DCATUS_VALIDATION_FAST_PATH), through both the paste and the URL fetch
paths: status codes, errors, `validation_incomplete` and refusals
(GSA/data.gov#6460)."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from dcatus_validation.errors import NESTING_TOO_DEEP_MESSAGE
from dcatus_validation.fetch import INVALID_JSON_NUMBER_MESSAGE

URL = "/api/v1/validate"
EXAMPLES = Path(__file__).resolve().parents[3] / "example_data" / "dcatus"
FEDERAL = "dcatus1.1: federal dataset"
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
    """POST a document by paste or by URL with the fast path on or off;
    returns (status, JSON body)."""
    monkeypatch.setenv("DCATUS_VALIDATION_FAST_PATH", "true" if fast else "false")
    if fetch_method == "paste":
        text = body if isinstance(body, str) else body.decode("utf-8", "surrogatepass")
        payload = {"schema": schema, "fetch_method": "paste", "json_text": text}
    else:
        raw = body if isinstance(body, bytes) else body.encode("utf-8", "surrogatepass")
        monkeypatch.setattr(
            "dcatus_validation.fetch._pinned_get",
            Mock(side_effect=lambda *args, **kwargs: _response(raw)),
        )
        payload = {
            "schema": schema,
            "fetch_method": "url",
            "url": "https://example.com/data.json",
        }
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
def test_url_documents_orjson_handles_differently(validator_client, monkeypatch, body):
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
def test_pasted_documents_orjson_handles_differently(
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
