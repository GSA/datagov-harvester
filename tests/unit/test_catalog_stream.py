import codecs
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dcatus_validation.catalog import (
    DCATUS3_NON_DATASET_RECORD_TYPES,
    record_identifier,
    split_dcatus3_catalog,
)
from harvester.utils.catalog_stream import CatalogFile, download_json_to_file
from harvester.utils.general_utils import (
    dataset_to_hash,
    sort_dataset,
    strip_dcatus3_catalog_objects,
)

DCATUS_FIXTURES = sorted(
    Path(__file__).parents[2].joinpath("example_data", "dcatus").glob("*.json")
)


def _identifier_field(record_type):
    return DCATUS3_NON_DATASET_RECORD_TYPES.get(record_type, {}).get(
        "identifier_field", "identifier"
    )


def _hash(record):
    return dataset_to_hash(json.dumps(sort_dataset(record)))


def _write(tmp_path, catalog, prefix=b""):
    path = tmp_path / "catalog.json"
    path.write_bytes(prefix + json.dumps(catalog).encode())
    return str(path)


def index_catalog(path, encoding, nested):
    return CatalogFile(str(path), encoding, nested).index()


def iter_materialized(path, encoding, nested, stubs):
    catalog = CatalogFile(str(path), encoding, nested)
    catalog.index()
    return catalog.records(stubs)


def _split_streamed(path):
    return split_dcatus3_catalog(index_catalog(path, "utf-8", nested=True))


@pytest.mark.parametrize("fixture", DCATUS_FIXTURES, ids=lambda p: p.name)
def test_dcatus3_split_matches_in_memory(fixture):
    catalog = json.loads(fixture.read_text())
    if not isinstance(catalog, dict):
        pytest.skip("not a catalog object")
    try:
        old_datasets, old_by_type = split_dcatus3_catalog(catalog)
    except Exception:
        pytest.skip("in-memory split rejects this fixture")

    stub_catalog = index_catalog(str(fixture), "utf-8", nested=True)
    new_datasets, new_by_type = split_dcatus3_catalog(stub_catalog)

    assert strip_dcatus3_catalog_objects(stub_catalog) == (
        strip_dcatus3_catalog_objects(catalog)
    )
    groups = [("dataset", old_datasets, new_datasets)] + [
        (t, old_by_type[t], new_by_type[t]) for t in old_by_type
    ]
    for record_type, old, new in groups:
        field = _identifier_field(record_type)
        # order matters: filter_duplicate_identifiers keeps the first copy
        assert [record_identifier(r, field) for r in new] == [
            record_identifier(r, field) for r in old
        ]
        rebuilt = list(iter_materialized(str(fixture), "utf-8", True, new))
        assert sorted(map(_hash, rebuilt)) == sorted(map(_hash, old))


@pytest.mark.parametrize("fixture", DCATUS_FIXTURES, ids=lambda p: p.name)
def test_dcatus1_1_datasets_match_in_memory(fixture):
    catalog = json.loads(fixture.read_text())
    if not isinstance(catalog, dict) or not isinstance(catalog.get("dataset"), list):
        pytest.skip("no top-level dataset array")

    stubs = index_catalog(str(fixture), "utf-8", nested=False)["dataset"]

    assert [r.get("identifier") for r in stubs] == [
        r.get("identifier") for r in catalog["dataset"]
    ]
    rebuilt = list(iter_materialized(str(fixture), "utf-8", False, stubs))
    assert rebuilt == catalog["dataset"]


def test_sub_catalog_before_top_level_keeps_in_memory_order(tmp_path):
    catalog = {
        "catalog": [{"dataset": [{"identifier": "a", "title": "sub"}]}],
        "dataset": [{"identifier": "a", "title": "top"}],
    }
    path = _write(tmp_path, catalog)

    datasets, _ = _split_streamed(path)

    assert [d["title"] for d in datasets] == ["top", "sub"]


def test_series_members_merge_parent_into_top_level_copy(tmp_path):
    catalog = {
        "dataset": [{"identifier": "a", "title": "top", "description": "kept"}],
        "datasetSeries": [
            {
                "@id": "series",
                "title": "Series",
                "seriesMember": [{"identifier": "a", "title": "member"}],
                "first": {"identifier": "b", "title": "first"},
            }
        ],
    }
    path = _write(tmp_path, catalog)
    datasets, by_type = _split_streamed(path)

    rebuilt = {
        r["identifier"]: r for r in iter_materialized(path, "utf-8", True, datasets)
    }

    assert rebuilt["a"] == {
        "identifier": "a",
        "title": "top",
        "description": "kept",
        "parent_identifier": "series",
    }
    assert rebuilt["b"]["parent_identifier"] == "series"
    (series,) = iter_materialized(path, "utf-8", True, by_type["data_series"])
    assert series["seriesMember"] == [{"identifier": "a", "title": "member"}]


def test_catalog_record_backfilled_id_is_kept(tmp_path):
    catalog = {"record": [{"primaryTopic": "a", "modified": "2024-01-01"}]}
    path = _write(tmp_path, catalog)
    _, by_type = _split_streamed(path)
    backfilled = by_type["catalog_record"][0]["@id"]

    (record,) = iter_materialized(path, "utf-8", True, by_type["catalog_record"])

    assert record["@id"] == backfilled
    assert backfilled.startswith("urn:datagov:catalogrecord:")


def test_stubs_keep_description_only_without_title(tmp_path):
    catalog = {
        "dataset": [
            {"title": "t", "description": "long text"},
            {"description": "fallback"},
        ]
    }
    path = _write(tmp_path, catalog)

    stubs = index_catalog(path, "utf-8", nested=False)["dataset"]

    assert "description" not in stubs[0]
    assert stubs[1]["description"] == "fallback"


def test_numbers_hash_like_json_loads(tmp_path):
    catalog = {"dataset": [{"identifier": "a", "f": 1.5, "e": 1e-7, "i": 10}]}
    path = _write(tmp_path, catalog)
    stubs = index_catalog(path, "utf-8", nested=False)["dataset"]

    (record,) = iter_materialized(path, "utf-8", False, stubs)

    assert _hash(record) == _hash(catalog["dataset"][0])


def test_utf8_bom_is_skipped(tmp_path):
    catalog = {"dataset": [{"identifier": "é"}]}
    path = _write(tmp_path, catalog, prefix=codecs.BOM_UTF8)

    stubs = index_catalog(path, "utf-8", nested=False)["dataset"]

    assert stubs[0]["identifier"] == "é"


def test_non_utf8_encoding_is_decoded(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_bytes('{"dataset": [{"identifier": "é"}]}'.encode("latin-1"))

    stubs = index_catalog(str(path), "ISO-8859-1", nested=False)["dataset"]

    assert stubs[0]["identifier"] == "é"


def test_invalid_utf8_byte_is_replaced_like_requests(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_bytes(b'{"dataset": [{"identifier": "a", "title": "caf\xe9"}]}')

    stubs = index_catalog(path, "utf-8", nested=False)["dataset"]

    assert stubs[0]["title"] == "caf\ufffd"


def test_unknown_charset_falls_back_to_utf8(tmp_path):
    path = _write(tmp_path, {"dataset": [{"identifier": "é"}]})

    stubs = index_catalog(path, "not-a-charset", nested=False)["dataset"]

    assert stubs[0]["identifier"] == "é"


@pytest.mark.parametrize(
    "raw, expected",
    [
        (
            b'{"dataset": [{"identifier": "a", "n": 123456789012345678901234567890}]}',
            {"identifier": "a", "n": 123456789012345678901234567890},
        ),
        (
            b'{"dataset": [{"identifier": "a", "s": "\\udc00"}]}',
            {"identifier": "a", "s": "\udc00"},
        ),
    ],
    ids=["wide-int", "lone-low-surrogate"],
)
def test_values_the_fast_parser_rejects_still_harvest(tmp_path, raw, expected):
    path = tmp_path / "catalog.json"
    path.write_bytes(raw)
    catalog = CatalogFile(str(path), "utf-8", nested=False)
    stubs = catalog.index()["dataset"]

    (record,) = catalog.records(stubs)

    assert record == expected


def test_invalid_json_still_raises(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_bytes(b'{"dataset": [{"identifier": "a"},]}')

    with pytest.raises(Exception):
        index_catalog(path, "utf-8", nested=False)


def test_harvested_field_names_outside_catalogs_stay_in_metadata(tmp_path):
    catalog = {
        "themeTaxonomy": [{"record": [{"@id": "r", "label": "x"}]}],
        "publisher": {"subOrganizationOf": {"service": [{"title": "s"}]}},
        "dataset": [{"identifier": "a"}],
    }
    path = _write(tmp_path, catalog)

    stub_catalog = index_catalog(path, "utf-8", nested=True)

    assert strip_dcatus3_catalog_objects(stub_catalog) == (
        strip_dcatus3_catalog_objects(catalog)
    )


def _response(status_code, body=b"{}", encoding=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.encoding = encoding
    resp.iter_content.return_value = [body[i : i + 3] for i in range(0, len(body), 3)]
    return resp


@patch("harvester.utils.catalog_stream.requests.get")
def test_download_writes_body_and_guesses_encoding(mock_get):
    mock_get.return_value = _response(200, b'{"dataset": []}')

    path, encoding = download_json_to_file("http://example.com/data.json")

    try:
        assert Path(path).read_bytes() == b'{"dataset": []}'
        assert encoding == "utf-8"
        assert mock_get.call_args.kwargs["stream"] is True
    finally:
        os.unlink(path)


@patch("harvester.utils.catalog_stream.requests.get")
def test_download_uses_declared_encoding(mock_get):
    mock_get.return_value = _response(200, encoding="ISO-8859-1")

    path, encoding = download_json_to_file("http://example.com/data.json")

    os.unlink(path)
    assert encoding == "ISO-8859-1"


@patch("harvester.utils.catalog_stream.requests.get")
def test_download_raises_on_error_status(mock_get):
    mock_get.return_value = _response(404)

    with pytest.raises(Exception, match="404"):
        download_json_to_file("http://example.com/data.json")
