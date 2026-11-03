"""Edge cases, each held to the same result on the fast path as on the old
pure-Python path (GSA/data.gov#6460): nesting at and past the limit, a
100,000-deep document, the error cap, empty catalogs, non-ASCII text, huge
numbers and nulls in odd places. Documents go through parse_json_document
first, as they do in the API."""

import json

import pytest

from dcatus_validation.errors import NESTING_TOO_DEEP_MESSAGE
from dcatus_validation.fetch import parse_json_document
from dcatus_validation.limits import MAX_DOCUMENT_NESTING_DEPTH, MAX_VALIDATION_ERRORS
from tests.unit.dcatus_validation.equivalence_helpers import (
    CAPS,
    DCATUS1_1_SCHEMAS,
    DCATUS3,
    DCATUS3_EXAMPLES,
    EXAMPLES,
    FEDERAL,
    NON_FEDERAL,
    both_paths,
    catalog_of,
    disagreements,
    fast_path,
    load_json,
    outcome,
)

ALL_SCHEMAS = [*DCATUS1_1_SCHEMAS, DCATUS3]
RECORD_1_1 = load_json(EXAMPLES / "dcatus.json")["dataset"][0]
DATASET_3_0 = load_json(DCATUS3_EXAMPLES / "Dataset" / "good" / "typical_example.json")


def parsed_both_ways(text, schema_name, cap=MAX_VALIDATION_ERRORS):
    """Parse and validate `text` as the API does (one `try` around both), on
    each path. A document nested 101-1024 deep is refused by the parser on the
    old path and by the post-parse walk on the fast path; the API answers both
    the same, and so does this."""
    results = []
    for enabled in (False, True):
        with fast_path(enabled):
            try:
                document = parse_json_document(text.encode())
            except Exception as error:
                results.append(("raised", type(error).__name__, str(error)))
                continue
            results.append(outcome(document, schema_name, cap))
    return results


def lists_nested(depth):
    document = 0
    for _ in range(depth):
        document = [document]
    return document


def organizations_nested(depth, bad=False):
    """A 1.1 publisher chain: organization `$ref`s itself (`"$ref": "#"`)."""
    organization = {"@type": "org:Organization", "name": 5 if bad else "x"}
    for _ in range(depth):
        organization = {
            "@type": "org:Organization",
            "name": "x",
            "subOrganizationOf": organization,
        }
    return organization


def catalogs_nested(depth, bad=False):
    """A 3.0 catalog chain: Catalog.catalog is `items: {"$ref": "#"}`."""
    catalog = {"@type": "Catalog", "title": 5 if bad else "t", "dataset": []}
    for _ in range(depth):
        catalog = {
            "@type": "Catalog",
            "title": "t",
            "dataset": [],
            "catalog": [catalog],
        }
    return catalog


def depth_of(document):
    if isinstance(document, dict):
        return 1 + max(map(depth_of, document.values()), default=0)
    if isinstance(document, list):
        return 1 + max(map(depth_of, document), default=0)
    return 0


class TestNesting:
    @pytest.mark.parametrize("depth", [99, 100, 101])
    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    def test_plain_nesting(self, depth, schema_name):
        old, new = parsed_both_ways(json.dumps(lists_nested(depth)), schema_name)

        assert new == old
        if depth > MAX_DOCUMENT_NESTING_DEPTH:
            assert old == ("raised", "CatalogTooDeeplyNested", NESTING_TOO_DEEP_MESSAGE)
        else:
            assert old[0] == "errors"

    @pytest.mark.parametrize("bad", [False, True], ids=["valid", "invalid"])
    @pytest.mark.parametrize("chain", [94, 95, 96, 97], ids=lambda n: f"chain={n}")
    @pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
    def test_1_1_publisher_chain(self, chain, bad, schema_name):
        """Python jsonschema recurses through each `subOrganizationOf`; at
        depth 100 it still answers rather than hitting RecursionError, so
        skipping it for a valid record changes nothing."""
        catalog = {
            "dataset": [{**RECORD_1_1, "publisher": organizations_nested(chain, bad)}]
        }
        depth = depth_of(catalog)
        old, new = parsed_both_ways(json.dumps(catalog), schema_name)

        assert new == old
        if depth <= MAX_DOCUMENT_NESTING_DEPTH:
            assert old[0] == "errors"
            assert bool(old[1]) is bad
        else:
            assert old == ("raised", "CatalogTooDeeplyNested", NESTING_TOO_DEEP_MESSAGE)

    @pytest.mark.parametrize("bad", [False, True], ids=["valid", "invalid"])
    @pytest.mark.parametrize("chain", [48, 49, 50], ids=lambda n: f"chain={n}")
    def test_3_0_catalog_chain(self, chain, bad):
        catalog = catalogs_nested(chain, bad)
        depth = depth_of(catalog)
        old, new = parsed_both_ways(json.dumps(catalog), DCATUS3)

        assert new == old
        assert (depth <= MAX_DOCUMENT_NESTING_DEPTH) is (old[0] == "errors")
        if old[0] == "errors":
            assert bool(old[1]) is bad

    def test_depths_99_100_and_101_are_the_ones_tested(self):
        assert [depth_of(catalogs_nested(n)) for n in (48, 49, 50)] == [98, 100, 102]
        record = {**RECORD_1_1, "publisher": organizations_nested(95)}
        assert depth_of({"dataset": [record]}) == 99

    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    @pytest.mark.parametrize("opening", ["[", '{"a":'])
    def test_a_malicious_100000_deep_document(self, schema_name, opening):
        closing = "]" if opening == "[" else "}"
        text = opening * 100_000 + "0" + closing * 100_000
        old, new = parsed_both_ways(text, schema_name)

        assert (
            new
            == old
            == (
                "raised",
                "CatalogTooDeeplyNested",
                NESTING_TOO_DEEP_MESSAGE,
            )
        )

    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    def test_100000_deep_built_in_python(self, schema_name):
        """A direct caller's document, which never went through a parser."""
        assert disagreements(lists_nested(100_000), schema_name) == []


class TestErrorCap:
    @pytest.mark.parametrize("records", [99, 100, 101, 150])
    @pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
    def test_1_1_around_the_cap(self, records, schema_name):
        """An empty 1.1 record has 10 (federal) or 8 (non-federal) errors, so
        these land under, at and over 1,000."""
        catalog = {"dataset": [{} for _ in range(records)]}

        assert disagreements(catalog, schema_name, caps=CAPS) == []

    def test_1_1_over_the_cap_is_incomplete(self):
        old, new = both_paths({"dataset": [{} for _ in range(101)]}, FEDERAL)

        assert new == old
        assert len(old[1]) == MAX_VALIDATION_ERRORS
        assert old[2] is True

    def test_1_1_exactly_at_the_cap_is_complete(self):
        old, new = both_paths({"dataset": [{} for _ in range(100)]}, FEDERAL)

        assert new == old
        assert len(old[1]) == MAX_VALIDATION_ERRORS
        assert old[2] is False

    @pytest.mark.parametrize("datasets", [999, 1000, 1001, 1500])
    def test_3_0_around_the_cap(self, datasets):
        catalog = catalog_of(*({"@type": "Dataset"} for _ in range(datasets)))

        assert disagreements(catalog, DCATUS3, caps=CAPS) == []

    def test_3_0_over_the_cap_is_incomplete(self):
        catalog = {"dataset": [{} for _ in range(251)]}
        old, new = both_paths(catalog, DCATUS3)

        assert new == old
        assert len(old[1]) == MAX_VALIDATION_ERRORS
        assert old[2] is True

    def test_valid_records_between_invalid_ones(self):
        """The fast path skips the valid ones; the cap must still land on the
        same error."""
        records = [
            RECORD_1_1 if i % 3 else {"identifier": f"bad-{i}"} for i in range(400)
        ]

        assert disagreements({"dataset": records}, FEDERAL) == []
        datasets = [DATASET_3_0 if i % 3 else {"title": i} for i in range(1200)]
        assert disagreements(catalog_of(*datasets), DCATUS3) == []


class TestEmptyAndOdd:
    @pytest.mark.parametrize(
        "text",
        ["{}", '{"dataset": []}', "[]", '""', "null", "0", '{"dataset": [{}]}'],
    )
    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    def test_empty_catalogs(self, text, schema_name):
        old, new = parsed_both_ways(text, schema_name)

        assert new == old

    @pytest.mark.parametrize("text", ["", " ", "\n"])
    def test_empty_documents(self, text):
        old, new = parsed_both_ways(text, FEDERAL)

        assert new == old
        assert old[:2] == ("raised", "JSONDecodeError")

    @pytest.mark.parametrize(
        "field, value",
        [
            ("title", None),
            ("keyword", [None]),
            ("keyword", [None, "a"]),
            ("distribution", [None]),
            ("distribution", None),
            ("publisher", None),
            ("contactPoint", None),
            ("contactPoint", {"fn": None, "hasEmail": None}),
            ("bureauCode", [None]),
            ("identifier", None),
            ("modified", None),
        ],
    )
    @pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
    def test_nulls_in_odd_places_1_1(self, field, value, schema_name):
        catalog = {"dataset": [{**RECORD_1_1, field: value}]}
        old, new = parsed_both_ways(json.dumps(catalog), schema_name)

        assert new == old

    @pytest.mark.parametrize(
        "field, value",
        [
            ("title", None),
            ("publisher", None),
            ("contactPoint", [None]),
            ("distribution", [None]),
            ("keyword", [None]),
            ("spatial", [None]),
            ("temporal", None),
        ],
    )
    def test_nulls_in_odd_places_3_0(self, field, value):
        old, new = parsed_both_ways(
            json.dumps(catalog_of({**DATASET_3_0, field: value})), DCATUS3
        )

        assert new == old

    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    @pytest.mark.parametrize(
        "value", ["null", "[null]", '{"dataset": [null]}', '{"dataset": null}']
    )
    def test_null_catalogs(self, schema_name, value):
        old, new = parsed_both_ways(value, schema_name)

        assert new == old


class TestNumbers:
    @pytest.mark.parametrize(
        "literal",
        [
            "0",
            "-0",
            "-0.0",
            "1.5",
            "1e300",
            "5e-324",
            "1E2",
            "9223372036854775807",
            "9223372036854775808",
            "18446744073709551616",
            "-9223372036854775809",
            "1" + "0" * 40,
            "9" * 4300,
            "123456789012345678901234567890.5",
        ],
    )
    @pytest.mark.parametrize("schema_name", ALL_SCHEMAS)
    def test_huge_ints_and_floats(self, literal, schema_name):
        """In a string field, a number field and a list; the value appears in
        the error message, so an int turned float would show."""
        if schema_name == DCATUS3:
            dataset = {**DATASET_3_0, "title": "__N__", "keyword": ["__N__"]}
            spatial = {
                "@type": "Location",
                "centroid": {"type": "Point", "coordinates": ["__N__", 1]},
            }
            text = json.dumps(catalog_of({**dataset, "spatial": spatial}))
        else:
            record = {
                **RECORD_1_1,
                "title": "__N__",
                "keyword": ["__N__"],
                "spatial": {"coordinates": [["__N__"]]},
            }
            text = json.dumps({"dataset": [record]})
        old, new = parsed_both_ways(text.replace('"__N__"', literal), schema_name)

        assert new == old


class TestText:
    TEXTS = [
        "Données ouvertes",
        "公開データ",
        "بيانات مفتوحة",
        "Открытые данные",
        "\U0001f600" * 3,
        "é",
        "‍",
        "٣٤",
        "﻿",
        "\x1c",
        "\x85",
        "line\nbreak",
        "tab\there",
        " ",
        "\x7f",
        "\\u0041",
    ]

    @pytest.mark.parametrize("text", TEXTS, ids=ascii)
    @pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
    def test_unicode_in_every_1_1_string(self, text, schema_name):
        record = json.loads(
            json.dumps(RECORD_1_1),
            object_hook=lambda o: {
                k: (v + text if isinstance(v, str) else v) for k, v in o.items()
            },
        )
        old, new = parsed_both_ways(
            json.dumps({"dataset": [record]}, ensure_ascii=False), schema_name
        )

        assert new == old

    @pytest.mark.parametrize("text", TEXTS, ids=ascii)
    def test_unicode_in_every_3_0_string(self, text):
        dataset = json.loads(
            json.dumps(DATASET_3_0),
            object_hook=lambda o: {
                k: (v + text if isinstance(v, str) else v) for k, v in o.items()
            },
        )
        old, new = parsed_both_ways(
            json.dumps(catalog_of(dataset), ensure_ascii=False), DCATUS3
        )

        assert new == old

    @pytest.mark.parametrize(
        "language",
        ["en", "\U0001f600\U0001f600", "\U0001f600\U0001f600\U0001f600", "é", "éx"],
    )
    def test_max_length_counts_code_points(self, language):
        """3.0 `language` allows two characters: code points in both."""
        catalog = catalog_of({**DATASET_3_0, "language": language})

        assert disagreements(catalog, DCATUS3) == []

    @pytest.mark.parametrize("length", [9999, 10000, 10001])
    @pytest.mark.parametrize("character", ["x", "\U0001f600"])
    def test_description_max_length(self, length, character):
        record = {**RECORD_1_1, "description": character * length}

        for schema_name in (FEDERAL, NON_FEDERAL):
            assert disagreements({"dataset": [record]}, schema_name) == []
