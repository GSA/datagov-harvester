"""The fast path (jsonschema-rs and the pre-crawled 1.1 registry) gives
exactly the old pure-Python path's results (GSA/data.gov#6460): golden
examples, edge cases, systematic mutations and Hypothesis fuzzing, each
comparing the whole outcome of both paths (see use_fast_path): the errors
(identifier, message, order) and `validation_incomplete`, or the exception
raised. Other test modules import the helpers below.
"""

import contextlib
import copy
import importlib
import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from unittest import mock

import jsonschema_rs
import pytest
import referencing._core
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator, FormatChecker

import dcatus_validation.fast_path as fast_path
import dcatus_validation.validate as validate
from dcatus_validation.errors import NESTING_TOO_DEEP_MESSAGE
from dcatus_validation.fast_path import (
    PYTHON_KEYWORDS,
    PYTHON_VALIDATION_KEYWORDS,
    RUST_KEYWORDS,
    SIMPLE_HTTP_URL,
    build_dcatus1_1_validator,
    build_prefilter,
    check_catalog,
    load_dcatus3_definitions,
    schema_keywords,
    unreviewed_keywords,
)
from dcatus_validation.fetch import parse_json_document
from dcatus_validation.limits import MAX_DOCUMENT_NESTING_DEPTH, MAX_VALIDATION_ERRORS
from dcatus_validation.messages import build_dcatus3_validator, open_json
from dcatus_validation.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR
from dcatus_validation.validate import (
    SCHEMA_NAMES,
    _fast_validators_for_schema,
    validate_records,
    validate_records_limited,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = REPO_ROOT / "example_data" / "dcatus"
DCATUS3_EXAMPLES = REPO_ROOT / "_external" / "dcat-us" / "jsonschema" / "examples"
FEDERAL, NON_FEDERAL, DCATUS3 = SCHEMA_NAMES
DCATUS1_1_SCHEMAS = (FEDERAL, NON_FEDERAL)

# The public API's cap, uncapped (direct callers), and small caps that land
# mid-record and mid-catalog.
CAPS = (MAX_VALIDATION_ERRORS, None, 1, 2, 7)


@contextlib.contextmanager
def use_fast_path(enabled: bool):
    """With enabled=False, validate with Python jsonschema alone, the reference
    the fast path must match, as a document that isn't plain JSON is."""
    if enabled:
        yield
        return
    validate = importlib.import_module("dcatus_validation.validate")
    check_nesting = validate._check_catalog_nesting

    def not_plain_json(document):
        check_nesting(document)
        return False

    with mock.patch.object(
        validate, "_check_catalog_nesting", side_effect=not_plain_json
    ):
        yield


def outcome(document, schema_name, max_errors=MAX_VALIDATION_ERRORS):
    """Everything a caller can observe from validating `document`."""
    try:
        if max_errors is None:
            return ("errors", validate_records(document, schema_name), None)
        errors, incomplete = validate_records_limited(
            document, schema_name, max_errors=max_errors
        )
        return ("errors", errors, incomplete)
    except Exception as error:
        return ("raised", type(error).__name__, str(error))


def both_paths(document, schema_name, max_errors=MAX_VALIDATION_ERRORS):
    with use_fast_path(False):
        old = outcome(document, schema_name, max_errors)
    with use_fast_path(True):
        new = outcome(document, schema_name, max_errors)
    return old, new


def disagreements(document, schema_name, caps=CAPS) -> list:
    """(cap, old outcome, new outcome) for every cap the paths disagree at."""
    found = []
    for cap in caps:
        old, new = both_paths(document, schema_name, cap)
        if old != new:
            found.append((cap, old, new))
    return found


def load_json(path):
    with open(path) as fp:
        return json.load(fp)


def example_catalogs():
    """(name, schema, catalog) for every catalog in example_data/dcatus, with
    each schema it is validated with: 1.1 catalogs under both 1.1 schemas."""
    cases = []
    for path in sorted(EXAMPLES.glob("*.json")):
        schemas = (DCATUS3,) if "3_0" in path.name else DCATUS1_1_SCHEMAS
        for schema in schemas:
            cases.append((path.name, schema, load_json(path)))
    return cases


def catalog_of(*datasets, version="3.0"):
    if version == "1.1":
        return {"dataset": list(datasets)}
    return {
        "@type": "Catalog",
        "title": "Test catalog",
        "description": "A catalog for the equivalence tests",
        "dataset": list(datasets),
    }


# --- systematic mutations ---------------------------------------------------

LONG = "x" * 10001  # past every maxLength in both schemas

# Replacements tried at every position. Wrong types, empties, nulls, extreme
# numbers and values from neighbouring fields.
VALUE_REPLACEMENTS = [
    None,
    True,
    False,
    0,
    -1,
    1.5,
    -0.0,
    1e300,
    2**70,
    -(2**70),
    "",
    " ",
    "x",
    LONG,
    [],
    [None],
    [""],
    ["x", "x"],
    {},
    {"@type": "x"},
]

# Edits tried on every string: what the regex dialects, format checkers and
# string lengths disagree on between libraries.
STRING_EDITS = [
    ("trailing newline", lambda s: s + "\n"),
    ("leading newline", lambda s: "\n" + s),
    ("trailing CRLF", lambda s: s + "\r\n"),
    ("trailing space", lambda s: s + " "),
    ("byte order mark", lambda s: "﻿" + s),
    ("BOM inside", lambda s: s[:1] + "﻿" + s[1:]),
    ("file separator inside", lambda s: s[:1] + "\x1c" + s[1:]),
    ("next line inside", lambda s: s[:1] + "\x85" + s[1:]),
    ("no-break space inside", lambda s: s[:1] + "\xa0" + s[1:]),
    ("arabic-indic digit", lambda s: s + "٣"),
    (
        "fullwidth digits",
        lambda s: s.translate(str.maketrans("0123456789", "０１２３４５６７８９")),
    ),
    ("superscript two", lambda s: s + "²"),
    ("combining accent", lambda s: s + "́"),
    ("connector punctuation", lambda s: s.replace("_", "‿").replace("-", "‿")),
    ("accented letter", lambda s: s + "é"),
    ("emoji", lambda s: s + "\U0001f600"),
    ("upper case", str.upper),
    ("lower case", str.lower),
    ("first char dropped", lambda s: s[1:]),
    ("last char dropped", lambda s: s[:-1]),
    ("doubled", lambda s: s + s),
    ("redacted", lambda s: "[[REDACTED-EX B6]]"),
    ("redacted unclosed", lambda s: "[[REDACTED " + s),
    ("not a uri", lambda s: "not a uri"),
    ("space in uri", lambda s: s.replace("/", "/ ", 1)),
    ("bad percent escape", lambda s: s + "%zz"),
    ("scheme only", lambda s: "http:"),
    ("relative", lambda s: "/relative/" + s),
    ("mailto", lambda s: "mailto:" + s),
    ("mailto without user", lambda s: "mailto:@example.gov"),
    ("bad date", lambda s: "2024-13-45"),
    ("leap day", lambda s: "2023-02-29"),
    ("date time", lambda s: "2024-01-15T10:20:30Z"),
    ("date time no zone", lambda s: "2024-01-15T10:20:30"),
    ("date time offset", lambda s: "2024-01-15T10:20:30.5-05:00"),
    ("date with space", lambda s: "2024-01-15 10:20:30"),
    ("week date", lambda s: "2024-W05-3"),
    ("ordinal date", lambda s: "2024-366"),
    ("year", lambda s: "2024"),
    ("year month", lambda s: "2024-01"),
    ("seven digit year", lambda s: "2024001"),
    ("duration", lambda s: "P1Y2M3DT4H5M6S"),
    ("repeating duration", lambda s: "R/P1Y"),
    ("bad duration", lambda s: "P1Q"),
    ("interval", lambda s: "2020-01-01/2021-12-31"),
    ("interval with duration", lambda s: "2020-01-01/P1Y"),
    ("duration then date", lambda s: "P1Y/2021-12-31"),
    ("open interval", lambda s: "2020-01-01/.."),
    ("media type", lambda s: "text/csv"),
    ("media type with suffix", lambda s: "application/ld+json"),
    ("bad media type", lambda s: "csv"),
    ("bureau code", lambda s: "015:11"),
    ("program code", lambda s: "015:001"),
    ("language tag", lambda s: "en-US"),
    ("bad language tag", lambda s: "english"),
    ("access level", lambda s: "restricted public"),
    ("frequency", lambda s: "irregular"),
]


def positions(document, prefix=()):
    """Every (path, value) below the root of `document`."""
    found = []
    if isinstance(document, dict):
        items = document.items()
    elif isinstance(document, list):
        items = enumerate(document)
    else:
        return found
    for key, value in items:
        found.append((prefix + (key,), value))
        found.extend(positions(value, prefix + (key,)))
    return found


def replaced(document, path, value):
    """A copy of `document` with a copy of `value` at `path`: shared values
    (VALUE_REPLACEMENTS, Hypothesis's draws) must never be edited in place by
    a later mutation."""
    document = copy.deepcopy(document)
    value = copy.deepcopy(value)
    parent = document
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    return document


def deleted(document, path):
    document = copy.deepcopy(document)
    parent = document
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    return document


def mutations(document, string_edits=STRING_EDITS, replacements=VALUE_REPLACEMENTS):
    """(description, mutated copy) for systematic breakages of `document`:
    every position deleted (or, in a list, removed) and replaced by each
    of `replacements`; every string edited by each of `string_edits`;
    every list emptied, duplicated into and grown past 1,000 items; and an
    unknown property added to every object."""
    for path, value in positions(document):
        where = "/".join(map(str, path))
        yield f"{where}: deleted", deleted(document, path)
        for replacement in replacements:
            yield f"{where}: = {replacement!r:.30}", replaced(
                document, path, replacement
            )
        if isinstance(value, str):
            for name, edit in string_edits:
                yield f"{where}: {name}", replaced(document, path, edit(value))
        elif isinstance(value, list) and value:
            yield f"{where}: duplicate item", replaced(
                document, path, value + value[:1]
            )
            yield f"{where}: 1001 items", replaced(document, path, value * 1001)
            yield f"{where}: null item", replaced(document, path, value + [None])
            yield f"{where}: wrapped", replaced(document, path, [value])
        elif isinstance(value, dict):
            yield f"{where}: unknown property", replaced(
                document, path, {**value, "unknownProperty": {"nested": [1]}}
            )
            yield f"{where}: wrapped in a list", replaced(document, path, [value])


# Golden tests: every catalog and schema example the repo has gives exactly the
# same result on the fast path as on the old pure-Python path, at every error
# cap (GSA/data.gov#6460). The comparison covers the whole outcome: each
# error's identifier and message, their order, `validation_incomplete`, or the
# exception.


DEFINITIONS = load_dcatus3_definitions(DCATUS3_DEFINITIONS_DIR)
DEFINITION_URIS = {uri.rsplit("/", 1)[-1]: uri for uri in DEFINITIONS}


def _dcat_us_examples():
    """(definition $id, example path) for every GSA/dcat-us example."""
    cases = []
    for path in sorted(DCATUS3_EXAMPLES.glob("*/*/*.json")):
        uri = DEFINITION_URIS[path.parts[-3].lower()]
        cases.append(pytest.param(uri, path, id="/".join(path.parts[-3:])))
    return cases


EXAMPLE_CATALOGS = example_catalogs()


@pytest.mark.parametrize(
    "name, schema_name, catalog",
    EXAMPLE_CATALOGS,
    ids=[f"{name}-{schema}" for name, schema, _ in EXAMPLE_CATALOGS],
)
@pytest.mark.parametrize("cap", CAPS, ids=lambda cap: f"cap={cap}")
def test_example_catalogs(name, schema_name, catalog, cap):
    old, new = both_paths(catalog, schema_name, cap)

    assert new == old


def test_the_golden_set_has_valid_and_invalid_catalogs_of_both_versions():
    """So the golden tests above exercise both answers for both versions."""
    results = {}
    for _, schema_name, catalog in EXAMPLE_CATALOGS:
        with use_fast_path(False):
            kind, errors, _ = outcome(catalog, schema_name)
        version = "3.0" if schema_name == DCATUS3 else "1.1"
        results.setdefault(version, set()).add(bool(errors))

    assert results == {"1.1": {True, False}, "3.0": {True, False}}


def test_fast_path_matches_python_alone_on_the_whole_golden_set():
    """The fast path and Python jsonschema alone, the whole golden set at once,
    in one comparison."""
    with use_fast_path(True):
        on = [outcome(c, s) for _, s, c in EXAMPLE_CATALOGS]
    with use_fast_path(False):
        off = [outcome(c, s) for _, s, c in EXAMPLE_CATALOGS]

    assert on == off
    assert any(errors for _, errors, _ in off)


@pytest.mark.parametrize("uri, path", _dcat_us_examples())
def test_dcat_us_examples_against_their_own_definition(uri, path):
    """Each GSA/dcat-us example (good and bad, for every definition):
    jsonschema-rs, as the fast path builds it, says valid exactly when Python
    jsonschema finds no errors."""
    python = build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR, root_ref=uri)
    prefilter = build_prefilter({"$ref": uri}, python, resources=DEFINITIONS)
    example = load_json(path)

    assert prefilter.is_valid(example) is (not list(python.iter_errors(example)))


def test_dcat_us_examples_cover_good_and_bad():
    kinds = {path.parts[-2] for _, path in (p.values for p in _dcat_us_examples())}

    assert kinds == {"good", "bad"}


@pytest.mark.parametrize(
    "path",
    sorted((DCATUS3_EXAMPLES / "Catalog").glob("*/*.json")),
    ids=lambda path: "/".join(path.parts[-2:]),
)
@pytest.mark.parametrize("cap", CAPS, ids=lambda cap: f"cap={cap}")
def test_dcat_us_catalog_examples(path, cap):
    old, new = both_paths(load_json(path), DCATUS3, cap)

    assert new == old


@pytest.mark.parametrize(
    "path",
    sorted((DCATUS3_EXAMPLES / "Dataset").glob("*/*.json")),
    ids=lambda path: "/".join(path.parts[-2:]),
)
def test_dcat_us_dataset_examples_in_a_catalog(path):
    dataset = load_json(path)
    catalog = catalog_of(dataset, dataset, {**dataset, "title": None})

    assert disagreements(catalog, DCATUS3) == []


def test_all_dcat_us_dataset_examples_in_one_catalog():
    """Valid and invalid datasets side by side, so the fast path skips some
    datasets and validates others in the same Python run."""
    datasets = [
        load_json(path)
        for path in sorted((DCATUS3_EXAMPLES / "Dataset").glob("*/*.json"))
    ]

    assert disagreements(catalog_of(*datasets, *datasets), DCATUS3) == []


# The structural cases tests/unit/dcatus_validation/test_validate.py pins, and
# other shapes a submitted document can have.
STRUCTURAL_CASES = [
    {},
    [],
    None,
    "catalog",
    5,
    {"dataset": {}},
    {"dataset": None},
    {"dataset": "x"},
    {"dataset": [1]},
    {"dataset": [True]},
    {"dataset": [None]},
    {"dataset": [[]]},
    {"dataset": []},
    {"dataset": [{}]},
    {"dataset": [{"identifier": ""}]},
    {"dataset": [{"identifier": None}]},
    {"dataset": [{"identifier": 42}]},
    {"dataset": [{"identifier": {"not": "a string"}}]},
    {"dataset": [{} for _ in range(101)]},
    {"@type": "Catalog", "dataset": [{}], "catalog": [{"dataset": [{}]}]},
]


@pytest.mark.parametrize("document", STRUCTURAL_CASES, ids=repr)
@pytest.mark.parametrize("schema_name", [*DCATUS1_1_SCHEMAS, DCATUS3])
def test_structural_cases(document, schema_name):
    assert disagreements(document, schema_name) == []


@pytest.mark.parametrize("schema_name", [*DCATUS1_1_SCHEMAS, DCATUS3])
def test_conftest_invalid_fixtures(
    schema_name,
    dcatus_long_description_json,
    dcatus_bad_license_uri_json,
    dcatus_no_identifier_json,
    dcatus_multiple_invalid_json,
    dcatus_3_catalog_missing_identifier,
):
    for document in (
        dcatus_long_description_json,
        dcatus_bad_license_uri_json,
        dcatus_no_identifier_json,
        dcatus_multiple_invalid_json,
        dcatus_3_catalog_missing_identifier,
    ):
        assert disagreements(json.loads(document), schema_name) == []


def test_distribution_example():
    record = load_json(
        REPO_ROOT / "tests" / "distribution-examples" / "dol-example.json"
    )

    for schema_name in (FEDERAL, NON_FEDERAL):
        assert disagreements({"dataset": [record]}, schema_name) == []


def test_every_example_record_alone_and_together():
    """1.1 records one at a time and all in one catalog, so the per-record
    cap arithmetic is compared at every boundary."""
    records = [
        record
        for _, schema_name, catalog in EXAMPLE_CATALOGS
        if schema_name == FEDERAL and isinstance(catalog.get("dataset"), list)
        for record in catalog["dataset"]
    ]
    for schema_name in DCATUS1_1_SCHEMAS:
        for record in records[:60]:
            assert disagreements({"dataset": [record]}, schema_name) == []
        assert disagreements({"dataset": records}, schema_name) == []
        for cap in range(1, 40):
            assert disagreements({"dataset": records}, schema_name, (cap,)) == []


def test_python_validators_match_on_the_golden_set():
    """The fast path's own Python validators (pre-crawled 1.1 registry; 3.0
    definitions without `$schema`, for the `$ref` skipping) report exactly
    what the old ones do, before any skipping."""
    from dcatus_validation.validate import (
        _fast_validators_for_schema,
        _validation_messages,
        _validator_for_schema,
    )

    for _, schema_name, catalog in EXAMPLE_CATALOGS:
        old = _validator_for_schema(schema_name)
        new = _fast_validators_for_schema(schema_name).python
        documents = (
            catalog["dataset"]
            if schema_name != DCATUS3 and isinstance(catalog.get("dataset"), list)
            else [catalog]
        )
        for document in documents:
            assert _validation_messages(new, document) == _validation_messages(
                old, document
            )


def test_jsonschema_rs_and_python_agree_on_every_example_record():
    """Record by record, jsonschema-rs says valid exactly when Python finds no
    errors: not only never more lenient (which the fast path needs), but not
    stricter either (which would only cost time)."""
    from dcatus_validation.validate import _fast_validators_for_schema

    for schema_name in DCATUS1_1_SCHEMAS:
        fast = _fast_validators_for_schema(schema_name)
        for _, catalog_schema, catalog in EXAMPLE_CATALOGS:
            if catalog_schema != schema_name:
                continue
            for record in catalog.get("dataset", []):
                python_valid = not list(fast.python.iter_errors(record))
                assert fast.prefilter.is_valid(record) is python_valid

    fast = _fast_validators_for_schema(DCATUS3)
    for _, schema_name, catalog in EXAMPLE_CATALOGS:
        if schema_name == DCATUS3:
            python_valid = not list(fast.python.iter_errors(catalog))
            assert fast.prefilter.is_valid(catalog) is python_valid


def test_prefilter_only_built_for_draft_2020_12_validators():
    """Both paths run the schemas as Draft 2020-12 with Python's format
    checker; the old validators are what define the expected answer."""
    from dcatus_validation.validate import _validator_for_schema

    for schema_name in (FEDERAL, NON_FEDERAL, DCATUS3):
        validator = _validator_for_schema(schema_name)
        assert isinstance(validator, Draft202012Validator)
        assert isinstance(validator.format_checker, FormatChecker)


# Edge cases, each held to the same result on the fast path as on the old pure-
# Python path (GSA/data.gov#6460): nesting at and past the limit, a
# 100,000-deep document, the error cap, empty catalogs, non-ASCII text, huge
# numbers and nulls in odd places. Documents go through parse_json_document
# first, as they do in the API.


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
        with use_fast_path(enabled):
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


# Mutation and differential tests: valid catalogs, broken systematically and at
# random, give exactly the same result on the fast path as on the old pure-
# Python path (GSA/data.gov#6460).  The systematic mutations
# (equivalence_helpers.mutations) delete every field, replace every value with
# wrong types, empties, nulls and extreme numbers, and edit every string in the
# ways regex dialects, format checkers and string lengths disagree on between
# libraries. The coverage tests prove those mutations make every validation
# keyword of both schemas fail somewhere, and reach into every `$ref`'d sub-
# object.  The Hypothesis test stacks random mutations with random values. It
# is derandomized (the same examples on every run); set
# DCATUS_EQUIVALENCE_EXAMPLES for a larger run, e.g.
# DCATUS_EQUIVALENCE_EXAMPLES=20000.


EXAMPLE_COUNT = int(os.getenv("DCATUS_EQUIVALENCE_EXAMPLES", "300"))

# A federal dataset with every 1.1 property set, valid under both 1.1 schemas.
MAXIMAL_1_1 = {
    "@type": "dcat:Dataset",
    "accessLevel": "public",
    "rights": "Available to the public without restriction.",
    "accrualPeriodicity": "R/P1M",
    "bureauCode": ["015:11"],
    "contactPoint": {
        "@type": "vcard:Contact",
        "fn": "Jane Doe",
        "hasEmail": "mailto:jane.doe@example.gov",
    },
    "describedBy": "https://example.gov/data-dictionary.json",
    "describedByType": "application/schema+json",
    "conformsTo": "https://example.gov/standards/v1",
    "dataQuality": True,
    "description": "Monthly counts of things, by state.",
    "distribution": [
        {
            "@type": "dcat:Distribution",
            "downloadURL": "https://example.gov/data/things.csv",
            "mediaType": "text/csv",
            "format": "CSV",
            "title": "Things (CSV)",
            "description": "All the things.",
            "conformsTo": "https://example.gov/standards/csv",
            "describedBy": "https://example.gov/data-dictionary.csv",
            "describedByType": "text/csv",
        },
        {
            "@type": "dcat:Distribution",
            "accessURL": "https://example.gov/api/things?format=json",
            "mediaType": "application/json",
            "format": "API",
            "title": "Things API",
        },
    ],
    "identifier": "https://example.gov/datasets/things",
    "issued": "2020-01-15",
    "keyword": ["things", "counts"],
    "landingPage": "https://example.gov/things",
    "language": ["en-US"],
    "license": "https://creativecommons.org/publicdomain/zero/1.0/",
    "modified": "2024-01-15T10:20:30Z",
    "primaryITInvestmentUII": "015-999999999",
    "programCode": ["015:001"],
    "publisher": {
        "@type": "org:Organization",
        "name": "Office of Things",
        "subOrganizationOf": {"@type": "org:Organization", "name": "Department"},
    },
    "references": ["https://example.gov/docs/things"],
    "spatial": "United States",
    "systemOfRecords": "https://example.gov/sorn/1",
    "temporal": "2020-01-01/2023-12-31",
    "isPartOf": "https://example.gov/collections/things",
    "theme": ["Statistics"],
    "title": "Things by state",
}


def _dcatus1_1_bases():
    """Valid 1.1 records to break: the maximal one, and real ones."""
    real = load_json(EXAMPLES / "dcatus.json")["dataset"]
    return {
        "maximal": MAXIMAL_1_1,
        "cftc": real[0],
        "spatial-object": load_json(EXAMPLES / "dcatus_spatial_no_translate.json")[
            "dataset"
        ][0],
    }


def _dcatus3_bases():
    """Valid 3.0 datasets to break. The large examples are split up: each of
    their properties alone inside the minimal dataset, which reaches the same
    schema locations at a fraction of the cost."""
    good = DCATUS3_EXAMPLES / "Dataset" / "good"
    bases = {
        name: load_json(good / f"{name}.json")
        for name in (
            "typical_example",
            "null_example",
            "theme_full_example",
            "good-accrual-periodicity",
        )
    }
    minimal = load_json(good / "minimal_example.json")
    complete = load_json(good / "complete_example.json")
    comprehensive = load_json(good / "comprehensive-dataset.json")
    for key, value in complete.items():
        if key not in minimal:
            bases[f"complete_example.{key}"] = {**minimal, key: value}
    for key, value in comprehensive.items():
        if key not in minimal and key not in complete:
            bases[f"comprehensive-dataset.{key}"] = {**minimal, key: value}
    # GeoJSON centroids carry 3.0's only minItems and maxItems.
    for path in sorted((DCATUS3_EXAMPLES / "Location" / "good").glob("*.json")):
        bases[f"location.{path.stem}"] = {**minimal, "spatial": load_json(path)}
    return bases


# The split-up properties get every other string edit; the whole examples get
# all of them.
SAMPLED_STRING_EDITS = STRING_EDITS[::2]


DCATUS1_1_BASES = _dcatus1_1_bases()
DCATUS3_BASES = _dcatus3_bases()


@lru_cache(maxsize=None)
def _mutated(version, base_name):
    base = (DCATUS1_1_BASES if version == "1.1" else DCATUS3_BASES)[base_name]
    edits = SAMPLED_STRING_EDITS if "." in base_name else STRING_EDITS
    return list(mutations(base, string_edits=edits))


@lru_cache(maxsize=None)
def _python_errors(schema_name, base_name):
    """Python jsonschema's raw errors for each mutation of a base, computed
    once for the coverage and soundness tests."""
    version = "3.0" if schema_name == DCATUS3 else "1.1"
    validator = _fast_validators_for_schema(schema_name).python
    wrap = catalog_of if version == "3.0" else (lambda document: document)
    return [
        list(validator.iter_errors(wrap(document)))
        for _, document in _mutated(version, base_name)
    ]


def _batch_disagreement(documents, schema_name, version):
    """Compare a whole batch in one catalog, uncapped and at the API cap; on a
    disagreement, find the first document that disagrees alone."""
    catalog = catalog_of(*documents, version=version)
    if not disagreements(catalog, schema_name, caps=(None, 1000)):
        return None
    for document in documents:
        found = disagreements(catalog_of(document, version=version), schema_name)
        if found:
            return document, found
    return "only together", disagreements(catalog, schema_name, caps=(None, 1000))


@pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
@pytest.mark.parametrize("base_name", sorted(DCATUS1_1_BASES))
def test_dcatus1_1_systematic_mutations(schema_name, base_name):
    cases = _mutated("1.1", base_name)
    assert len(cases) > 1000

    for start in range(0, len(cases), 500):
        batch = [document for _, document in cases[start : start + 500]]
        assert _batch_disagreement(batch, schema_name, "1.1") is None


@pytest.mark.parametrize("base_name", sorted(DCATUS3_BASES))
def test_dcatus3_systematic_mutations(base_name):
    cases = _mutated("3.0", base_name)

    for start in range(0, len(cases), 300):
        batch = [document for _, document in cases[start : start + 300]]
        assert _batch_disagreement(batch, DCATUS3, "3.0") is None


@pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
def test_dcatus1_1_mutations_one_record_at_a_time(schema_name):
    """Each mutation alone, at small caps that stop inside the record."""
    for description, document in _mutated("1.1", "maximal")[::7]:
        found = disagreements({"dataset": [document]}, schema_name, caps=(1, 2, 1000))
        assert not found, description


def test_dcatus3_mutations_one_catalog_at_a_time():
    """Each mutation alone, so a catalog jsonschema-rs finds valid as a whole
    returns before Python runs at all."""
    for description, document in _mutated("3.0", "typical_example"):
        found = disagreements(catalog_of(document), DCATUS3, caps=(1, 1000))
        assert not found, description


def test_dcatus3_catalog_level_mutations():
    """The catalog's own properties, around valid datasets (whose own
    mutations are above)."""
    catalog = load_json(DCATUS3_EXAMPLES / "Catalog" / "good" / "complete_example.json")
    datasets = catalog.pop("dataset")
    for description, document in mutations(
        catalog, string_edits=STRING_EDITS[::4], replacements=VALUE_REPLACEMENTS[::2]
    ):
        if isinstance(document, dict):
            document = {**document, "dataset": datasets}
        assert not disagreements(document, DCATUS3, caps=(1000,)), description


# --- coverage of the mutations ------------------------------------------------


def _schema_paths(errors):
    """Every schema keyword on the way to every error, contexts included."""
    seen = set()
    pending = list(errors)
    while pending:
        error = pending.pop()
        seen.update(k for k in error.absolute_schema_path if isinstance(k, str))
        seen.add(error.validator)
        pending.extend(error.context)
    return seen


def _first_key(error):
    path = list(error.absolute_path)
    return path[0] if path else None


def _validation_keywords(schemas):
    return set().union(*map(schema_keywords, schemas)) & PYTHON_VALIDATION_KEYWORDS


@pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
def test_dcatus1_1_mutations_fail_every_keyword(schema_name):
    schema_file = {
        FEDERAL: "federal_dataset.json",
        NON_FEDERAL: "non-federal_dataset.json",
    }[schema_name]
    expected = _validation_keywords([open_json(DCATUS1_1_DIR / schema_file)])
    failed, paths = set(), set()
    for base_name in DCATUS1_1_BASES:
        for errors in _python_errors(schema_name, base_name):
            failed |= _schema_paths(errors)
            paths.update(_first_key(error) for error in errors)

    # $ref leaves no trace in a schema path; the json paths below show the
    # errors reach into each $ref'd definition instead.
    assert expected - {"$ref"} <= failed
    assert {"contactPoint", "publisher", "distribution"} <= paths


def test_dcatus3_mutations_fail_every_keyword():
    expected = _validation_keywords(
        load_dcatus3_definitions(DCATUS3_DEFINITIONS_DIR).values()
    )
    failed, reached = set(), set()
    for base_name in DCATUS3_BASES:
        for errors in _python_errors(DCATUS3, base_name):
            failed |= _schema_paths(errors)
            reached.update(
                error.absolute_path[2]
                for error in errors
                if list(error.absolute_path)[:2] == ["dataset", 0]
                and len(error.absolute_path) > 2
            )

    assert expected - {"$ref"} <= failed
    # Errors inside $ref'd sub-objects of the dataset (agent, kind,
    # distribution, period of time, location, ...).
    assert {
        "publisher",
        "contactPoint",
        "distribution",
        "temporal",
        "spatial",
        "theme",
        "identifier",
        "conformsTo",
        "landingPage",
    } <= reached


@pytest.mark.parametrize("schema_name", DCATUS1_1_SCHEMAS)
def test_jsonschema_rs_never_accepts_a_mutation_python_rejects(schema_name):
    """The property the fast path rests on, record by record, without the
    rest of the machinery: jsonschema-rs valid implies Python valid. They
    also agree the other way, so the fast path loses no time."""
    prefilter = _fast_validators_for_schema(schema_name).prefilter
    for base_name in DCATUS1_1_BASES:
        cases = zip(_mutated("1.1", base_name), _python_errors(schema_name, base_name))
        for (description, document), errors in cases:
            assert prefilter.is_valid(document) is (not errors), description


def test_jsonschema_rs_never_accepts_a_3_0_mutation_python_rejects():
    fast = _fast_validators_for_schema(DCATUS3)
    _, dataset_prefilter = fast.item_prefilters["dataset"]
    for base_name in DCATUS3_BASES:
        cases = zip(_mutated("3.0", base_name), _python_errors(DCATUS3, base_name))
        for (description, document), errors in cases:
            catalog = catalog_of(document)
            assert fast.prefilter.is_valid(catalog) is (not errors), description
            assert dataset_prefilter.is_valid(document) is (not errors), description
            # The item-by-item check the fast path uses gives the same answer.
            assert check_catalog(catalog, fast).valid is (not errors), description


# --- random mutations (Hypothesis) -------------------------------------------

JSON_SCALARS = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text()
    | st.sampled_from([s for s in VALUE_REPLACEMENTS if isinstance(s, str)])
)
JSON_VALUES = st.recursive(
    JSON_SCALARS,
    lambda children: st.lists(children, max_size=3)
    | st.dictionaries(st.text(max_size=8), children, max_size=3),
    max_leaves=6,
)


@st.composite
def mutated_documents(draw, bases):
    """A base document with one to four random changes: a value replaced by
    a random JSON value or by one of the systematic replacements, a string
    edited, or a field deleted."""
    name = draw(st.sampled_from(sorted(bases)))
    document = copy.deepcopy(bases[name])
    for _ in range(draw(st.integers(1, 4))):
        found = positions(document)
        if not found:
            break
        path, value = draw(st.sampled_from(found))
        action = draw(st.sampled_from(["random", "systematic", "edit", "delete"]))
        if action == "random":
            document = replaced(document, path, draw(JSON_VALUES))
        elif action == "systematic":
            document = replaced(
                document, path, draw(st.sampled_from(VALUE_REPLACEMENTS))
            )
        elif action == "edit" and isinstance(value, str):
            _, edit = draw(st.sampled_from(STRING_EDITS))
            document = replaced(document, path, edit(value))
        else:
            parent = document
            for key in path[:-1]:
                parent = parent[key]
            del parent[path[-1]]
    return document


HYPOTHESIS_SETTINGS = settings(
    max_examples=EXAMPLE_COUNT,
    derandomize=True,
    deadline=None,
    suppress_health_check=list(HealthCheck),
)


@HYPOTHESIS_SETTINGS
@given(
    st.lists(mutated_documents(DCATUS1_1_BASES), min_size=1, max_size=4),
    st.sampled_from(DCATUS1_1_SCHEMAS),
    st.sampled_from([None, 1, 3, 1000]),
)
def test_dcatus1_1_random_mutations(records, schema_name, cap):
    old, new = both_paths({"dataset": records}, schema_name, cap)

    assert new == old


@HYPOTHESIS_SETTINGS
@given(
    st.lists(mutated_documents(DCATUS3_BASES), min_size=1, max_size=4),
    st.sampled_from([None, 1, 3, 1000]),
)
def test_dcatus3_random_mutations(datasets, cap):
    old, new = both_paths(catalog_of(*datasets), DCATUS3, cap)

    assert new == old


@HYPOTHESIS_SETTINGS
@given(JSON_VALUES, st.sampled_from([*DCATUS1_1_SCHEMAS, DCATUS3]))
def test_random_documents(document, schema_name):
    """Anything at all, including non-catalogs, lone surrogates and integers
    of any size: the same outcome, exceptions included."""
    old, new = both_paths(document, schema_name)

    assert new == old


# The fast path's machinery: the pure-Python reference it's compared with,
# which keywords jsonschema-rs may decide alone, the Python keyword bridge, the
# URL shortcut, DCAT-US 3.0's skipping of known-valid items and the pre-crawled
# 1.1 registry (GSA/data.gov#6460). test_equivalence.py compares the two paths'
# results.


FAST_PATH_1_1_SCHEMA_FILES = [
    DCATUS1_1_DIR / "federal_dataset.json",
    DCATUS1_1_DIR / "non-federal_dataset.json",
]


def _all_schemas():
    """Every schema the fast path builds jsonschema-rs for."""
    return [open_json(path) for path in FAST_PATH_1_1_SCHEMA_FILES] + list(
        load_dcatus3_definitions(DCATUS3_DEFINITIONS_DIR).values()
    )


def _bridged(schema):
    """jsonschema-rs for `schema`, built as the fast path builds it."""
    python = Draft202012Validator(schema, format_checker=FormatChecker())
    return build_prefilter(schema, python), python


def _native(schema):
    return jsonschema_rs.Draft202012Validator(schema, validate_formats=True)


class TestReferencePath:
    """The equivalence tests compare the fast path with use_fast_path(False),
    which has to be Python jsonschema alone."""

    @pytest.mark.parametrize("schema_name", SCHEMA_NAMES)
    def test_it_never_touches_jsonschema_rs(self, schema_name):
        from dcatus_validation.fetch import parse_json_document

        document = (EXAMPLES / "dcatus_multiple_invalid.json").read_bytes()
        if schema_name == DCATUS3:
            document = (EXAMPLES / "dcatus3_0_invalid.json").read_bytes()

        with (
            use_fast_path(False),
            mock.patch.object(
                fast_path.Prefilter,
                "is_valid",
                side_effect=AssertionError("jsonschema-rs used"),
            ),
            mock.patch.object(
                validate,
                "_fast_validators_for_schema",
                side_effect=AssertionError("fast validators used"),
            ),
        ):
            errors = validate_records(parse_json_document(document), schema_name)

        assert errors

    def test_the_fast_path_uses_jsonschema_rs(self):
        with mock.patch.object(
            fast_path.Prefilter, "is_valid", autospec=True, return_value=False
        ) as is_valid:
            validate_records(load_json(EXAMPLES / "dcatus.json"), FEDERAL)

        assert is_valid.called


class TestKeywordReview:
    """jsonschema-rs only decides keywords someone has checked it against
    Python jsonschema for. When a schema (say, after a GSA/dcat-us bump) uses
    any other validation keyword, this fails, and the fast path turns itself
    off for that schema until the keyword is reviewed and tested."""

    def test_the_schemas_use_only_reviewed_keywords(self):
        assert unreviewed_keywords(_all_schemas()) == set()

    def test_every_reviewed_keyword_is_one_python_jsonschema_validates(self):
        assert RUST_KEYWORDS <= PYTHON_VALIDATION_KEYWORDS
        assert PYTHON_KEYWORDS <= PYTHON_VALIDATION_KEYWORDS
        assert not RUST_KEYWORDS & PYTHON_KEYWORDS

    def test_every_reviewed_keyword_is_still_used(self):
        """Keep the reviewed list to what the schemas need, so it can't grow
        stale and admit a keyword no test exercises."""
        used = set().union(*map(schema_keywords, _all_schemas()))

        assert RUST_KEYWORDS <= used
        assert PYTHON_KEYWORDS <= used

    def test_the_schemas_are_draft_2020_12(self):
        """jsonschema-rs and the Python path both run them as 2020-12."""
        dialects = set()
        for schema in _all_schemas():
            pending = [schema]
            while pending:
                node = pending.pop()
                if isinstance(node, dict):
                    if "$schema" in node:
                        dialects.add(node["$schema"])
                    pending.extend(node.values())
                elif isinstance(node, list):
                    pending.extend(node)

        assert dialects == {"https://json-schema.org/draft/2020-12/schema"}

    @pytest.mark.parametrize(
        "keyword, value",
        [("minimum", 1), ("not", {"type": "null"}), ("oneOf", [{}, {}])],
    )
    def test_an_unreviewed_keyword_turns_the_fast_path_off(self, keyword, value):
        schema = {"properties": {"a": {keyword: value}}}

        assert build_prefilter(schema, Draft202012Validator(schema)) is None

    @pytest.mark.parametrize(
        "ref, reviewable",
        [
            ("#", True),
            ("#/$defs/vcard", True),
            ("/dcat-us/3.0.0/definitions/dataset", True),
            ("https://example.gov/schema.json", True),
            ("https://example.gov/schema.json#/$defs/a", True),
            ("#/_oldDocs/range", False),
            ("#/properties/a", False),
            ("#/$defs/vcard/properties/fn", False),
            ("#anchor", False),
        ],
    )
    def test_refs_must_point_where_keywords_are_checked(self, ref, reviewable):
        """A `$ref` into an annotation (or deep into a subschema) could apply
        keywords the review never saw."""
        schema = {
            "$defs": {"vcard": {"type": "object"}},
            "_oldDocs": {"range": {"minimum": 1}},
            "properties": {"a": {"$ref": ref}},
        }

        assert (unreviewed_keywords([schema]) == set()) is reviewable

    def test_annotations_and_unknown_keywords_are_not_validation_keywords(self):
        schema = {
            "title": "t",
            "_oldDocs": {"minimum": 3},
            "requirementLevel": "Mandatory",
            "properties": {"minimum": {"type": "string"}},
        }

        assert unreviewed_keywords([schema]) == set()

    @pytest.mark.parametrize("schema_name", SCHEMA_NAMES)
    def test_every_schema_gets_a_prefilter(self, schema_name):
        assert _fast_validators_for_schema(schema_name).prefilter is not None

    def test_a_schema_jsonschema_rs_cannot_compile_falls_back_to_python(self):
        schema = {"$ref": "https://example.invalid/not-bundled.json"}

        assert build_prefilter(schema, Draft202012Validator(schema)) is None

    def test_schemas_are_never_fetched(self):
        """offline=True: an unbundled $ref is an error, not a request."""
        with mock.patch("socket.getaddrinfo", side_effect=AssertionError("DNS")):
            build_prefilter(
                {"$ref": "https://example.com/schema.json"},
                Draft202012Validator({}),
            )


class TestPythonKeywords:
    """Where jsonschema-rs on its own disagrees with Python jsonschema, and
    the fast path's jsonschema-rs, which calls back into Python for these
    keywords, does not."""

    # (schema, instance): jsonschema-rs alone says valid, Python says invalid,
    # or the other way round.
    DISAGREEMENTS = [
        # \w and \d are ASCII-only in jsonschema-rs's ECMA-262 dialect
        ({"pattern": r"^\w+$"}, "é"),
        ({"pattern": r"^\w+$"}, "²"),
        ({"pattern": r"^\d+$"}, "٣"),
        ({"pattern": r"^\D+$"}, "٣"),
        # \s: U+FEFF is whitespace only to jsonschema-rs, U+001C and U+0085
        # only to Python
        ({"pattern": r"^[T\s]$"}, "﻿"),
        ({"pattern": r"^[T\s]$"}, "\x1c"),
        ({"pattern": r"^\S+$"}, "\x1c"),
        ({"pattern": r"^\S+$"}, "﻿"),
        # Python's $ matches before a trailing newline
        ({"pattern": "^abc$"}, "abc\n"),
        # the 1.1 date pattern, with a BOM where the T goes
        (
            {"pattern": r"^(\d{4})-(\d{2})-(\d{2})([T\s](\d{2}):(\d{2}))?$"},
            "2024-01-15﻿10:30",
        ),
        # an integer above 2**64 equals the float nearest to it in Python
        ({"uniqueItems": True}, [2**64, float(2**64)]),
        # format checkers
        ({"format": "uri"}, "http://example.com/a b"),
        ({"format": "date"}, "2024-02-30"),
    ]

    @pytest.mark.parametrize("schema, instance", DISAGREEMENTS)
    def test_bridged_keywords_answer_as_python_does(self, schema, instance):
        prefilter, python = _bridged(schema)

        assert prefilter.is_valid(instance) is python.is_valid(instance)

    def test_these_are_real_disagreements_without_the_bridge(self):
        """Guards the cases above: every regex and equality case does trip
        jsonschema-rs on its own, so the bridge is what makes them agree.
        Five of them would be silent misses: jsonschema-rs alone says valid
        where Python says invalid. (The format cases happen to agree.)"""
        native = {
            index: _native(schema).is_valid(instance)
            for index, (schema, instance) in enumerate(self.DISAGREEMENTS)
        }
        python = {
            index: Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).is_valid(instance)
            for index, (schema, instance) in enumerate(self.DISAGREEMENTS)
        }
        regex_and_equality = range(len(self.DISAGREEMENTS) - 2)

        assert all(native[i] != python[i] for i in regex_and_equality)
        assert sum(native[i] and not python[i] for i in regex_and_equality) == 5

    @pytest.mark.parametrize("keyword", sorted(fast_path._ONLY_CHECKS))
    def test_type_shortcuts_match_python(self, keyword):
        """`pattern` and `uniqueItems` skip the call into Python for values
        Python's own keyword ignores. Every value of the type each one checks
        fails it here, so Python passing exactly the other values shows it
        ignores them."""
        value = {"pattern": "(?!)", "uniqueItems": True}[keyword]
        applies_to = fast_path._ONLY_CHECKS[keyword]
        python = Draft202012Validator({keyword: value})
        prefilter, _ = _bridged({keyword: value})
        instances = [None, True, 0, 1.5, "a", [1, 1], {"a": 1, "b": 1}]

        for instance in instances:
            assert python.is_valid(instance) is (type(instance) is not applies_to)
            assert prefilter.is_valid(instance) is python.is_valid(instance)

    @pytest.mark.parametrize(
        "schema, instance",
        [
            ({"enum": [1]}, True),
            ({"enum": [0]}, False),
            ({"enum": [1]}, 1.0),
            ({"const": 2**64}, float(2**64)),
            ({"const": 2**64 + 1}, 2**64),
            ({"const": [1, {"a": 1}]}, [1.0, {"a": 1.0}]),
            ({"enum": ["a"]}, "a\n"),
            ({"const": "dcat:Dataset"}, "dcat:dataset"),
        ],
    )
    def test_enum_and_const_use_python_equality(self, schema, instance):
        prefilter, python = _bridged(schema)

        assert prefilter.is_valid(instance) is python.is_valid(instance)

    def test_a_python_keyword_error_is_left_to_python(self):
        """An exception inside a Python keyword makes the fast path step aside,
        so Python jsonschema raises it just as it did before."""
        checker = FormatChecker()
        checker.checks("boom")(lambda instance: 1 / 0)
        schema = {"format": "boom"}
        python = Draft202012Validator(schema, format_checker=checker)
        prefilter = build_prefilter(schema, python)

        assert prefilter.is_valid("x") is False
        with pytest.raises(ZeroDivisionError):
            python.validate("x")

    def test_string_answers_are_remembered_per_location(self):
        calls = []
        checker = FormatChecker()
        checker.checks("counted")(lambda instance: calls.append(instance) or True)
        schema = {"items": {"format": "counted"}}
        prefilter = build_prefilter(
            schema, Draft202012Validator(schema, format_checker=checker)
        )

        assert prefilter.is_valid(["a", "a", "b", "a"])
        assert calls == ["a", "b"]

    def test_long_strings_are_not_remembered(self):
        """The validators live as long as the worker, so a submitted catalog's
        long strings mustn't stay in their cache after the request."""
        calls = []
        checker = FormatChecker()
        checker.checks("counted")(lambda instance: calls.append(instance) or True)
        schema = {"items": {"format": "counted"}}
        prefilter = build_prefilter(
            schema, Draft202012Validator(schema, format_checker=checker)
        )
        short, long = "x" * 256, "x" * 257

        assert prefilter.is_valid([short, short, long, long])
        assert calls == [short, long, long]


class TestValuesJsonSchemaRsCannotTake:
    def test_lone_surrogate_is_left_to_python(self):
        prefilter, python = _bridged({"type": "string", "maxLength": 5})

        assert prefilter.is_valid("\ud800") is False
        assert python.is_valid("\ud800") is True

    def test_tuple_is_not_plain_json(self):
        """jsonschema-rs takes a tuple for an array; Python jsonschema
        doesn't. Parsers never produce one, and a document containing one
        skips the fast path entirely."""
        assert _native({"type": "array"}).is_valid((1,)) is True
        assert Draft202012Validator({"type": "array"}).is_valid((1,)) is False
        assert validate._check_catalog_nesting({"dataset": [(1,)]}) is False

    @pytest.mark.parametrize(
        "document",
        [{"a": [1, "x", None, True, 1.5, {"b": []}]}, [], "x", None, 1],
    )
    def test_parsed_json_is_plain(self, document):
        assert validate._check_catalog_nesting(document) is True

    @pytest.mark.parametrize(
        "document",
        [
            {"a": (1,)},
            {"a": b"x"},
            {"a": {1, 2}},
            [type("S", (str,), {})("x")],
            {"a": type("D", (dict,), {})()},
        ],
    )
    def test_anything_else_is_not(self, document):
        assert validate._check_catalog_nesting(document) is False

    def test_a_tuple_catalog_validates_in_python_only(self):
        record = load_json(EXAMPLES / "dcatus.json")["dataset"][0]
        record = {**record, "keyword": tuple(record["keyword"])}
        with use_fast_path(True):
            fast = validate_records({"dataset": [record]}, FEDERAL)
        with use_fast_path(False):
            old = validate_records({"dataset": [record]}, FEDERAL)

        assert fast == old
        assert fast  # a tuple is not an array to Python jsonschema

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_numbers_are_left_to_python(self, value):
        """No parser here produces these, but a direct caller might."""
        prefilter, python = _bridged({"type": "number"})

        assert prefilter.is_valid(value) is False
        assert python.is_valid(value) is True


class TestSimpleHttpUrl:
    """SIMPLE_HTTP_URL lets common URLs skip rfc3987. It must never accept a
    string rfc3987 (through Python jsonschema's format checker) rejects."""

    CHECKER = FormatChecker()

    @settings(
        max_examples=EXAMPLE_COUNT,
        derandomize=True,
        deadline=None,
        suppress_health_check=list(HealthCheck),
    )
    @given(st.from_regex(SIMPLE_HTTP_URL, fullmatch=True))
    def test_everything_it_matches_is_a_uri_and_an_iri(self, url):
        assert self.CHECKER.conforms(url, "uri"), url
        assert self.CHECKER.conforms(url, "iri"), url

    @settings(
        max_examples=EXAMPLE_COUNT,
        derandomize=True,
        deadline=None,
        suppress_health_check=list(HealthCheck),
    )
    @given(
        st.text(
            alphabet=st.characters(codec="utf-8")
            | st.sampled_from("%:/?#@[]!$&'()*+,;=-._~09azAZ \n"),
            max_size=40,
        ).map(lambda tail: "http://" + tail)
    )
    def test_random_url_like_strings(self, url):
        if SIMPLE_HTTP_URL.fullmatch(url):
            assert self.CHECKER.conforms(url, "uri"), url
            assert self.CHECKER.conforms(url, "iri"), url

    @pytest.mark.parametrize(
        "url",
        [
            "http://x",
            "https://example.gov",
            "https://example.gov:8443/",
            "https://data.example.gov/a/b/c.csv?x=1&y=%2A#frag",
            "http://x/a//b/?q/?#f/?",
            "http://1.2.3.4/",
            "http://999.999.999.999/",
            "http://a-b.c-d/",
            "http://x/%C3%A9",
            "http://x/%c3%a9",
            "http://x/:@!$&'()*+,;=-._~",
        ],
    )
    def test_matches_and_is_valid(self, url):
        assert SIMPLE_HTTP_URL.fullmatch(url)
        assert self.CHECKER.conforms(url, "uri")

    @pytest.mark.parametrize(
        "url",
        [
            "http://x\n",
            "http://x/a b",
            "http://x/%zz",
            "http://x/%4",
            "http://x/#a#b",
            "http://-x/",
            "http://x-/",
            "http://x:123456/",
            "http://user@x/",
            "http://[::1]/",
            "http://x/é",
            "HTTP://x/",
            "ftp://x/",
            "http:/x",
            "http://",
            "http://x/[",
        ],
    )
    def test_leaves_everything_else_to_rfc3987(self, url):
        assert not SIMPLE_HTTP_URL.fullmatch(url)

    @pytest.mark.parametrize(
        "url",
        [
            "http://a/" + "a" * 100_000 + " ",
            "http://a?" + "a" * 100_000 + "#" + "b" * 100_000 + " ",
            "http://" + "a-" * 50_000 + "!",
            "http://a/" + "%41" * 50_000 + "%4",
        ],
    )
    def test_a_long_url_that_does_not_match_fails_fast(self, url):
        start = time.perf_counter()
        SIMPLE_HTTP_URL.fullmatch(url)

        assert time.perf_counter() - start < 0.5

    def test_only_used_when_python_checks_urls_with_rfc3987(self, monkeypatch):
        python = Draft202012Validator({}, format_checker=FormatChecker())
        assert fast_path._python_checks_urls_with_rfc3987(python)

        no_formats = Draft202012Validator({})
        assert not fast_path._python_checks_urls_with_rfc3987(no_formats)

        other = FormatChecker()
        other.checks("uri")(lambda instance: True)
        assert not fast_path._python_checks_urls_with_rfc3987(
            Draft202012Validator({}, format_checker=other)
        )

        monkeypatch.setattr("jsonschema._format.rfc3987", None, raising=False)
        assert not fast_path._python_checks_urls_with_rfc3987(python)


class TestDcatus3Skipping:
    def test_skippable_arrays(self):
        fast = _fast_validators_for_schema(DCATUS3)

        assert {name: ref for name, (ref, _) in fast.item_prefilters.items()} == {
            "contactPoint": "/dcat-us/3.0.0/definitions/kind",
            "dataset": "/dcat-us/3.0.0/definitions/dataset",
            "datasetSeries": "/dcat-us/3.0.0/definitions/datasetseries",
            "record": "/dcat-us/3.0.0/definitions/catalogrecord",
            "service": "/dcat-us/3.0.0/definitions/dataservice",
            "theme": "/dcat-us/3.0.0/definitions/concept",
            "themeTaxonomy": "/dcat-us/3.0.0/definitions/conceptscheme",
            "creator": "/dcat-us/3.0.0/definitions/agent",
            "otherIdentifier": "/dcat-us/3.0.0/definitions/identifier",
            "rightsHolder": "/dcat-us/3.0.0/definitions/organization",
            "spatial": "/dcat-us/3.0.0/definitions/location",
            "subject": "/dcat-us/3.0.0/definitions/concept",
            "temporal": "/dcat-us/3.0.0/definitions/periodoftime",
            "category": "/dcat-us/3.0.0/definitions/concept",
            "qualifiedAttribution": "/dcat-us/3.0.0/definitions/attribution",
        }

    def test_nested_catalogs_are_never_skipped(self):
        """`"$ref": "#"` means a different schema in each definition."""
        fast = _fast_validators_for_schema(DCATUS3)

        assert "catalog" not in fast.item_prefilters
        assert "hasPart" not in fast.item_prefilters

    def test_definitions_have_no_nested_dialect_or_id(self):
        """The skipping relies on this (see _build_dcatus3_fast_validators)."""
        for definition in load_dcatus3_definitions(DCATUS3_DEFINITIONS_DIR).values():
            assert not fast_path._has_nested(definition, "$schema")
            assert not fast_path._has_nested(definition, "$id")

    def test_only_invalid_datasets_are_validated_in_python(self):
        good = load_json(EXAMPLES / "dcatus3_0.json")["dataset"]
        bad = {**good[0], "title": 5}
        catalog = catalog_of(*good, bad, *good)
        dataset_ref = "/dcat-us/3.0.0/definitions/dataset"
        python_ref = fast_path._PYTHON_REF
        into_datasets = []

        def counting_ref(validator, ref, instance, schema):
            if ref == dataset_ref:
                into_datasets.append(instance)
            yield from python_ref(validator, ref, instance, schema)

        with (
            use_fast_path(True),
            mock.patch.object(fast_path, "_PYTHON_REF", counting_ref),
        ):
            fast = validate_records(catalog, DCATUS3)
        with use_fast_path(False):
            old = validate_records(catalog, DCATUS3)

        assert fast == old
        assert fast == [
            (
                "",
                f"$.dataset[{len(good)}].title, 5 does not match any of the "
                "acceptable formats: 'string'",
            )
        ]
        assert into_datasets == [bad]

    def test_the_catalog_splits_into_items(self):
        """jsonschema-rs checks a 3.0 catalog item by item (bounded memory)
        rather than in one call, because the catalog schema allows it."""
        assert _fast_validators_for_schema(DCATUS3).splits_into_items is True

    @pytest.mark.parametrize(
        "change",
        [
            {"properties": {"dataset": {"minItems": 1}}},
            {"properties": {"dataset": {"uniqueItems": True}}},
            {"properties": {"dataset": {"maxItems": 3}}},
            {"properties": {"dataset": {"contains": {}}}},
            {"anyOf": [{"required": ["dataset"]}]},
            {"dependentSchemas": {"title": {}}},
            {"unevaluatedProperties": False},
        ],
    )
    def test_no_split_when_the_schema_looks_at_the_arrays(self, change):
        """Emptying an array could then change the answer: a split would be a
        guess, so the whole catalog goes to jsonschema-rs in one call."""
        catalog_schema = {
            "type": "object",
            "required": ["dataset"],
            "properties": {
                "dataset": {"type": "array", "items": {"$ref": "/d"}},
                "title": {"type": "string"},
            },
        }
        for key, value in change.items():
            if key == "properties":
                catalog_schema["properties"]["dataset"].update(value["dataset"])
            else:
                catalog_schema[key] = value

        assert fast_path._splits_into_items(catalog_schema, ["dataset"]) is False
        assert fast_path._splits_into_items(
            {
                "type": "object",
                "properties": {"dataset": {"type": "array", "items": {"$ref": "/d"}}},
            },
            ["dataset"],
        )

    def test_the_rest_of_the_catalog_is_checked_with_the_arrays_emptied(self):
        fast = _fast_validators_for_schema(DCATUS3)
        datasets = load_json(EXAMPLES / "dcatus3_0.json")["dataset"]
        catalog = {**catalog_of(*datasets), "service": [], "keyword": ["a"]}
        seen = []
        real = fast.prefilter.is_valid

        def recording(instance):
            seen.append(instance)
            return real(instance)

        with mock.patch.object(fast.prefilter, "is_valid", side_effect=recording):
            check = fast_path.check_catalog(catalog, fast)

        assert check.valid is True
        assert seen == [{**catalog, "dataset": []}]
        assert len(check.known_valid["/dcat-us/3.0.0/definitions/dataset"]) == len(
            datasets
        )

    @pytest.mark.parametrize(
        "path", sorted((EXAMPLES).glob("dcatus3_0*.json")), ids=lambda p: p.name
    )
    def test_split_and_whole_catalog_checks_agree(self, path):
        fast = _fast_validators_for_schema(DCATUS3)
        catalog = load_json(path)

        assert fast_path.check_catalog(catalog, fast).valid is fast.prefilter.is_valid(
            catalog
        )

    def test_the_known_valid_set_only_lasts_one_validation(self):
        catalog = catalog_of(*load_json(EXAMPLES / "dcatus3_0.json")["dataset"])
        catalog["title"] = 5
        with use_fast_path(True):
            validate_records(catalog, DCATUS3)

        assert fast_path._known_valid_refs.get() is None


class TestPrecrawledRegistry:
    """Change C: the validator API's 1.1 validator crawls its schema once,
    not once per record."""

    @pytest.mark.parametrize("schema_path", FAST_PATH_1_1_SCHEMA_FILES)
    def test_no_crawling_while_validating(self, schema_path):
        validator = build_dcatus1_1_validator(open_json(schema_path))
        records = load_json(EXAMPLES / "dcatus.json")["dataset"]
        crawl = referencing._core.Registry.crawl
        with mock.patch.object(
            referencing._core.Registry, "crawl", autospec=True, wraps=crawl
        ) as crawled:
            for record in records * 3:
                list(validator.iter_errors(record))

        assert crawled.call_count == 0

    def test_the_old_build_crawls_for_every_record(self):
        """What C fixes: publishers with subOrganizationOf crawl each time."""
        validator = Draft202012Validator(
            open_json(DCATUS1_1_DIR / "federal_dataset.json"),
            format_checker=FormatChecker(),
        )
        records = load_json(EXAMPLES / "dcatus.json")["dataset"]
        crawl = referencing._core.Registry.crawl
        with mock.patch.object(
            referencing._core.Registry, "crawl", autospec=True, wraps=crawl
        ) as crawled:
            for record in records:
                list(validator.iter_errors(record))

        assert crawled.call_count >= len(records)

    @pytest.mark.parametrize("schema_path", FAST_PATH_1_1_SCHEMA_FILES)
    def test_same_errors_as_the_old_build(self, schema_path):
        schema = open_json(schema_path)
        old = Draft202012Validator(schema, format_checker=FormatChecker())
        new = build_dcatus1_1_validator(schema)
        records = [
            record
            for path in sorted(EXAMPLES.glob("dcatus*.json"))
            if "3_0" not in path.name
            for record in load_json(path)["dataset"]
        ]
        nested_publisher = {
            "@type": "org:Organization",
            "name": "a",
            "subOrganizationOf": {"name": "b", "subOrganizationOf": {"name": 5}},
        }
        records.append({**records[0], "publisher": nested_publisher})

        def describe(validator, record):
            return [
                (e.message, e.json_path, list(e.schema_path))
                for e in validator.iter_errors(record)
            ]

        for record in records:
            assert describe(new, record) == describe(old, record)


def test_messages_never_come_from_jsonschema_rs():
    """jsonschema-rs only ever answers valid or not. Every message comes from
    Python jsonschema; nothing asks jsonschema-rs for its errors."""
    source = Path(fast_path.__file__).read_text() + Path(validate.__file__).read_text()

    assert set(re.findall(r"_rust_validator\.(\w+)", source)) == {"is_valid"}
    assert set(re.findall(r"prefilter\.(\w+)\(", source)) == {"is_valid"}


def test_example_catalogs_are_json():
    """The golden set the equivalence tests use exists and parses."""
    paths = sorted(EXAMPLES.glob("*.json"))

    assert len(paths) >= 30
    for path in paths:
        json.loads(path.read_text())
