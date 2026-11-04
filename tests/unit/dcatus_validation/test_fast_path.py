"""The fast path's machinery: the pure-Python reference it's compared with,
which keywords jsonschema-rs may
decide alone, the Python keyword bridge, the URL shortcut, DCAT-US 3.0's
skipping of known-valid items and the pre-crawled 1.1 registry
(GSA/data.gov#6460). test_equivalence_*.py compare the two paths' results."""

import json
import os
import re
import time
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
from dcatus_validation.fast_path import (
    PYTHON_KEYWORDS,
    PYTHON_VALIDATION_KEYWORDS,
    RUST_KEYWORDS,
    SIMPLE_HTTP_URL,
    build_prefilter,
    schema_keywords,
    unreviewed_keywords,
)
from dcatus_validation.messages import (
    build_dcatus1_1_validator,
    load_dcatus3_definitions,
    open_json,
)
from dcatus_validation.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR
from dcatus_validation.validate import (
    SCHEMA_NAMES,
    _fast_validators_for_schema,
    validate_records,
)
from tests.unit.dcatus_validation.equivalence_helpers import (
    DCATUS3,
    EXAMPLES,
    FEDERAL,
    catalog_of,
)
from tests.unit.dcatus_validation.equivalence_helpers import fast_path as use_fast_path
from tests.unit.dcatus_validation.equivalence_helpers import load_json

# The validator API's two 1.1 dataset schemas. schemas/dcatus1.1/catalog.json
# validates nothing here.
FAST_PATH_1_1_SCHEMA_FILES = [
    DCATUS1_1_DIR / "federal_dataset.json",
    DCATUS1_1_DIR / "non-federal_dataset.json",
]
EXAMPLE_COUNT = int(os.getenv("DCATUS_EQUIVALENCE_EXAMPLES", "300"))


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
    """The equivalence tests compare the fast path with equivalence_helpers'
    fast_path(False): that has to be Python jsonschema and the stdlib parser
    alone."""

    @pytest.mark.parametrize("schema_name", SCHEMA_NAMES)
    def test_it_never_touches_jsonschema_rs_or_orjson(self, schema_name):
        from dcatus_validation.fetch import parse_json_document

        document = (EXAMPLES / "dcatus_multiple_invalid.json").read_bytes()
        if schema_name == DCATUS3:
            document = (EXAMPLES / "dcatus3_0_invalid.json").read_bytes()

        with (
            use_fast_path(False),
            mock.patch("orjson.loads", side_effect=AssertionError("orjson used")),
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
