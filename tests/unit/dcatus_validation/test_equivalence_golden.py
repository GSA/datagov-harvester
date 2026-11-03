"""Golden tests: every catalog and schema example the repo has gives exactly
the same result on the fast path as on the old pure-Python path, at every error
cap (GSA/data.gov#6460). The comparison covers the whole outcome: each error's
identifier and message, their order, `validation_incomplete`, or the exception.

The kill switch (DCATUS_VALIDATION_FAST_PATH) is what selects the path, so
these are also the kill switch's tests: both settings give identical results
on the golden set.
"""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from dcatus_validation.fast_path import build_prefilter
from dcatus_validation.messages import (
    build_dcatus3_validator,
    load_dcatus3_definitions,
)
from dcatus_validation.schema_paths import DCATUS3_DEFINITIONS_DIR
from tests.unit.dcatus_validation.equivalence_helpers import (
    CAPS,
    DCATUS1_1_SCHEMAS,
    DCATUS3,
    DCATUS3_EXAMPLES,
    FEDERAL,
    NON_FEDERAL,
    both_paths,
    catalog_of,
    disagreements,
    example_catalogs,
    fast_path,
    load_json,
    outcome,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
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
        with fast_path(False):
            kind, errors, _ = outcome(catalog, schema_name)
        version = "3.0" if schema_name == DCATUS3 else "1.1"
        results.setdefault(version, set()).add(bool(errors))

    assert results == {"1.1": {True, False}, "3.0": {True, False}}


def test_kill_switch_settings_agree_on_the_whole_golden_set():
    """Both settings, the whole golden set at once, in one comparison."""
    with fast_path(True):
        on = [outcome(c, s) for _, s, c in EXAMPLE_CATALOGS]
    with fast_path(False):
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


def test_harvest_runner_validators_match_on_the_golden_set():
    """Change C in the harvest runner: per-record 1.1 errors with the
    pre-crawled registry are the old ones, message for message."""
    from dcatus_validation.messages import (
        assemble_validation_errors,
        build_dcatus1_1_validator,
        open_json,
    )
    from dcatus_validation.schema_paths import DCATUS1_1_DIR

    for schema_file in (
        "federal_dataset.json",
        "non-federal_dataset.json",
        "iso-non-federal_dataset.json",
    ):
        schema = open_json(DCATUS1_1_DIR / schema_file)
        old = build_dcatus1_1_validator(schema, precrawl=False)
        new = build_dcatus1_1_validator(schema)
        for _, schema_name, catalog in EXAMPLE_CATALOGS:
            if schema_name != FEDERAL or not isinstance(catalog.get("dataset"), list):
                continue
            for record in catalog["dataset"]:
                assert [
                    e.message
                    for e in assemble_validation_errors(new.iter_errors(record))
                ] == [
                    e.message
                    for e in assemble_validation_errors(old.iter_errors(record))
                ]


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
