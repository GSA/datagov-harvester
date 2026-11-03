"""Mutation and differential tests: valid catalogs, broken systematically and
at random, give exactly the same result on the fast path as on the old
pure-Python path (GSA/data.gov#6460).

The systematic mutations (equivalence_helpers.mutations) delete every field,
replace every value with wrong types, empties, nulls and extreme numbers, and
edit every string in the ways regex dialects, format checkers and string
lengths disagree on between libraries. The coverage tests prove those
mutations make every validation keyword of both schemas fail somewhere, and
reach into every `$ref`'d sub-object.

The Hypothesis test stacks random mutations with random values. It is
derandomized (the same examples on every run); set DCATUS_EQUIVALENCE_EXAMPLES
for a larger run, e.g. DCATUS_EQUIVALENCE_EXAMPLES=20000.
"""

import copy
import os
from functools import lru_cache

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dcatus_validation.fast_path import PYTHON_VALIDATION_KEYWORDS, schema_keywords
from dcatus_validation.messages import load_dcatus3_definitions, open_json
from dcatus_validation.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR
from dcatus_validation.validate import _fast_validators_for_schema
from tests.unit.dcatus_validation.equivalence_helpers import (
    DCATUS1_1_SCHEMAS,
    DCATUS3,
    DCATUS3_EXAMPLES,
    EXAMPLES,
    FEDERAL,
    NON_FEDERAL,
    STRING_EDITS,
    VALUE_REPLACEMENTS,
    both_paths,
    catalog_of,
    disagreements,
    load_json,
    mutations,
    positions,
    replaced,
)

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
            assert fast.prefilter.is_valid(catalog_of(document)) is (
                not errors
            ), description
            assert dataset_prefilter.is_valid(document) is (not errors), description


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
