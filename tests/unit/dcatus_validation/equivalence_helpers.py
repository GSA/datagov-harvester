"""Shared by the tests that hold the fast path (orjson, jsonschema-rs and the
pre-crawled 1.1 registry) to exactly the old pure-Python path's results
(GSA/data.gov#6460).

Each comparison runs one document through both paths, switching with the
DCATUS_VALIDATION_FAST_PATH kill switch, and compares the whole outcome: the
errors (identifier, message, order) and `validation_incomplete`, or the
exception raised.
"""

import contextlib
import copy
import json
import os
from pathlib import Path

from dcatus_validation.fast_path import FAST_PATH_ENV_VAR
from dcatus_validation.limits import MAX_VALIDATION_ERRORS
from dcatus_validation.validate import (
    SCHEMA_NAMES,
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
def fast_path(enabled: bool):
    previous = os.environ.get(FAST_PATH_ENV_VAR)
    os.environ[FAST_PATH_ENV_VAR] = "true" if enabled else "false"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(FAST_PATH_ENV_VAR, None)
        else:
            os.environ[FAST_PATH_ENV_VAR] = previous


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
    with fast_path(False):
        old = outcome(document, schema_name, max_errors)
    with fast_path(True):
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
    document = copy.deepcopy(document)
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
