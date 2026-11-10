"""The fast path for DCAT-US validation.

Python jsonschema alone takes tens of seconds on a catalog near
MAX_UPLOAD_MB (GSA/data.gov#6460). The fast path reaches the same answer
sooner: jsonschema-rs decides which records are valid. Python jsonschema still produces
every error, exactly as before, for whatever jsonschema-rs could not show is
valid. The messages, their paths and order, and the MAX_VALIDATION_ERRORS cap
all still come from Python jsonschema alone.

So jsonschema-rs is only ever trusted to say "valid", and the fast path is
correct only while jsonschema-rs never accepts something Python jsonschema
rejects. A miss there would be silent, so this module builds jsonschema-rs to
agree with Python jsonschema by construction rather than by luck:

- Keywords whose meaning depends on the implementation call back into Python
  jsonschema's own function for that keyword (PYTHON_KEYWORDS). jsonschema-rs
  uses ECMA-262 regular expressions, its own format checkers and its own
  number equality; Python jsonschema uses `re`, rfc3987 and friends, and
  Python's `==`.
- Every other keyword the schemas use is one both libraries implement the same
  way (RUST_KEYWORDS, reviewed one by one). A schema that starts using any
  other keyword gets no fast path at all, and a unit test fails, rather than
  trusting jsonschema-rs with a keyword nobody has checked.
- Anything jsonschema-rs can't take (a lone surrogate, a type JSON can't
  produce) counts as "not known to be valid", which costs time, not accuracy.

DCAT-US 1.1 is validated record by record, so a record jsonschema-rs finds
valid simply skips Python. DCAT-US 3.0 is validated as one catalog, so Python
still runs over the catalog but skips the `$ref`s into the datasets (and other
top-level items) jsonschema-rs found valid; see skipping_known_valid_items.
"""

import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from urllib.parse import urljoin

import jsonschema._format
import jsonschema_rs
from jsonschema import Draft202012Validator, FormatChecker, validators
from referencing import Registry
from referencing.jsonschema import DRAFT202012

from dcatus_validation.messages import open_json

logger = logging.getLogger("dcatus_validation")


def build_dcatus1_1_validator(schema: dict):
    """Python jsonschema for one DCAT-US 1.1 dataset schema, with its registry
    crawled once. Python jsonschema adds the root schema uncrawled, so every
    record whose publisher has a subOrganizationOf re-crawled the whole schema
    to resolve its `$id`s, about half of all 1.1 validation time."""
    resource = DRAFT202012.create_resource(schema)
    registry = Registry().with_resource(uri=resource.id() or "", resource=resource)
    return Draft202012Validator(
        schema, registry=registry.crawl(), format_checker=FormatChecker()
    )


def load_dcatus3_definitions(definitions_dir) -> dict:
    """Every DCAT-US 3.0 definition, by its `$id`."""
    schemas = map(open_json, sorted(definitions_dir.glob("*.json")))
    return {schema["$id"]: schema for schema in schemas}


# Validation keywords whose behavior depends on the implementation. For these
# jsonschema-rs calls Python jsonschema's own keyword function, so the answer is
# Python's by construction:
# - pattern: jsonschema-rs uses ECMA-262 regexes, where \w, \d and \s mean
#   something else (e.g. \s matches U+FEFF; Python's doesn't) and `$` doesn't
#   match before a trailing newline. Python uses `re`.
# - format: Python checks "uri" and "iri" with rfc3987, and "date" and
#   "date-time" with its own checkers; jsonschema-rs has its own.
# - uniqueItems, enum, const: equality differs at the edges (an integer above
#   2**64 and the float nearest to it are equal in Python, not in
#   jsonschema-rs).
PYTHON_KEYWORDS = frozenset({"pattern", "format", "uniqueItems", "enum", "const"})

# Validation keywords left to jsonschema-rs, each reviewed against Python
# jsonschema's implementation: structural checks with one meaning in the spec
# and no regex, format, equality or float subtleties. String lengths count
# code points in both. `type` treats bools as neither integer nor number in
# both; jsonschema-rs is stricter only for NaN and infinity (not numbers to
# it), which costs time, not accuracy. Adding a keyword here needs the same
# review and differential tests in tests/unit/dcatus_validation/.
RUST_KEYWORDS = frozenset(
    {
        "$ref",
        "anyOf",
        "dependentSchemas",
        "items",
        "maxItems",
        "maxLength",
        "minItems",
        "minLength",
        "properties",
        "required",
        "type",
    }
)

# Every keyword Python jsonschema acts on. A key outside this set (title,
# $defs, the DCAT-US schemas' own `requirementLevel`, ...) is an annotation to
# Python, so it can't make anything invalid there.
PYTHON_VALIDATION_KEYWORDS = frozenset(Draft202012Validator.VALIDATORS)

# Keywords whose value is a subschema, a list of subschemas, or a mapping of
# names to subschemas: what schema_keywords walks into.
_SUBSCHEMA_KEYWORDS = frozenset(
    {
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)
_SUBSCHEMA_LIST_KEYWORDS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
_SUBSCHEMA_MAP_KEYWORDS = frozenset(
    {"$defs", "definitions", "dependentSchemas", "patternProperties", "properties"}
)


def schema_keywords(schema) -> set[str]:
    """Every keyword used anywhere in a schema, including its subschemas."""
    keywords = set()
    pending = [schema]
    while pending:
        node = pending.pop()
        if not isinstance(node, dict):
            continue  # a boolean schema
        for keyword, value in node.items():
            keywords.add(keyword)
            if keyword in _SUBSCHEMA_KEYWORDS:
                pending.append(value)
            elif keyword in _SUBSCHEMA_LIST_KEYWORDS and isinstance(value, list):
                pending.extend(value)
            elif keyword in _SUBSCHEMA_MAP_KEYWORDS and isinstance(value, dict):
                pending.extend(value.values())
    return keywords


# `$ref`s schema_keywords can follow: to a whole schema (by `$id`, or "#") or
# to one of its `$defs`. A `$ref` into anything else (say "#/_oldDocs/x") could
# make keywords count that schema_keywords never saw.
_REVIEWABLE_REF = re.compile(r"[^#]*(?:#(?:/\$defs/[^/]+)?)?")


def unreviewed_keywords(schemas) -> set[str]:
    """Validation keywords used by `schemas` that no one has checked
    jsonschema-rs against Python jsonschema for, and `$ref`s to places their
    keywords can't be checked."""
    used = set()
    for schema in schemas:
        used |= schema_keywords(schema)
    unreviewed = (used & PYTHON_VALIDATION_KEYWORDS) - RUST_KEYWORDS - PYTHON_KEYWORDS
    unreviewed |= {
        f"$ref {ref}"
        for schema in schemas
        for ref in refs_in(schema)
        if not _REVIEWABLE_REF.fullmatch(ref)
    }
    return unreviewed


def refs_in(schema) -> list[str]:
    """Every `$ref` anywhere in `schema`."""
    refs = []
    pending = [schema]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            if isinstance(node.get("$ref"), str):
                refs.append(node["$ref"])
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return refs


class _NotValidInPython(Exception):
    """Raised inside jsonschema-rs to fail a Python-checked keyword. Only
    jsonschema-rs's valid/invalid answer is used, never this message."""


# Python jsonschema's `pattern` only looks at strings and `uniqueItems` only at
# arrays: `validator.is_type(instance, "string")` (an isinstance check on str)
# or `"array"` (on list) is the first thing each does. Any other value passes
# them, so it needn't call into Python at all.
_ONLY_CHECKS = {"pattern": str, "uniqueItems": list}

# An http(s) URL in a narrow subset of RFC 3986's `URI` grammar: a reg-name
# host, an optional port, then path segments, a query and a fragment made of
# unreserved characters, sub-delims, ":", "@" (and "/" and "?" in the query
# and fragment) or complete %XX escapes. Every string this matches is a URI
# and an IRI to rfc3987, which is what Python jsonschema checks "uri" and "iri"
# with (tests/unit/dcatus_validation/test_equivalence.py checks that with
# Hypothesis). Most catalog URLs match it in about a microsecond, against about
# five for rfc3987's full grammar; anything else still goes to rfc3987.
_PCHARS = r"A-Za-z0-9._~!$&'()*+,;=:@\-"
_PCT_ENCODED = r"%[0-9A-Fa-f]{2}"
SIMPLE_HTTP_URL = re.compile(
    # Possessive (`++`, `*+`): each run stops at a character it can't contain
    # and that must come next, so backtracking into it could never help. This
    # keeps a long URL that doesn't match from taking exponential time.
    r"https?://[A-Za-z0-9](?:[A-Za-z0-9.\-]*[A-Za-z0-9])?(?::[0-9]{1,5})?"
    rf"(?:/(?:[{_PCHARS}]++|{_PCT_ENCODED})*+)*+"
    rf"(?:\?(?:[{_PCHARS}/?]++|{_PCT_ENCODED})*+)?"
    rf"(?:#(?:[{_PCHARS}/?]++|{_PCT_ENCODED})*+)?"
)


def _python_checks_urls_with_rfc3987(python_validator) -> bool:
    """Whether "uri" and "iri" are checked with rfc3987 (the `format` extra),
    the checker SIMPLE_HTTP_URL is known to agree with."""
    format_checker = python_validator.format_checker
    if format_checker is None or getattr(jsonschema._format, "rfc3987", None) is None:
        return False
    return all(
        format_checker.checkers.get(name, (None,))[0]
        is getattr(jsonschema._format, function, None)
        for name, function in (("uri", "is_uri"), ("iri", "is_iri"))
    )


def _python_keyword(keyword: str, python_validator: Draft202012Validator):
    """A jsonschema-rs custom keyword that runs Python jsonschema's own
    function for `keyword`, with the validator the Python path itself uses
    (for its format checker and type checker)."""
    check = python_validator.VALIDATORS[keyword]
    only_checks = _ONLY_CHECKS.get(keyword)
    urls_by_rfc3987 = keyword == "format" and _python_checks_urls_with_rfc3987(
        python_validator
    )

    class PythonKeyword:
        # A string's answer never changes, and catalogs repeat many of them
        # (bureau and program codes, media types, dates), so each schema
        # location remembers recent answers for short strings. The validators
        # live as long as the worker, so the cache is small and skips long
        # strings: a submitted catalog's strings can't stay in memory after
        # its request (at most 256 x 256 characters per location).
        _MAX_REMEMBERED = 256
        _MAX_REMEMBERED_CHARS = 256

        def __init__(self, parent_schema, value, schema_path):
            self._value = value
            self._parent_schema = parent_schema
            self._remembered = {}
            self._simple_url_is_valid = urls_by_rfc3987 and value in ("uri", "iri")

        def _is_valid(self, instance) -> bool:
            for _ in check(
                python_validator, self._value, instance, self._parent_schema
            ):
                return False
            return True

        def validate(self, instance):
            if only_checks is not None and not isinstance(instance, only_checks):
                return
            if type(instance) is not str:
                valid = self._is_valid(instance)
            elif self._simple_url_is_valid and SIMPLE_HTTP_URL.fullmatch(instance):
                return
            elif len(instance) > self._MAX_REMEMBERED_CHARS:
                valid = self._is_valid(instance)
            else:
                valid = self._remembered.get(instance)
                if valid is None:
                    valid = self._is_valid(instance)
                    if len(self._remembered) >= self._MAX_REMEMBERED:
                        self._remembered.clear()
                    self._remembered[instance] = valid
            if not valid:
                raise _NotValidInPython(keyword)

    PythonKeyword.__name__ = f"Python_{keyword}"
    return PythonKeyword


class Prefilter:
    """jsonschema-rs, built to agree with one Python jsonschema validator on
    what is valid. `is_valid` is True only when jsonschema-rs found the
    instance valid; False means "check it with Python jsonschema", not
    "invalid"."""

    def __init__(self, rust_validator):
        self._rust_validator = rust_validator

    def is_valid(self, instance) -> bool:
        try:
            return self._rust_validator.is_valid(instance)
        except Exception:
            # Values jsonschema-rs can't represent (a lone surrogate, a type no
            # JSON parser produces) and errors from a Python keyword. Python
            # jsonschema will answer instead, as it did before the fast path,
            # including by raising the same error.
            return False


def build_prefilter(
    root_schema: dict,
    python_validator: Draft202012Validator,
    resources: dict[str, dict] | None = None,
) -> Prefilter | None:
    """Build jsonschema-rs for `root_schema`, the schema `python_validator`
    validates against. `resources` are the other schemas it may `$ref`, by
    `$id` (DCAT-US 3.0's definitions).

    Returns None, so the caller validates everything in Python, if the schemas
    use a validation keyword jsonschema-rs hasn't been reviewed for, or if
    jsonschema-rs can't compile them.
    """
    resources = resources or {}
    unreviewed = unreviewed_keywords([root_schema, *resources.values()])
    if unreviewed:
        logger.warning(
            "DCAT-US fast path disabled: schema uses unreviewed keywords %s",
            sorted(unreviewed),
        )
        return None

    keywords = {
        keyword: _python_keyword(keyword, python_validator)
        for keyword in PYTHON_KEYWORDS
    }
    try:
        registry = (
            jsonschema_rs.Registry(
                list(resources.items()), draft=jsonschema_rs.Draft202012
            )
            if resources
            else None
        )
        rust_validator = jsonschema_rs.Draft202012Validator(
            root_schema,
            # The Python `format` keyword above checks formats; jsonschema-rs's
            # own format checkers stay off.
            validate_formats=False,
            keywords=keywords,
            registry=registry,
            # Never fetch a schema over the network: every $ref resolves to a
            # bundled schema or not at all.
            offline=True,
        )
    except Exception:
        # A schema jsonschema-rs can't compile (after a dcat-us bump, say)
        # still validates, in Python alone, rather than stopping the app.
        logger.exception("DCAT-US fast path disabled: jsonschema-rs rejected a schema")
        return None
    return Prefilter(rust_validator)


@dataclass(frozen=True)
class FastValidators:
    """What the fast path validates one schema with."""

    # The Python jsonschema validator whose errors are reported: the old
    # path's schema and messages.
    python: Draft202012Validator
    # jsonschema-rs for one 1.1 record, or for a whole 3.0 catalog. None when
    # build_prefilter declined.
    prefilter: Prefilter | None
    # DCAT-US 3.0: catalog property -> (the `$ref` its items use, jsonschema-rs
    # for one item), for the arrays whose valid items Python can skip.
    item_prefilters: dict[str, tuple[str, Prefilter]] = field(default_factory=dict)
    # DCAT-US 3.0: whether the catalog is valid exactly when every item of
    # those arrays is and the rest of the catalog is (see _splits_into_items).
    splits_into_items: bool = False


def build_dcatus1_1_fast_validators(python_validator) -> FastValidators:
    """1.1 is validated record by record, so jsonschema-rs for one record is
    all it needs: a record jsonschema-rs finds valid skips Python."""
    return FastValidators(
        python_validator, build_prefilter(python_validator.schema, python_validator)
    )


# --- DCAT-US 3.0: skipping known-valid items ----------------------------------
#
# 3.0 validates the whole catalog in one Python jsonschema run, so it can't
# simply skip the datasets jsonschema-rs found valid. Instead the run skips the
# `$ref`s into them. For the length of one validation this holds the objects
# jsonschema-rs found valid against a `$ref` target: by the `$ref` as written in
# the schema, then by id().
_known_valid_refs: ContextVar[dict[str, dict[int, object]] | None] = ContextVar(
    "dcatus_known_valid_refs", default=None
)
_PYTHON_REF = Draft202012Validator.VALIDATORS["$ref"]


def _ref_unless_known_valid(validator, ref, instance, schema):
    """Python jsonschema's `$ref`, except for an instance jsonschema-rs already
    found valid against this same `$ref` target. A valid instance yields no
    errors, so skipping it leaves the error stream, and with it every message,
    their order and the error cap, exactly as it was."""
    known_valid = _known_valid_refs.get()
    if known_valid is not None and id(instance) in known_valid.get(ref, ()):
        return
    yield from _PYTHON_REF(validator, ref, instance, schema)


_SkipKnownValidValidator = validators.extend(
    Draft202012Validator, {"$ref": _ref_unless_known_valid}
)


def _has_nested(schema: dict, keyword: str) -> bool:
    """Whether `keyword` appears anywhere in `schema` below its top level."""
    pending = list(schema.values())
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            if keyword in node:
                return True
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return False


def _skippable_item_refs(catalog_schema: dict, definitions: dict) -> dict[str, str]:
    """Catalog property -> `$ref`, for each array whose items are exactly one
    `$ref`, and only where that `$ref` means the same schema in every
    definition, so a skip keyed on the `$ref` as written can't apply to a
    different schema. "#" means a different schema in each definition, so
    nested catalogs are never skipped."""
    targets = {}
    for uri, definition in definitions.items():
        for ref in refs_in(definition):
            targets.setdefault(ref, set()).add(urljoin(uri, ref))

    skippable = {}
    for name, subschema in catalog_schema.get("properties", {}).items():
        items = subschema.get("items") if isinstance(subschema, dict) else None
        if isinstance(items, dict) and set(items) == {"$ref"}:
            ref = items["$ref"]
            if len(targets.get(ref, ())) == 1:
                skippable[name] = ref
    return skippable


def build_dcatus3_fast_validators(
    definitions: dict, catalog_ref: str
) -> FastValidators:
    """DCAT-US 3.0's definitions (by `$id`), for a catalog at catalog_ref."""
    # Python jsonschema switches back to its own Draft202012Validator wherever
    # a schema declares `$schema`, which would drop the `$ref` override above.
    # Each 3.0 definition declares 2020-12 once, at its top, the dialect the
    # validator already runs, so leaving it off changes nothing else. If a
    # definition ever nests `$schema` or `$id`, nothing is skipped rather than
    # guessing what its `$ref`s point to.
    can_skip = not any(
        _has_nested(definition, "$schema") or _has_nested(definition, "$id")
        for definition in definitions.values()
    )
    registry = Registry()
    for uri, definition in definitions.items():
        if can_skip:
            definition = {k: v for k, v in definition.items() if k != "$schema"}
        registry = registry.with_resource(
            uri=uri, resource=DRAFT202012.create_resource(definition)
        )
    validator_class = _SkipKnownValidValidator if can_skip else Draft202012Validator
    python_validator = validator_class(
        {"$ref": catalog_ref}, registry=registry, format_checker=FormatChecker()
    )

    prefilter = build_prefilter({"$ref": catalog_ref}, python_validator, definitions)
    item_prefilters = {}
    if prefilter is not None and can_skip:
        skippable = _skippable_item_refs(definitions[catalog_ref], definitions)
        for name, ref in skippable.items():
            item_prefilter = build_prefilter(
                {"$ref": urljoin(catalog_ref, ref)}, python_validator, definitions
            )
            if item_prefilter is not None:
                item_prefilters[name] = (ref, item_prefilter)
    return FastValidators(
        python_validator,
        prefilter,
        item_prefilters,
        _splits_into_items(definitions[catalog_ref], item_prefilters),
    )


def _splits_into_items(catalog_schema: dict, names) -> bool:
    """Whether a catalog is valid exactly when each item in the `names` arrays
    is valid against its `$ref` and the catalog with those arrays emptied is
    valid. That holds when nothing in the catalog schema looks at those arrays
    except each one's own `type` and `items`: emptying an array changes neither
    its type nor any other property.

    jsonschema-rs then checks a large catalog one item at a time. In one call
    it holds on to memory for every Python keyword it calls back into until the
    call returns: about 150MB for a 150MB catalog."""
    root = set(catalog_schema) & PYTHON_VALIDATION_KEYWORDS
    if not root <= {"type", "required", "properties"}:
        return False
    return all(
        set(catalog_schema["properties"][name]) & PYTHON_VALIDATION_KEYWORDS
        <= {"type", "items"}
        for name in names
    )


@dataclass(frozen=True)
class CatalogCheck:
    """What jsonschema-rs found about one 3.0 catalog."""

    valid: bool
    # By `$ref` as written, the top-level items it found valid, by id(). The
    # objects are kept too, so their ids can't be reused meanwhile.
    known_valid: dict[str, dict[int, object]]


def check_catalog(catalog, fast_validators: FastValidators) -> CatalogCheck:
    """Whether jsonschema-rs finds a 3.0 catalog valid, and which of its
    top-level datasets, services, ... it finds valid."""
    if not isinstance(catalog, dict):
        return CatalogCheck(fast_validators.prefilter.is_valid(catalog), {})

    known_valid, all_items_valid = {}, True
    for name, (ref, item_prefilter) in fast_validators.item_prefilters.items():
        items = catalog.get(name)
        if not isinstance(items, list):
            continue
        valid = known_valid.setdefault(ref, {})
        for item in items:
            if item_prefilter.is_valid(item):
                valid[id(item)] = item
            else:
                all_items_valid = False

    if not fast_validators.splits_into_items:
        valid = fast_validators.prefilter.is_valid(catalog)
    elif not all_items_valid:
        valid = False
    else:
        # The catalog with the arrays just checked emptied.
        rest = {
            key: (
                []
                if key in fast_validators.item_prefilters and isinstance(value, list)
                else value
            )
            for key, value in catalog.items()
        }
        valid = fast_validators.prefilter.is_valid(rest)
    return CatalogCheck(valid, known_valid)


@contextmanager
def skipping_known_valid_items(check: CatalogCheck):
    """For Python validation of the checked catalog inside this block, skip
    the top-level items jsonschema-rs found valid."""
    token = _known_valid_refs.set(check.known_valid)
    try:
        yield
    finally:
        _known_valid_refs.reset(token)
