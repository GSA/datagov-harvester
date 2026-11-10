"""Validate a whole submitted DCAT-US catalog (the validator API and
scripts/validate_catalog). The harvest runner validates record by record with
the same messages module instead."""

from functools import lru_cache
from itertools import islice

from jsonschema import Draft202012Validator, FormatChecker

from dcatus_validation.errors import (
    NESTING_TOO_DEEP_MESSAGE,
    CatalogTooDeeplyNested,
)
from dcatus_validation.fast_path import (
    FastValidators,
    Prefilter,
    build_dcatus1_1_fast_validators,
    build_dcatus1_1_validator,
    build_dcatus3_fast_validators,
    build_prefilter,
    check_catalog,
    load_dcatus3_definitions,
    skipping_known_valid_items,
)
from dcatus_validation.limits import MAX_DOCUMENT_NESTING_DEPTH
from dcatus_validation.messages import (
    assemble_validation_errors,
    build_dcatus3_validator,
    open_json,
)
from dcatus_validation.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR

SCHEMA_NAMES = [
    "dcatus1.1: federal dataset",
    "dcatus1.1: non-federal dataset",
    "dcatus3.0 catalog",
]

DCATUS3_CATALOG_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/catalog"

_DCATUS1_1_CATALOG_STRUCTURE_SCHEMA = {
    "type": "object",
    "required": ["dataset"],
    "properties": {
        "dataset": {
            "type": "array",
            "items": {"type": "object"},
        }
    },
}
_DCATUS1_1_CATALOG_STRUCTURE_VALIDATOR = Draft202012Validator(
    _DCATUS1_1_CATALOG_STRUCTURE_SCHEMA
)

# Exactly the types a JSON parser produces. The fast path relies on them:
# jsonschema-rs takes a tuple for an array, for one, where Python jsonschema
# doesn't.
_PLAIN_JSON_TYPES = frozenset({dict, list, str, int, float, bool, type(None)})


def _check_catalog_nesting(document) -> bool:
    """Reject deep containers with a depth-first walk that does not recurse.

    Returns whether every value in the document is a plain JSON type (exactly
    dict, list, str, int, float, bool or None), as it always is after parsing.
    """
    plain = type(document) in _PLAIN_JSON_TYPES
    if isinstance(document, dict):
        root_children = iter(document.values())
    elif isinstance(document, list):
        root_children = iter(document)
    else:
        return plain

    # Keep one iterator per active level instead of putting every item from a
    # wide catalog on a second, potentially large work list.
    stack = [root_children]
    while stack:
        for value in stack[-1]:
            if type(value) not in _PLAIN_JSON_TYPES:
                plain = False
            if isinstance(value, dict):
                child_iterator = iter(value.values())
            elif isinstance(value, list):
                child_iterator = iter(value)
            else:
                continue
            if len(stack) >= MAX_DOCUMENT_NESTING_DEPTH:
                raise CatalogTooDeeplyNested(NESTING_TOO_DEEP_MESSAGE)
            # Resume this level's iterator after the child's subtree.
            stack.append(child_iterator)
            break
        else:
            stack.pop()
    return plain


def _validation_messages(
    validator, document: dict, max_errors: int | None = None
) -> tuple[list, bool]:
    try:
        raw_errors = validator.iter_errors(document)
        incomplete = False
        if max_errors is not None:
            raw_errors = list(islice(raw_errors, max_errors + 1))
            incomplete = len(raw_errors) > max_errors
            raw_errors = raw_errors[:max_errors]

        errors = assemble_validation_errors(raw_errors)
    except RecursionError:
        # `from None`: the stack trace is jsonschema's ref resolution, not a cause
        # the submitter can act on.
        raise CatalogTooDeeplyNested(NESTING_TOO_DEEP_MESSAGE) from None

    messages = [e.message for e in errors]
    if max_errors is not None and len(messages) > max_errors:
        incomplete = True
        messages = messages[:max_errors]
    return messages, incomplete


@lru_cache(maxsize=len(SCHEMA_NAMES))
def _validator_for_schema(schema_name: str):
    """Python jsonschema alone, for a document that isn't plain JSON (a direct
    caller's tuple, say), which the fast path can't take."""
    if schema_name.startswith("dcatus1.1"):
        return Draft202012Validator(
            open_json(_dcatus1_1_schema_path(schema_name)),
            format_checker=FormatChecker(),
        )
    return build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR)


def _dcatus1_1_schema_path(schema_name: str):
    return {
        "dcatus1.1: federal dataset": DCATUS1_1_DIR / "federal_dataset.json",
        "dcatus1.1: non-federal dataset": DCATUS1_1_DIR / "non-federal_dataset.json",
    }[schema_name]


@lru_cache(maxsize=1)
def _dcatus1_1_structure_prefilter() -> Prefilter | None:
    return build_prefilter(
        _DCATUS1_1_CATALOG_STRUCTURE_SCHEMA, _DCATUS1_1_CATALOG_STRUCTURE_VALIDATOR
    )


@lru_cache(maxsize=len(SCHEMA_NAMES))
def _fast_validators_for_schema(schema_name: str) -> FastValidators:
    """The fast path's validators: the same Python validation (with the 1.1
    registry pre-crawled), and jsonschema-rs in front of it."""
    if schema_name.startswith("dcatus1.1"):
        return build_dcatus1_1_fast_validators(
            build_dcatus1_1_validator(open_json(_dcatus1_1_schema_path(schema_name)))
        )
    return build_dcatus3_fast_validators(
        load_dcatus3_definitions(DCATUS3_DEFINITIONS_DIR), DCATUS3_CATALOG_REF
    )


def _validate_records(
    dcatus_catalog: dict,
    schema_name: str,
    max_errors: int | None,
) -> tuple[list, bool]:
    """Return validation errors and whether validation stopped at max_errors."""
    if max_errors is not None and max_errors < 1:
        raise ValueError("max_errors must be at least 1")

    plain_json = _check_catalog_nesting(dcatus_catalog)

    # The fast path only ever skips Python jsonschema for what jsonschema-rs
    # found valid; everything else, and every error, is Python jsonschema's.
    fast = plain_json
    if fast:
        fast_validators = _fast_validators_for_schema(schema_name)
        validator = fast_validators.python
        prefilter = fast_validators.prefilter
    else:
        validator = _validator_for_schema(schema_name)
        prefilter = None

    output = []

    if schema_name.startswith("dcatus1.1"):
        structure_prefilter = _dcatus1_1_structure_prefilter() if fast else None
        if structure_prefilter is None or not structure_prefilter.is_valid(
            dcatus_catalog
        ):
            structure_errors, incomplete = _validation_messages(
                _DCATUS1_1_CATALOG_STRUCTURE_VALIDATOR,
                dcatus_catalog,
                max_errors,
            )
            if structure_errors:
                return (
                    list(zip([""] * len(structure_errors), structure_errors)),
                    incomplete,
                )

        records = dcatus_catalog["dataset"]

        for idx, record in enumerate(records):
            remaining = None if max_errors is None else max_errors - len(output)
            if remaining == 0:
                return output, True

            if prefilter is not None and prefilter.is_valid(record):
                # What Python jsonschema returns for a valid record.
                errors, incomplete = [], False
            else:
                errors, incomplete = _validation_messages(validator, record, remaining)
            identifier = record.get("identifier")
            # A missing, empty or non-string identifier can't label the record,
            # so key its errors by its position in `dataset` instead.
            if not isinstance(identifier, str) or not identifier:
                identifier = idx
            output += list(zip([identifier] * len(errors), errors))

            if incomplete:
                return output, True
            if (
                max_errors is not None
                and len(output) == max_errors
                and idx + 1 < len(records)
            ):
                return output, True

        return output, False

    check = (
        None if prefilter is None else check_catalog(dcatus_catalog, fast_validators)
    )
    if check is None:
        errors, incomplete = _validation_messages(validator, dcatus_catalog, max_errors)
    elif check.valid:
        # What Python jsonschema returns for a valid catalog.
        errors, incomplete = [], False
    else:
        with skipping_known_valid_items(check):
            errors, incomplete = _validation_messages(
                validator, dcatus_catalog, max_errors
            )
    # DCAT-US 3.0 messages carry the JSON path instead of a record identifier.
    return list(zip([""] * len(errors), errors)), incomplete


def load_schema_validators() -> None:
    """Build and cache every schema's validators now, so a missing or broken
    schema fails at startup rather than on the first request that needs it."""
    for schema_name in SCHEMA_NAMES:
        _validator_for_schema(schema_name)
        _fast_validators_for_schema(schema_name)
    _dcatus1_1_structure_prefilter()


def validate_records(dcatus_catalog: dict, schema_name: str) -> list:
    """
    validates records from the input dcatus catalog based on the provided schema_name

    raises CatalogTooDeeplyNested if the document is too deeply nested to walk.
    Direct callers receive the complete error list; the public API uses
    validate_records_limited to bound response amplification.
    """
    errors, _ = _validate_records(dcatus_catalog, schema_name, max_errors=None)
    return errors


def validate_records_limited(
    dcatus_catalog: dict, schema_name: str, max_errors: int
) -> tuple[list, bool]:
    """Validate up to max_errors and report whether validation stopped early."""
    return _validate_records(dcatus_catalog, schema_name, max_errors=max_errors)
