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

_DCATUS1_1_CATALOG_STRUCTURE_VALIDATOR = Draft202012Validator(
    {
        "type": "object",
        "required": ["dataset"],
        "properties": {
            "dataset": {
                "type": "array",
                "items": {"type": "object"},
            }
        },
    }
)


def _check_catalog_nesting(document) -> None:
    """Reject deep containers with a depth-first walk that does not recurse."""

    def children(value):
        if isinstance(value, dict):
            return iter(value.values())
        if isinstance(value, list):
            return iter(value)
        return None

    root_children = children(document)
    if root_children is None:
        return

    # Keep one iterator per active level instead of putting every item from a
    # wide catalog on a second, potentially large work list.
    stack = [root_children]
    while stack:
        try:
            value = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue

        child_iterator = children(value)
        if child_iterator is None:
            continue
        if len(stack) >= MAX_DOCUMENT_NESTING_DEPTH:
            raise CatalogTooDeeplyNested(NESTING_TOO_DEEP_MESSAGE)
        stack.append(child_iterator)


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
    if schema_name.startswith("dcatus1.1"):
        schema_path = {
            "dcatus1.1: federal dataset": DCATUS1_1_DIR / "federal_dataset.json",
            "dcatus1.1: non-federal dataset": (
                DCATUS1_1_DIR / "non-federal_dataset.json"
            ),
        }[schema_name]
        return Draft202012Validator(
            open_json(schema_path), format_checker=FormatChecker()
        )
    return build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR)


def _validate_records(
    dcatus_catalog: dict,
    schema_name: str,
    max_errors: int | None,
) -> tuple[list, bool]:
    """Return validation errors and whether validation stopped at max_errors."""
    if max_errors is not None and max_errors < 1:
        raise ValueError("max_errors must be at least 1")

    _check_catalog_nesting(dcatus_catalog)

    output = []

    if schema_name.startswith("dcatus1.1"):
        structure_errors, incomplete = _validation_messages(
            _DCATUS1_1_CATALOG_STRUCTURE_VALIDATOR,
            dcatus_catalog,
            max_errors,
        )
        if structure_errors:
            return list(zip([""] * len(structure_errors), structure_errors)), incomplete

        validator = _validator_for_schema(schema_name)
        records = dcatus_catalog["dataset"]

        for idx, record in enumerate(records):
            remaining = None if max_errors is None else max_errors - len(output)
            if remaining == 0:
                return output, True

            errors, incomplete = _validation_messages(validator, record, remaining)
            identifier = record.get("identifier")
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

    validator = _validator_for_schema(schema_name)
    errors, incomplete = _validation_messages(validator, dcatus_catalog, max_errors)
    # DCAT-US 3.0 messages carry the JSON path instead of a record identifier.
    return list(zip([""] * len(errors), errors)), incomplete


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
