"""Validate a whole submitted DCAT-US catalog (the validator API and
scripts/validate_catalog). The harvest runner validates record by record with
the same messages module instead."""

from jsonschema import Draft202012Validator, FormatChecker

from dcatus_validation.messages import (
    assemble_validation_errors,
    build_dcatus3_validator,
    open_json,
)
from dcatus_validation.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR

# the values accepted for the validator API's `schema` field
SCHEMA_NAMES = [
    "dcatus1.1: federal dataset",
    "dcatus1.1: non-federal dataset",
    "dcatus3.0 catalog",
]


class CatalogTooDeeplyNested(ValueError):
    """
    Catalog's `catalog` and `hasPart` are `items: {"$ref": "#"}`, which jsonschema
    resolves by recursion, so a chain of nested catalogs exhausts the stack at ~17KB
    (depth 200 fails, 150 does not). Raised so callers can say so instead of 500ing.
    """


NESTING_TOO_DEEP_MESSAGE = (
    "Catalog is nested too deeply to validate. "
    "Flatten the nested catalog or hasPart chains and try again."
)


def _validation_messages(validator, document: dict) -> list:
    try:
        errors = assemble_validation_errors(validator.iter_errors(document))
    except RecursionError:
        # `from None`: the stack trace is jsonschema's ref resolution, not a cause
        # the submitter can act on.
        raise CatalogTooDeeplyNested(NESTING_TOO_DEEP_MESSAGE) from None

    return [e.message for e in errors]


def validate_records(dcatus_catalog: dict, schema_name: str) -> list:
    """
    validates records from the input dcatus catalog based on the provided schema_name

    raises CatalogTooDeeplyNested if the document is too deeply nested to walk.
    """

    output = []

    schemas = {
        "dcatus1.1: federal dataset": DCATUS1_1_DIR / "federal_dataset.json",
        "dcatus1.1: non-federal dataset": DCATUS1_1_DIR / "non-federal_dataset.json",
        "dcatus3.0 catalog": DCATUS3_DEFINITIONS_DIR,
    }

    schema = schemas[schema_name]

    if schema_name.startswith("dcatus1.1"):
        validator = Draft202012Validator(
            open_json(schema), format_checker=FormatChecker()
        )

        for idx, record in enumerate(dcatus_catalog["dataset"]):
            errors = _validation_messages(validator, record)
            identifier = idx if "identifier" not in record else record["identifier"]
            output += list(zip([identifier] * len(errors), errors))
    else:
        validator = build_dcatus3_validator(schema)
        errors = _validation_messages(validator, dcatus_catalog)
        # not going to pull the record identifier from the error message for now.
        # the json path will clearly indicate which dataset is
        # wrong (e.g. $.dataset[0] )
        output += list(zip([""] * len(errors), errors))

    return output
