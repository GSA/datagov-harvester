"""Error-message formatting for DCAT-US schema validation.

Shared by the harvest runner (the record errors a harvest job reports) and the
validator API, so both describe the same bad record the same way.
"""

import json
import logging
import re
from collections import defaultdict

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry
from referencing.jsonschema import DRAFT202012

logger = logging.getLogger("dcatus_validation")


def open_json(file_path):
    """Load a JSON file."""
    with open(file_path) as fp:
        return json.load(fp)


def get_format_from_str(validation_msg: str) -> str:
    """Extract the format or rule from a jsonschema message."""
    if "is too long" in validation_msg:
        match = re.search(r"\[maxLength=(\d+)\]", validation_msg)
        if match:
            return f"max string length of {match.group(1)} characters"
        match = re.search(r"\[maxItems=(\d+)\]", validation_msg)
        if match:
            return f"max {match.group(1)} items"
        return "max string length requirement"

    if "is too short" in validation_msg:
        match = re.search(r"\[minItems=(\d+)\]", validation_msg)
        if match:
            return f"min {match.group(1)} items"
        return "min items requirement"

    # Match jsonschema's full "has non-unique elements" wording, not just
    # "non-unique" -- an invalid value containing that text would hijack the match.
    if "has non-unique elements" in validation_msg:
        return "unique items"

    if "was expected" in validation_msg:
        return f"constant value {validation_msg}"
    return validation_msg.split(" ")[-1]


def found_simple_message(
    validation_error: ValidationError, forced: bool = False
) -> bool:
    """
    determine whether the input validation error represents the most
    succinct cause for error based on its json_path or dtype.

    `forced` is a last-resort override set by `_collect_validation_messages`.
    """
    if validation_error.json_path == "$":
        return True

    if isinstance(validation_error.instance, (dict, list)):
        if len(validation_error.instance) == 0:
            return True

        # `type` errors have no `context`. Keep them when the allowed types are
        # a list, when this is a top-level error, or when the path is deeper
        # than the parent (a real cause inside a branch). Same-path single-type
        # errors are anyOf/oneOf branch noise unless `forced`.
        if validation_error.validator == "type":
            return bool(
                isinstance(validation_error.validator_value, list)
                or validation_error.parent is None
                or validation_error.json_path != validation_error.parent.json_path
                or forced
            )

        # Combinators are not a cause; their `.context` holds the per-branch errors.
        if validation_error.validator in ("anyOf", "oneOf", "allOf", "not"):
            return False

        # Other container validators (maxItems, minItems, uniqueItems, ...) are
        # already the specific cause.
        return True
    return True


def is_required_property(messages: str) -> bool:
    for message in messages:
        if "is a required property" in message:
            return True

    return False


def _unquoted_type_error_value(message: str) -> str | None:
    """Extract JSON scalars that jsonschema renders without quotes."""
    value, separator, _ = message.partition(" is not of type ")
    if not separator or not value or value[0] in "'\"[{<":
        return None
    return value


def finalize_validation_messages(messages: defaultdict) -> list:
    """Bundle validation messages by JSON path."""

    output = []

    for json_path, formats in messages.items():
        if is_required_property(formats):
            output += map(
                lambda error: ValidationError(f"{json_path}, {error}"),
                messages[json_path],
            )
            continue

        # jsonschema renders containers as repr; quoting an inner element (or a
        # const's expected value) would mislead, so name the kind of value.
        # "[]" already reads as itself.
        container = next(
            (f for f in formats if f[:1] in ("[", "{") and f[:2] != "[]"), None
        )
        if formats[-1].startswith("None"):
            invalid_value = "None"
        elif container is not None:
            invalid_value = "array value" if container[0] == "[" else "object value"
        elif unquoted_value := _unquoted_type_error_value(formats[-1]):
            invalid_value = unquoted_value
        else:
            # group(0): the `[]` alternative has no capture groups.
            match = re.search(r"'(.*?)'|\[\]", formats[-1])
            invalid_value = match.group(0) if match else None

        if invalid_value is None:
            logger.warning(f"can't find invalid data from error message: {formats[0]}")
            continue

        # @type values are consts too
        if invalid_value in [
            "'dcat:Distribution'",
            "'org:Organization'",
            "'dcat:Dataset'",
            "'vcard:Contact'",
        ]:
            invalid_value = "@type value"

        formats = map(get_format_from_str, formats)

        msg = ValidationError(
            f"{json_path}, {invalid_value} does not match any of "
            "the acceptable formats: " + ", ".join(formats)
        )
        output.append(msg)

    return output


def assemble_validation_errors(validation_errors: list, messages=None) -> list:
    """Return the most specific causes, grouped by JSON path."""

    if messages is None:
        messages = defaultdict(list)

    _collect_validation_messages(validation_errors, messages, forced=False)
    return finalize_validation_messages(messages)


def _collect_validation_messages(
    validation_errors: list, messages: defaultdict, *, forced: bool
) -> int:
    """Append specific causes recursively and return how many were added."""

    recorded = 0

    for error in validation_errors:
        if found_simple_message(error, forced=forced):
            generic_msg = "is not valid under any of the given schemas"
            is_generic_msg = error.message.endswith(generic_msg)
            if error.validator == "maxLength":
                formatted_message = (
                    f"{error.message} [maxLength={error.validator_value}]"
                )
            elif error.validator == "maxItems":
                formatted_message = (
                    f"{error.message} [maxItems={error.validator_value}]"
                )
            elif error.validator == "minItems" and "is too short" in error.message:
                # minItems: 1 already says "should be non-empty";
                # only "is too short" needs the count.
                formatted_message = (
                    f"{error.message} [minItems={error.validator_value}]"
                )
            else:
                formatted_message = error.message
            # Avoid duplicate messages for a path.
            if (
                not is_generic_msg
                and formatted_message not in messages[error.json_path]
            ):
                messages[error.json_path].append(formatted_message)
                recorded += 1

        from_context = _collect_validation_messages(
            error.context, messages, forced=False
        )
        recorded += from_context

        # Fall back to a vague type error rather than dropping the defect.
        if error.context and from_context == 0:
            recorded += _collect_validation_messages(
                error.context, messages, forced=True
            )

    return recorded


def build_dcatus3_validator(
    definitions_dir,
    root_ref="https://resources.data.gov/dcat-us/3.0.0/definitions/catalog",
):
    """Build a DCAT-US 3.0 validator for the selected root definition."""
    registry = Registry()

    schema_files = sorted(definitions_dir.glob("*.json"))
    if not schema_files:
        raise FileNotFoundError(
            f"no JSON Schema definitions found in {definitions_dir}. "
            "DCAT-US 3.0 definitions come from the GSA/dcat-us git submodule; "
            "run `git submodule update --init _external/dcat-us`."
        )

    for schema_file in schema_files:
        schema = open_json(schema_file)
        registry = registry.with_resource(
            uri=schema["$id"],
            resource=DRAFT202012.create_resource(schema),
        )

    return Draft202012Validator(
        schema={"$ref": root_ref},
        registry=registry,
        format_checker=FormatChecker(),
    )
