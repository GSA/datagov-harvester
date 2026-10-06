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
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    with open(file_path) as fp:
        return json.load(fp)


def get_format_from_str(validation_msg: str) -> str:
    """
    gets the format/rule used against the data (e.g. 'uri', 'string', some regex)
    """
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

    # for constants where a single value is acceptable
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
    # these are all the unique dtypes found in the
    # non-federal schema (no different than federal)
    # {"'boolean'", "'null'", "'array'", "'number'", "'object'", "'string'"}

    # the required field at the root is missing entirely
    if validation_error.json_path == "$":
        return True

    # we need to dig a little deeper when it's a list or dict
    if isinstance(validation_error.instance, (dict, list)):
        # if it's empty you'll get something like
        # ['$.keyword', '[] should be non-empty']
        # which is simple and what we want
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
    """
    build the final validation messages either individually (root) or
    bundled by field. see tests for output.

    the input default dict is organized by { json_path: [errors...]} format
        { "$": [ "'a' is required", "'b' is required" ],
          "$.keyword": [ "[] should be non-empty", "[] is not of type 'string'" ],
          "$.contactPoint.hasEmail": [ format1, format2, format3, etc...]
        }

    the regex says: get me the first word(s) in single quotes or just empty brackets [].
    what's inside the single quotes represents the invalid data
    """

    output = []

    for json_path, formats in messages.items():
        # required property messages aren't based on format but simply
        # "[field] is a required property"
        if is_required_property(formats):
            output += map(
                lambda error: ValidationError(f"{json_path}, {error}"),
                messages[json_path],
            )
            continue

        # all other errors are bundled based on the formats/rules

        # constants like in "accrualPeriodicity" don't include the invalid data
        # but >1 format/rule is used against it so grabbing
        # the last one which is a regex and does include the invalid data
        # excluding constants [0] == [n]
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

        # if neither branch above found anything, none of them will
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

        # build the bundled error message by json_path
        msg = ValidationError(
            f"{json_path}, {invalid_value} does not match any of "
            "the acceptable formats: " + ", ".join(formats)
        )
        output.append(msg)

    return output


def assemble_validation_errors(validation_errors: list, messages=None) -> list:
    """
    given a list of errors, follow each one recursively through its context
    and get the simplest cause for error. store the error in a defaultdict
    such that { json_path: [errors...]}

    errors with lists or dicts (other than empty)
    will often return the entire object followed by 'is not valid under any
    of the given schemas' which isn't helpful.

    pass `messages` to accumulate across calls; the formatted list is returned
    either way.
    """

    if messages is None:
        # {'$.distribution[2].title' = ["'' should be non-empty", etc...]}
        messages = defaultdict(list)

    _collect_validation_messages(validation_errors, messages, forced=False)
    return finalize_validation_messages(messages)


def _collect_validation_messages(
    validation_errors: list, messages: defaultdict, *, forced: bool
) -> int:
    """
    fill `messages` and return how many were appended, nested walks included.
    Formatting is the caller's job; doing it on every recursive return, like
    re-counting the dict, made this quadratic in the number of errors.

    `forced` is a last-resort fallback. After an unforced context walk records
    nothing, we re-walk forced so a same-path type error is reported vaguely
    instead of silently. A walk that already recorded a specific cause is left
    alone.
    """

    recorded = 0

    for error in validation_errors:
        if found_simple_message(error, forced=forced):
            # these aren't specific enough which make them unhelpful
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
            # if not the generic message, and if the message is not already
            # present in the list for the given path we skip to avoid duplicates
            # based on how messages are returned from the validator
            if (
                not is_generic_msg
                and formatted_message not in messages[error.json_path]
            ):
                messages[error.json_path].append(formatted_message)
                recorded += 1

        # Prefer a specific cause in context before falling back.
        from_context = _collect_validation_messages(
            error.context, messages, forced=False
        )
        recorded += from_context

        # Nothing recorded: re-walk forced so the defect is not dropped.
        # `forced` only flips `type` errors, which have no context to recurse.
        if error.context and from_context == 0:
            recorded += _collect_validation_messages(
                error.context, messages, forced=True
            )

    return recorded


def build_dcatus3_validator(
    definitions_dir,
    root_ref="https://resources.data.gov/dcat-us/3.0.0/definitions/catalog",
):
    """
    builds a dcatus v3.0 validator based on schema files in [definitions_dir].

    root_ref selects the entry point into the schema definitions. it defaults to
    the catalog definition (used by the validator web tool to validate a whole
    catalog), but can be pointed at the dataset definition so the validator can
    check a single dataset record at a time during harvest.
    """
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
