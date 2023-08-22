from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError


def is_dcatus_schema(catalog):
    if "dataset" in catalog:
        return True

    return False


def parse_errors(errors):
    error_message = ""

    for error in errors:
        error_message += (
            f"error: {error.message}. offending element: {error.json_path} \n"
        )

    return error_message


def validate_json_schema(json_data, dataset_schema):
    success = None
    error_message = ""

    validator = Draft202012Validator(dataset_schema)

    try:
        validator.validate(json_data)
        success = True
        error_message = "no errors"
    except ValidationError:
        success = False
        errors = validator.iter_errors(json_data)
        error_message = parse_errors(errors)

    return success, error_message
