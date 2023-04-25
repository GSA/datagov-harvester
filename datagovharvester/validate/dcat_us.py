from jsonschema import Draft202012Validator


def parse_errors(errors):
    error_message = ""

    for error in errors:
        error_message += (
            f"error: {error.message}. offending element: {error.json_path} \n"
        )

    return error_message


def validate_json_schema(json_data, dataset_schema):
    success = False
    error_message = ""

    validator = Draft202012Validator(dataset_schema)
    errors = list(validator.iter_errors(json_data))

    if len(errors) == 0:
        success = True
    else:
        error_message = parse_errors(errors)

    return success, error_message
