import jsonschema


def validate_json_schema(json_data, dataset_schema):
    success = None
    error_message = ""

    try:
        jsonschema.validate(json_data, schema=dataset_schema)
        success = True
    except jsonschema.ValidationError as e:
        error_message = f"error: {e.message}. offending element: {e.json_path}"
        print(error_message)
        success = False
    return success, error_message
