import jsonschema


def validate_json_schema(json_data, dataset_schema, valid_flag):
    # valid_flag: invalid = False; valid = True
    output = valid_flag
    try:
        jsonschema.validate(json_data, schema=dataset_schema)
    except jsonschema.ValidationError as e:
        error_message = f"error: {e.message}. offending element: {e.json_path}"
        print(error_message)
        output = not valid_flag
    return output
