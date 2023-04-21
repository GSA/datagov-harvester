import json

import jsonschema


def open_json(file_path):
    with open(file_path) as fp:
        return json.load(fp)


def validate_json_schema(json_data, dataset_schema, valid_flag):
    # valid_flag: invalid = False; valid = True
    try:
        jsonschema.validate(json_data, schema=dataset_schema)
        assert valid_flag
    except jsonschema.ValidationError as e:
        error_message = f"error: {e.message}. offending element: {e.json_path}"
        print(error_message)
        assert not valid_flag
