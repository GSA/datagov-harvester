import json


def open_json(file_path):
    json_data = None
    error_message = None

    try:
        with open(file_path) as fp:
            json_data = json.load(fp)
    except Exception as e:
        error_message = e

    return json_data, error_message
