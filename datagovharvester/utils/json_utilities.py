import json


def open_json(file_path):
    with open(file_path) as fp:
        return json.load(fp)
