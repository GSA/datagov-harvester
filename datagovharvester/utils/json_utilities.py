import json


def open_json(file_path):
    """ open input json file as dictionary
    file_path (str)     :   json file path.
    """
    try:
        with open(file_path) as fp:
            return json.load(fp)
    except Exception as e: #noqa 
        pass
