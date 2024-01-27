import json
import hashlib
import sansjson

# ruff: noqa: F841


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


def open_json(file_path):
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    with open(file_path) as fp:
        return json.load(fp)
