import hashlib
import json

import sansjson


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    """Hashes json-dumped, key-sorted object with sha256"""
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()
