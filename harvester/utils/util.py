import hashlib
import json

import sansjson


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()
