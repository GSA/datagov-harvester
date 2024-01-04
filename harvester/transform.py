import json
import logging
import os

import requests

logger = logging.getLogger("harvester")


def transform(transform_obj):
    """Transforms records"""
    logger.info("Hello from harvester.transform()")

    url = os.getenv("MDTRANSLATOR_URL")
    res = requests.post(url, transform_obj)

    if res.status_code == 200:
        data = json.loads(res.content.decode("utf-8"))
        transform_obj["transformed_data"] = data["writerOutput"]

    return transform_obj
