import json
import logging
import os

import requests

logger = logging.getLogger("harvester")

def transform(transform_obj):
    """Transforms records"""
    dataset_title = transform_obj["title"]
    logger.info("Hello from harvester.transform()")
    logger.info(f"I received this dataset: {dataset_title}.")
    logger.info("Isn't life grand!")

    #
    # TODO
    # We will make an HTTP call to MDTranslator here 
    # using the source dataset as the payload 
    # and return the transformed result
    #

    url = os.getenv("MDTRANSLATOR_URL")
    res = requests.post(url, transform_obj)

    if res.status_code == 200:
        data = json.loads(res.content.decode("utf-8"))
        transform_obj["transformed_data"] = data["writerOutput"]

    return transform_obj
