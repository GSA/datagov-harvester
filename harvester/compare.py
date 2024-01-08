import logging

from harvester.harvest import Source

logger = logging.getLogger("harvester")


def compare(harvest_source: Source, ckan_source: Source):
    """Compares records"""
    # TODO better logging
    logger.info(f"Comparing harvest source: {harvest_source} to ckan's: {ckan_source}.")

    output = {
        "create": [],
        "update": [],
        "delete": [],
    }

    harvest_ids = set(harvest_source.records.keys())
    ckan_ids = set(ckan_source.records.keys())
    same_ids = harvest_ids & ckan_ids

    output["create"] += list(harvest_ids - ckan_ids)
    output["delete"] += list(ckan_ids - harvest_ids)
    output["update"] += [
        i
        for i in same_ids
        if harvest_source.records[i].raw_hash != ckan_source.records[i].raw_hash
    ]

    for r in output["create"]:
        r

    return output
