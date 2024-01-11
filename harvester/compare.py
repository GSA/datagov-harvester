import logging

from harvester.harvest import Source

logger = logging.getLogger("harvester")


def compare(harvest_source: Source, ckan_source: Source) -> dict:
    """Compares records of Source objects, returns output dict"""
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

    for r_id in output["create"]:
        harvest_source.records[r_id].operation = "create"
    for r_id in output["delete"]:
        harvest_source.records[r_id] = ckan_source.records[r_id]
        harvest_source.records[r_id].operation = "delete"
    for r_id in output["update"]:
        harvest_source.records[r_id].operation = "update"

    return output
