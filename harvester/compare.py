import logging

logger = logging.getLogger("harvester")


def compare(harvest_source, ckan_source):
    """Compares records"""
    logger.info("Hello from harvester.compare()")

    output = {
        "create": [],
        "update": [],
        "delete": [],
    }

    harvest_ids = set(harvest_source.keys())
    ckan_ids = set(ckan_source.keys())
    same_ids = harvest_ids & ckan_ids

    output["create"] += list(harvest_ids - ckan_ids)
    output["delete"] += list(ckan_ids - harvest_ids)
    output["update"] += [i for i in same_ids if harvest_source[i] != ckan_source[i]]

    return output
