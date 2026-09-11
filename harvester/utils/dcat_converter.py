"""
Wrapper for DCAT 1.1 to 3.0 conversion.

This module provides a clean interface to the conversion logic from
_external/dcat-us/jsonschema with minimal inlined transforms to avoid
external dependencies.
"""

import copy
import logging
from datetime import datetime, timezone

logger = logging.getLogger("harvest_runner")

# Access rights mapping
ACCESS_RIGHTS_BY_LEVEL = {
    "public": "public",
    "restricted public": "Access restricted. Contact the publisher to request access.",
    "non-public": "Not available for public release. Contact the publisher for more information.",
}


def _propagate_license(dataset: dict) -> dict:
    """Copy dataset-level license down to each Distribution without one."""
    license_value = dataset.get("license")
    if not license_value:
        return dataset

    distributions = dataset.get("distribution")
    if not distributions:
        return dataset

    new_dataset = copy.deepcopy(dataset)
    for dist in new_dataset["distribution"]:
        if isinstance(dist, dict) and "license" not in dist:
            dist["license"] = license_value

    return new_dataset


def _transform_access_rights(dataset: dict) -> dict:
    """Add accessRights based on accessLevel."""
    if "accessRights" in dataset:
        return dataset

    access_level = dataset.get("accessLevel")
    if access_level not in ACCESS_RIGHTS_BY_LEVEL:
        return dataset

    new_dataset = copy.deepcopy(dataset)
    new_dataset["accessRights"] = ACCESS_RIGHTS_BY_LEVEL[access_level]
    return new_dataset


def convert_dcat_catalog(old_catalog: dict) -> dict:
    """Convert DCAT-US v1.1 catalog to DCAT-US v3.0 catalog.

    This is a streamlined version of the converter that applies
    the essential transformations needed for DCAT 3.0 compliance.

    Args:
        old_catalog: A DCAT-US 1.1 catalog dictionary

    Returns:
        A DCAT-US 3.0 catalog dictionary

    Raises:
        Exception: If conversion fails for any dataset
    """
    new_catalog = copy.deepcopy(old_catalog)

    # conformsTo on the Catalog
    new_catalog["conformsTo"] = {
        "@type": "Standard",
        "title": "DCAT-US 3.0",
        "identifier": "https://resources.data.gov/dcat-us/3.0.0",
    }

    # remove @context and describedBy from the Catalog
    new_catalog.pop("@context", None)
    new_catalog.pop("describedBy", None)

    # The catalog itself may have a `modified` timestamp so we normalize it to a
    # timezone-aware date-time string, since v3.0 requires one.
    catalog_modified = new_catalog.get("modified")
    if isinstance(catalog_modified, str):
        try:
            parsed = datetime.fromisoformat(catalog_modified)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            new_catalog["modified"] = parsed.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except ValueError:
            del new_catalog["modified"]

    datasets = new_catalog.get("dataset", [])
    logger.debug(f"Transforming {len(datasets)} datasets to DCAT 3.0")

    for i, dataset in enumerate(datasets):
        identifier = dataset.get("identifier", f"index {i}")
        try:
            # Apply essential transformations
            dataset = _transform_access_rights(dataset)
            dataset = _propagate_license(dataset)
            datasets[i] = dataset
        except Exception as e:
            raise Exception(f"Failed to convert dataset {identifier}: {e}") from e

    return new_catalog
