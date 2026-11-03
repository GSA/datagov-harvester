"""Catalog-level issues the harvest runner reports beyond schema validation:
duplicate identifiers (errors, the duplicate is dropped) and content-quality
warnings. Used by the public validator so a catalog that validates cleanly
doesn't produce surprises once harvested (GSA/data.gov#6380).

This mirrors harvester/harvest.py: records come from the same split, the
duplicate check runs per record type, dropped duplicates get no further
checks, DCAT-US 3.0 records get `detect_dcat_warnings`, and every record type
that becomes a dataset gets the spatial transformation check.
"""

from typing import NamedTuple

from dcatus_validation.catalog import (
    DCATUS3_NON_DATASET_RECORD_TYPES,
    find_indexes_for_duplicates,
    record_identifier,
    split_dcatus3_catalog,
)
from dcatus_validation.content_warnings import detect_dcat_warnings
from dcatus_validation.spatial import PlaceLookup, translate_spatial

# harvester.exceptions.SpatialTransformationException; the harvester stores
# exception-based issues under their class name.
SPATIAL_TRANSFORMATION_TYPE = "SpatialTransformationException"

# Record types the harvester turns into a dataset row, which is where its
# spatial transformation check runs (see RECORD_TYPES_WITHOUT_DATASET_ROW).
_SPATIAL_CHECKED_TYPES = {"dataset", "data_service", "data_series"}


class CatalogWarning(NamedTuple):
    identifier: str | int
    record_type: str
    warning_type: str
    message: str


def _label(record: dict, identifier_field: str, position: int) -> str | int:
    """The record's identifier, or its position when it has none."""
    return record_identifier(record, identifier_field) or position


def _drop_duplicates(records: list, identifier_field: str) -> tuple[list, list]:
    """Return (kept_records, duplicate_errors) the way the harvester filters
    them: later occurrences of an identifier are dropped."""
    dropped = set(find_indexes_for_duplicates(records, identifier_field))
    errors = []
    for idx in sorted(dropped):
        identifier = record_identifier(records[idx], identifier_field)
        errors.append([identifier or "", f"Duplicate identifier '{identifier}' found"])
    kept = [(idx, record) for idx, record in enumerate(records) if idx not in dropped]
    return kept, errors


def _spatial_warning(
    record: dict, label, record_type: str, place_lookup: PlaceLookup | None
) -> CatalogWarning | None:
    spatial = record.get("spatial")
    if not spatial or translate_spatial(spatial, place_lookup):
        return None
    return CatalogWarning(
        label,
        record_type,
        SPATIAL_TRANSFORMATION_TYPE,
        f"unable to spatially fix {spatial}",
    )


def _records_by_type(catalog: dict, schema_name: str) -> list[tuple[str, list, str]]:
    """(record_type, records, identifier_field) in the harvester's order."""
    if schema_name.startswith("dcatus1.1"):
        datasets = catalog.get("dataset")
        if not isinstance(datasets, list):
            return []
        return [("dataset", [r for r in datasets if isinstance(r, dict)], "identifier")]

    datasets, by_type = split_dcatus3_catalog(catalog)
    groups = [("dataset", datasets, "identifier")]
    for record_type, config in DCATUS3_NON_DATASET_RECORD_TYPES.items():
        groups.append(
            (record_type, by_type.get(record_type, []), config["identifier_field"])
        )
    return groups


def find_catalog_issues(
    catalog: dict,
    schema_name: str,
    place_lookup: PlaceLookup | None = None,
    max_warnings: int | None = None,
) -> tuple[list, list[CatalogWarning], bool]:
    """Return (duplicate_errors, warnings, warnings_incomplete).

    duplicate_errors are [identifier, message] pairs shaped like schema
    validation errors. Warnings stop at max_warnings; warnings_incomplete says
    whether more were found.
    """
    duplicate_errors = []
    warnings: list[CatalogWarning] = []
    warnings_incomplete = False
    is_dcatus3 = not schema_name.startswith("dcatus1.1")

    for record_type, records, identifier_field in _records_by_type(
        catalog, schema_name
    ):
        kept, errors = _drop_duplicates(records, identifier_field)
        duplicate_errors += errors
        # Duplicate errors are always complete; only warnings are capped.
        if warnings_incomplete:
            continue
        for position, record in kept:
            label = _label(record, identifier_field, position)
            if is_dcatus3:
                warnings += [
                    CatalogWarning(label, record_type, w.warning_type, w.message)
                    for w in detect_dcat_warnings(record, place_lookup)
                ]
            if record_type in _SPATIAL_CHECKED_TYPES:
                spatial = _spatial_warning(record, label, record_type, place_lookup)
                if spatial is not None:
                    warnings.append(spatial)
            if max_warnings is not None and len(warnings) > max_warnings:
                warnings = warnings[:max_warnings]
                warnings_incomplete = True
                break

    return duplicate_errors, warnings, warnings_incomplete
