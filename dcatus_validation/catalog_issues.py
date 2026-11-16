"""Catalog-level issues the harvest runner reports beyond schema validation:
duplicate identifiers (errors, the duplicate is dropped) and content-quality
warnings. Used by the public validator so a catalog that validates cleanly
doesn't produce surprises once harvested (GSA/data.gov#6380).

This mirrors harvester/harvest.py: records come from the same split, datasets
and catalog records without an identifier are dropped, the duplicate check
runs per record type, dropped records get no further checks, DCAT-US 3.0
records get `detect_dcat_warnings`, and every schema-valid record type that
becomes a dataset gets the spatial transformation check.
"""

from collections import Counter
from typing import NamedTuple

from dcatus_validation.catalog import (
    DCATUS3_NON_DATASET_RECORD_TYPES,
    IDENTIFIER_REQUIRED_RECORD_TYPES,
    RECORD_TYPES_WITHOUT_DATASET_ROW,
    RESOLVED_IDENTIFIER_KEY,
    find_indexes_for_duplicates,
    normalize_dataset_identifier,
    record_identifier,
    split_dcatus3_catalog,
)
from dcatus_validation.content_warnings import detect_dcat_warnings
from dcatus_validation.deadline import check_deadline
from dcatus_validation.spatial import PlaceLookup, translate_spatial
from dcatus_validation.validate import record_validator

# Issue types as the harvester stores them: exception-based issues under the
# exception's class name (see harvester/harvest.py `_report_error`).
VALIDATION_ERROR_TYPE = "ValidationException"
DUPLICATE_IDENTIFIER_TYPE = "DuplicateIdentifierException"
SPATIAL_TRANSFORMATION_TYPE = "SpatialTransformationException"

# Record types the harvester turns into a dataset row, which is where its
# spatial transformation check runs.
_SPATIAL_CHECKED_TYPES = {
    "dataset",
    *DCATUS3_NON_DATASET_RECORD_TYPES,
} - RECORD_TYPES_WITHOUT_DATASET_ROW


def duplicate_identifier_message(identifier) -> str:
    return f"Duplicate identifier '{identifier}' found"


def spatial_transformation_message(spatial) -> str:
    return f"unable to spatially fix {spatial}"


class CatalogWarning(NamedTuple):
    identifier: str | int
    record_type: str
    warning_type: str
    message: str
    title: str | None = None


def _title(record: dict) -> str | None:
    title = record.get("title")
    return title if isinstance(title, str) else None


def _label(record: dict, identifier_field: str, position: int) -> str | int:
    """The record's identifier, or its position when it has none."""
    return record_identifier(record, identifier_field) or position


def _drop_missing_identifiers(
    records: list, record_type: str, identifier_field: str
) -> list:
    if record_type not in IDENTIFIER_REQUIRED_RECORD_TYPES:
        return records
    return [
        r
        for r in records
        if normalize_dataset_identifier(r.get(identifier_field)) is not None
    ]


def _drop_duplicates(records: list, identifier_field: str) -> tuple[list, list]:
    """Return (kept_records, duplicate_errors) the way the harvester filters
    them: later occurrences of an identifier are dropped."""
    dropped = set(find_indexes_for_duplicates(records, identifier_field))
    errors = []
    for idx in sorted(dropped):
        identifier = record_identifier(records[idx], identifier_field)
        errors.append([identifier or "", duplicate_identifier_message(identifier)])
    kept = [(idx, record) for idx, record in enumerate(records) if idx not in dropped]
    return kept, errors


def _is_schema_valid(record: dict, schema_name: str, record_type: str) -> bool:
    # The harvester validates the record without its resolved-identifier key.
    record = {k: v for k, v in record.items() if k != RESOLVED_IDENTIFIER_KEY}
    try:
        return record_validator(schema_name, record_type).is_valid(record)
    except RecursionError:
        return False


def _spatial_warning(
    record: dict,
    label,
    record_type: str,
    schema_name: str,
    place_lookup: PlaceLookup | None,
) -> CatalogWarning | None:
    spatial = record.get("spatial")
    if not spatial or translate_spatial(spatial, place_lookup):
        return None
    # The harvester only checks spatial while syncing, which skips records
    # that failed validation. Checked last since validating is the costly step.
    if not _is_schema_valid(record, schema_name, record_type):
        return None
    return CatalogWarning(
        label,
        record_type,
        SPATIAL_TRANSFORMATION_TYPE,
        spatial_transformation_message(spatial),
        _title(record),
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
        records = _drop_missing_identifiers(records, record_type, identifier_field)
        kept, errors = _drop_duplicates(records, identifier_field)
        duplicate_errors += errors
        # Duplicate errors are always complete; only warnings are capped.
        if warnings_incomplete:
            continue
        for position, record in kept:
            check_deadline()
            label = _label(record, identifier_field, position)
            record_warnings = []
            if is_dcatus3:
                record_warnings += [
                    CatalogWarning(
                        label, record_type, w.warning_type, w.message, _title(record)
                    )
                    for w in detect_dcat_warnings(record, place_lookup)
                ]
            if record_type in _SPATIAL_CHECKED_TYPES:
                spatial = _spatial_warning(
                    record, label, record_type, schema_name, place_lookup
                )
                if spatial is not None:
                    record_warnings.append(spatial)
            warnings += record_warnings
            if max_warnings is not None and len(warnings) > max_warnings:
                warnings = warnings[:max_warnings]
                warnings_incomplete = True
                break

    return duplicate_errors, warnings, warnings_incomplete


def summarize_issues(
    schema_error_count: int, duplicate_error_count: int, warnings: list
) -> list[dict]:
    """Rows like the job page's record issue type summary: severity, type and
    count, ordered by severity then type."""
    counts = Counter()
    if schema_error_count:
        counts["error", VALIDATION_ERROR_TYPE] = schema_error_count
    if duplicate_error_count:
        counts["error", DUPLICATE_IDENTIFIER_TYPE] = duplicate_error_count
    for warning in warnings:
        counts["warning", warning.warning_type] += 1
    return [
        {"severity": severity, "type": issue_type, "count": count}
        for (severity, issue_type), count in sorted(counts.items())
    ]
