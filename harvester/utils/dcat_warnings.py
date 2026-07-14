"""DCAT-US 3 warning detection (GSA/data.gov#6127).

Warnings are semantic / content-quality signals that sit *above* schema
validation: they do not fail a record, they flag likely data-quality issues or
incomplete v1.1 -> v3 migrations. Each rule is a small pure function that takes
the offending value (and any sibling value a cross-field rule needs) and returns
a templated message, or None when the value is fine. The entry point
`detect_dcat_warnings` walks a dataset record, dispatches each typed object to
its class rules, and returns a flat list of message strings.

Rules follow the agreed spec: GSA/data.gov#6093 (comment 4799323747).
"""

import re
from collections import Counter
from datetime import date, datetime
from typing import Optional

from jsonschema import FormatChecker

from harvester.utils.general_utils import is_number, translate_spatial

_IRI_FORMAT_CHECKER = FormatChecker()

# xsd:duration / ISO 8601 duration. Requires something after P (and after T
# when a time part is present); accepts an optional leading sign and weeks.
_XSD_DURATION_RE = re.compile(
    r"^-?P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?!$)(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?$"
)

# Legacy DCAT-US 1.1 accessLevel terms that should have been migrated to a v3
# accessRights value.
_LEGACY_ACCESS_LEVEL_TERMS = {"restricted public", "non-public"}

# Country-name spellings we treat as "US" for postal-code validation.
_US_COUNTRY_NAMES = {
    "us",
    "usa",
    "u.s.",
    "u.s.a.",
    "united states",
    "united states of america",
}


def _is_valid_iri(value) -> bool:
    """True when value is a string that conforms to the IRI format."""
    return isinstance(value, str) and _IRI_FORMAT_CHECKER.conforms(value, "iri")


def _parse_dcat_date(value):
    """Parse a DCAT date/date-time/YYYY/YYYY-MM string to a date, else None."""
    if not isinstance(value, str):
        return None
    v = value.strip()
    try:
        if re.fullmatch(r"\d{4}", v):
            return date(int(v), 1, 1)
        if re.fullmatch(r"\d{4}-\d{2}", v):
            year, month = v.split("-")
            return date(int(year), int(month), 1)
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _date_later_than(value, other) -> bool:
    """True when both values parse to dates and value is strictly after other."""
    parsed_value = _parse_dcat_date(value)
    parsed_other = _parse_dcat_date(other)
    if parsed_value is None or parsed_other is None:
        return False
    return parsed_value > parsed_other


def _warn_iri(field: str, value) -> Optional[str]:
    """Warn if a single field value is not a valid IRI."""
    if value is None or _is_valid_iri(value):
        return None
    return f'`{field}` value "{value}" is not a valid IRI.'


def _warn_iris(field: str, values) -> list:
    """Warn for each string entry in an array-valued field that is not an IRI.

    Non-string entries (e.g. Standard objects on `conformsTo`) are skipped here;
    their `@id` is covered by the universal object walk.
    """
    if not isinstance(values, list):
        return []
    warnings = []
    for value in values:
        if isinstance(value, str) and not _is_valid_iri(value):
            warnings.append(f'`{field}` value "{value}" is not a valid IRI.')
    return warnings


def _warn_duplicate_keywords(keywords) -> list:
    """Warn once per keyword that appears more than once."""
    if not isinstance(keywords, list):
        return []
    counts = Counter(k for k in keywords if isinstance(k, str))
    return [
        f'Duplicate keyword detected: "{value}". Keywords should be unique.'
        for value, count in counts.items()
        if count > 1
    ]


def _warn_spatial_resolution(value) -> Optional[str]:
    """Warn if spatialResolutionInMeters is non-numeric or not greater than zero."""
    if value is None:
        return None
    if not is_number(value):
        return (
            f'`spatialResolutionInMeters` value "{value}" '
            "does not appear to be a valid number."
        )
    if float(value) <= 0:
        return f'`spatialResolutionInMeters` value "{value}" must be greater than zero.'
    return None


def _warn_iso8601_duration(value) -> Optional[str]:
    """Warn if a Dataset temporalResolution is not an ISO 8601 duration."""
    if value is None or (isinstance(value, str) and _XSD_DURATION_RE.match(value)):
        return None
    return (
        f'`temporalResolution` value "{value}" does not appear to be a valid '
        'ISO 8601 duration (e.g., "P1Y", "P6M", "P1D").'
    )


def _warn_xsd_duration(value) -> Optional[str]:
    """Warn if a Distribution/DataService temporalResolution is not xsd:duration."""
    if value is None or (isinstance(value, str) and _XSD_DURATION_RE.match(value)):
        return None
    return (
        f'`temporalResolution` value "{value}" '
        "does not appear to be a valid xsd:duration."
    )


def _warn_byte_size(value) -> Optional[str]:
    """Warn if byteSize does not parse to a number."""
    if value is None or is_number(value):
        return None
    return f'`byteSize` value "{value}" does not appear to be a valid number.'


def _warn_legacy_access_rights(value) -> Optional[str]:
    """Warn if accessRights is a legacy v1.1 accessLevel term."""
    if isinstance(value, str) and value.strip().lower() in _LEGACY_ACCESS_LEVEL_TERMS:
        return (
            f'`accessRights` value "{value}" appears to be a legacy DCAT-US 1.1 '
            "`accessLevel` term. Migrate to an appropriate DCAT-US 3 access "
            "rights value."
        )
    return None


def _warn_created_after(created, modified, issued) -> list:
    """Warn if created is later than modified or issued, where each is present."""
    warnings = []
    for other_field, other_value in (("modified", modified), ("issued", issued)):
        if other_value is not None and _date_later_than(created, other_value):
            warnings.append(
                f'`created` value "{created}" is later than `{other_field}` value '
                f'"{other_value}". A dataset cannot be created after it was '
                "modified or issued."
            )
    return warnings


def _warn_issued_after_modified(issued, modified) -> Optional[str]:
    """Warn if issued is later than modified, where modified is present."""
    if modified is not None and _date_later_than(issued, modified):
        return (
            f'`issued` value "{issued}" is later than `modified` value '
            f'"{modified}". A dataset cannot be issued after it was last modified.'
        )
    return None


def _warn_period_start_after_end(start, end) -> Optional[str]:
    """Warn if a PeriodOfTime startDate is later than its endDate."""
    if start is not None and end is not None and _date_later_than(start, end):
        return (
            f'`startDate` value "{start}" is later than `endDate` value "{end}". '
            "The start of a time period cannot be after its end."
        )
    return None


def _warn_tel_has_letters(value) -> Optional[str]:
    """Warn if a tel value contains letters."""
    if isinstance(value, str) and re.search(r"[A-Za-z]", value):
        return (
            f'`tel` value "{value}" contains letters and may not be a valid '
            "telephone number."
        )
    return None


def _warn_expected_data_type(value) -> list:
    """Warn for each expectedDataType value that does not begin with `xsd:`."""
    values = value if isinstance(value, list) else [value]
    warnings = []
    for entry in values:
        if isinstance(entry, str) and not entry.startswith("xsd:"):
            warnings.append(
                f'`expectedDataType` value "{entry}" does not appear to be a valid '
                'XSD type. Expected format begins with "xsd:" (e.g., "xsd:string", '
                '"xsd:decimal").'
            )
    return warnings


def _warn_spatial_unresolved(field: str, value) -> Optional[str]:
    """Warn if a spatial value cannot be resolved to a geographic area."""
    if value is None:
        return None
    if translate_spatial(value):
        return None
    return (
        f'spatial value "{value}" could not be resolved to a recognized '
        "geographic area."
    )


def _warn_us_postal_code(postal_code, country_name) -> Optional[str]:
    """Warn if a US address postal code contains letters."""
    is_us = (
        isinstance(country_name, str)
        and country_name.strip().lower() in _US_COUNTRY_NAMES
    )
    if is_us and isinstance(postal_code, str) and re.search(r"[A-Za-z]", postal_code):
        return (
            f'Postal code "{postal_code}" contains letters, which is not valid '
            "for a US address."
        )
    return None


def _warn_empty_address(address: dict) -> Optional[str]:
    """Warn if an Address object has no populated fields."""
    fields = (
        "street-address",
        "locality",
        "region",
        "postal-code",
        "country-name",
    )
    if all(not (address.get(field) or "") for field in fields):
        return "Address object has no populated fields."
    return None


def _warn_cui_banner_marking(value) -> Optional[str]:
    """Warn if a CUI banner marking does not follow the expected format."""
    if isinstance(value, str) and not value.startswith("CUI//"):
        return (
            f'`cuiBannerMarking` value "{value}" does not appear to follow the CUI '
            'banner marking format. Expected format begins with "CUI//".'
        )
    return None


def _append(warnings: list, result) -> None:
    """Append a single optional message or extend with a list of messages."""
    if result is None:
        return
    if isinstance(result, list):
        warnings.extend(result)
    else:
        warnings.append(result)


def _dataset_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_duplicate_keywords(obj.get("keyword")))
    _append(warnings, _warn_spatial_resolution(obj.get("spatialResolutionInMeters")))
    _append(warnings, _warn_iso8601_duration(obj.get("temporalResolution")))
    _append(warnings, _warn_legacy_access_rights(obj.get("accessRights")))
    _append(
        warnings,
        _warn_created_after(obj.get("created"), obj.get("modified"), obj.get("issued")),
    )
    _append(
        warnings,
        _warn_issued_after_modified(obj.get("issued"), obj.get("modified")),
    )
    _append(warnings, _warn_iris("relation", obj.get("relation")))
    _append(warnings, _warn_iri("image", obj.get("image")))
    _append(warnings, _warn_iris("isReferencedBy", obj.get("isReferencedBy")))
    _append(warnings, _warn_iris("conformsTo", obj.get("conformsTo")))
    return warnings


def _distribution_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_byte_size(obj.get("byteSize")))
    _append(warnings, _warn_spatial_resolution(obj.get("spatialResolutionInMeters")))
    _append(warnings, _warn_xsd_duration(obj.get("temporalResolution")))
    _append(warnings, _warn_iris("conformsTo", obj.get("conformsTo")))
    return warnings


def _dataservice_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_spatial_resolution(obj.get("spatialResolutionInMeters")))
    _append(warnings, _warn_xsd_duration(obj.get("temporalResolution")))
    _append(warnings, _warn_iris("conformsTo", obj.get("conformsTo")))
    return warnings


def _kind_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_tel_has_letters(obj.get("tel")))
    return warnings


def _location_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_spatial_unresolved("bbox", obj.get("bbox")))
    _append(warnings, _warn_spatial_unresolved("geometry", obj.get("geometry")))
    return warnings


def _metric_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_expected_data_type(obj.get("expectedDataType")))
    return warnings


def _period_of_time_warnings(obj: dict) -> list:
    warnings = []
    _append(
        warnings,
        _warn_period_start_after_end(obj.get("startDate"), obj.get("endDate")),
    )
    return warnings


def _address_warnings(obj: dict) -> list:
    warnings = []
    _append(
        warnings,
        _warn_us_postal_code(obj.get("postal-code"), obj.get("country-name")),
    )
    _append(warnings, _warn_empty_address(obj))
    return warnings


def _cui_restriction_warnings(obj: dict) -> list:
    warnings = []
    _append(warnings, _warn_cui_banner_marking(obj.get("cuiBannerMarking")))
    return warnings


# Maps a DCAT-US 3 @type to the rule function for that class. Adding a class is
# a one-line addition here plus its rule function above.
_TYPE_RULES = {
    "Dataset": _dataset_warnings,
    "Distribution": _distribution_warnings,
    "DataService": _dataservice_warnings,
    "Kind": _kind_warnings,
    "Location": _location_warnings,
    "Metric": _metric_warnings,
    "PeriodOfTime": _period_of_time_warnings,
    "Address": _address_warnings,
    "CUIRestriction": _cui_restriction_warnings,
}


def _walk_objects(node):
    """Yield every dict nested anywhere within a record (depth-first)."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk_objects(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_objects(item)


def detect_dcat_warnings(data: dict) -> list:
    """Detect DCAT-US 3 content-quality warnings in a dataset record.

    Walks the record, runs the universal `@id` IRI check on every object, and
    dispatches each typed object to its class rules. Returns a flat list of
    warning message strings (empty when the record is clean).
    """
    warnings = []
    for obj in _walk_objects(data):
        _append(warnings, _warn_iri("@id", obj.get("@id")))
        rule = _TYPE_RULES.get(obj.get("@type"))
        if rule is not None:
            _append(warnings, rule(obj))
    return warnings
