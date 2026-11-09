"""DCAT-US catalog record extraction, identifier resolution, and duplicate
detection shared by the harvest runner and the public validator."""

import hashlib
import re

PACKAGE_NAME_MAX_LENGTH = 90


PACKAGE_NAME_MIN_LENGTH = 2


def _extract_dcatus3_catalog_objects(catalog: dict, field: str) -> list:
    """
    recursively collect every entry of [field] ("dataset" or "service") from a
    DCAT-US3 Catalog dict, including entries nested arbitrarily deep within
    its "catalog" (sub-catalog) field.
    """
    objects = list(catalog.get(field) or [])

    for sub_catalog in catalog.get("catalog") or []:
        objects.extend(_extract_dcatus3_catalog_objects(sub_catalog, field))

    return objects


def extract_dcatus3_catalog_datasets(catalog: dict) -> list:
    """
    recursively collect every dataset from a DCAT-US3 Catalog dict, including
    datasets nested arbitrarily deep within its "catalog" (sub-catalog) field.
    """
    return _extract_dcatus3_catalog_objects(catalog, "dataset")


def extract_dcatus3_catalog_services(catalog: dict) -> list:
    """
    recursively collect every DataService from a DCAT-US3 Catalog dict,
    including services nested arbitrarily deep within its "catalog"
    (sub-catalog) field.
    """
    return _extract_dcatus3_catalog_objects(catalog, "service")


def extract_dcatus3_catalog_records(catalog: dict) -> list:
    """
    recursively collect every CatalogRecord from a DCAT-US3 Catalog dict,
    including records nested arbitrarily deep within its "catalog"
    (sub-catalog) field.
    """
    return _extract_dcatus3_catalog_objects(catalog, "record")


def backfill_catalog_record_identifiers(records: list) -> list:
    """
    CatalogRecord's @id is optional per the DCAT-US3.0 schema. Give each
    @id-less record a stable @id synthesized from its two required fields
    (modified, primaryTopic) so harvester can still track it.
    """
    backfilled = []
    for record in records:
        record = dict(record)
        if normalize_dataset_identifier(record.get("@id")) is None:
            basis = f"{record.get('primaryTopic')}|{record.get('modified')}"
            digest = hashlib.sha256(basis.encode()).hexdigest()
            record["@id"] = f"urn:datagov:catalogrecord:{digest}"
        backfilled.append(record)
    return backfilled


RESOLVED_IDENTIFIER_KEY = "_resolved_identifier"


def resolve_object_identifier(records: list) -> list:
    """
    DataService's "identifier" and DatasetSeries's "@id" are both optional
    per the DCAT-US3.0 schema. Resolve each record's own identifier --
    preferring "identifier", then "@id", then a slug munged from its title
    (required by both schemas) -- so harvester can still track the object
    and link its child datasets to it.

    This is for harvester's own bookkeeping only: the resolved value is
    stashed under RESOLVED_IDENTIFIER_KEY, never written into "identifier"
    or "@id" themselves. The record we validate, persist, and index must
    stay exactly what the provider sent us -- see GSA/data.gov#6431.
    """
    resolved_records = []
    for record in records:
        record = dict(record)
        resolved = (
            normalize_dataset_identifier(record.get("identifier"))
            or normalize_dataset_identifier(record.get("@id"))
            or munge_title_to_name(str(record.get("title")))
        )
        record[RESOLVED_IDENTIFIER_KEY] = resolved
        resolved_records.append(record)
    return resolved_records


def extract_dcatus3_catalog_dataset_series(catalog: dict) -> list:
    """
    recursively collect every DatasetSeries from a DCAT-US3 Catalog dict,
    including series nested arbitrarily deep within its "catalog"
    (sub-catalog) field.
    """
    return _extract_dcatus3_catalog_objects(catalog, "datasetSeries")


def extract_dcatus3_nested_datasets(
    parents: list, *fields: str, parent_identifier_field: str = "identifier"
) -> list:
    """
    Pull full inline Dataset objects out of [fields] on each dict in
    [parents] (e.g. DataService.servesDataset, DatasetSeries.seriesMember/
    first/last), tagging each with "parent_identifier". parent_identifier_
    field selects which key on the parent holds its own identifier, since
    DatasetSeries and CatalogRecord use "@id" instead of "identifier".

    If a parent carries RESOLVED_IDENTIFIER_KEY (see resolve_object_identifier),
    that bookkeeping value is used instead of parent_identifier_field directly,
    so a child's parent_identifier always matches the parent's own tracked
    identifier even when the parent's "identifier"/"@id" is missing.

    The same dataset can appear in more than one of [fields] on the same
    parent (e.g. a series's "first" usually also appears in "seriesMember").
    That's redundant source data, so each parent contributes one copy per
    distinct identifier.
    """
    nested_datasets = []

    for parent in parents:
        parent_identifier = normalize_dataset_identifier(
            parent.get(RESOLVED_IDENTIFIER_KEY)
        ) or normalize_dataset_identifier(parent.get(parent_identifier_field))
        seen_identifiers = set()
        for field in fields:
            value = parent.get(field)
            if value is None:
                continue
            candidates = value if isinstance(value, list) else [value]
            for dataset in candidates:
                dataset_identifier = normalize_dataset_identifier(
                    dataset.get("identifier")
                )
                if dataset_identifier is not None:
                    if dataset_identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(dataset_identifier)

                dataset = dict(dataset)
                dataset["parent_identifier"] = parent_identifier
                nested_datasets.append(dataset)

    return nested_datasets


def merge_dcatus3_datasets(top_level: list, *nested_lists: list) -> list:
    """
    Merge top-level catalog.dataset entries with nested ones (DataService.
    servesDataset, DatasetSeries.seriesMember/first/last). A nested dataset
    matching a top-level identifier keeps the top-level copy and just adds
    its parent_identifier. Nested-vs-nested overlaps stay separate, for
    filter_duplicate_identifiers to catch.
    """
    merged = []
    top_level_by_identifier = {}

    for dataset in top_level:
        dataset = dict(dataset)
        merged.append(dataset)
        identifier = normalize_dataset_identifier(dataset.get("identifier"))
        if identifier is not None:
            top_level_by_identifier[identifier] = dataset

    for nested in nested_lists:
        for dataset in nested:
            identifier = normalize_dataset_identifier(dataset.get("identifier"))
            existing = (
                top_level_by_identifier.get(identifier)
                if identifier is not None
                else None
            )
            if existing is not None:
                existing.setdefault(
                    "parent_identifier", dataset.get("parent_identifier")
                )
                continue

            merged.append(dict(dataset))

    return merged


def normalize_dataset_identifier(identifier) -> str | None:
    """Return a canonical string identifier from a DCAT-US identifier value.

    DCAT-US 3.0 allows identifier as a string or an Identifier object. For
    harvesting, object identifiers must provide @id.
    """
    if identifier is None:
        return None
    if isinstance(identifier, str):
        return identifier if identifier.strip() else None
    if isinstance(identifier, dict):
        value = identifier.get("@id")
        if isinstance(value, str) and value.strip():
            return value
    return None


def describe_identifier_error(identifier, field: str = "identifier") -> str:
    """Describe why an identifier cannot be used for harvesting."""
    if identifier is None:
        return f"is missing '{field}' field"
    if isinstance(identifier, str) and not identifier.strip():
        return f"is missing '{field}' field"
    if isinstance(identifier, dict):
        return f"has an object '{field}' with no usable '@id' field"
    return f"has an invalid '{field}' field"


def find_indexes_for_duplicates(records: list, identifier_field: str = "identifier"):
    """
    output is a list of integers representing element positions of
    duplicates records. this list is then used to record duplicate
    record errors in the db and to remove from self.external_records.
    sorting it in reverse (.sort edits in place) places the largest
    numbers first. this is necessary to avoid index shifting
    when you're deleting from a list.

    identifier_field selects which key holds the record's identifier value.
    CatalogRecord objects have no "identifier" field, only a top-level "@id".

    scenario without sorting output
        positions = [ 1, 3 ]
        data = [ 0, 0, 1, 1 ]

        deleting data[1] can shift the data since we're mutating the list.
        what happens when we try to del data[3] on [0, 1, 1]?
    """
    seen = set()
    output = []
    for i in range(len(records)):
        identifier = record_identifier(records[i], identifier_field)
        if identifier in seen:
            output.append(i)
        seen.add(identifier)
    output.sort(reverse=True)  # avoid index shifting

    return output


def _munge_to_length(string: str, min_length: int, max_length: int) -> str:
    """Pad or truncate a string to ensure it fits within the provided bounds."""

    if len(string) < min_length:
        string += "_" * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string


def substitute_ascii_equivalents(text_unicode: str) -> str:
    """
    Replace Latin-1 characters with ASCII equivalents when possible.
    This is copied from CKAN's ckan.lib.munge but made more efficient.
    https://github.com/ckan/ckan/blob/ckan-2.11.4/ckan/lib/munge.py#L66
    """

    char_mapping = {
        0xC0: "A",
        0xC1: "A",
        0xC2: "A",
        0xC3: "A",
        0xC4: "A",
        0xC5: "A",
        0xC6: "Ae",
        0xC7: "C",
        0xC8: "E",
        0xC9: "E",
        0xCA: "E",
        0xCB: "E",
        0xCC: "I",
        0xCD: "I",
        0xCE: "I",
        0xCF: "I",
        0xD0: "Th",
        0xD1: "N",
        0xD2: "O",
        0xD3: "O",
        0xD4: "O",
        0xD5: "O",
        0xD6: "O",
        0xD8: "O",
        0xD9: "U",
        0xDA: "U",
        0xDB: "U",
        0xDC: "U",
        0xDD: "Y",
        0xDE: "th",
        0xDF: "ss",
        0xE0: "a",
        0xE1: "a",
        0xE2: "a",
        0xE3: "a",
        0xE4: "a",
        0xE5: "a",
        0xE6: "ae",
        0xE7: "c",
        0xE8: "e",
        0xE9: "e",
        0xEA: "e",
        0xEB: "e",
        0xEC: "i",
        0xED: "i",
        0xEE: "i",
        0xEF: "i",
        0xF0: "th",
        0xF1: "n",
        0xF2: "o",
        0xF3: "o",
        0xF4: "o",
        0xF5: "o",
        0xF6: "o",
        0xF8: "o",
        0xF9: "u",
        0xFA: "u",
        0xFB: "u",
        0xFC: "u",
        0xFD: "y",
        0xFE: "th",
        0xFF: "y",
    }

    result = []
    for char in text_unicode:
        code_point = ord(char)
        if code_point in char_mapping:
            result.append(char_mapping[code_point])
        elif code_point >= 0x80:
            continue
        else:
            result.append(char)
    return "".join(result)


def munge_title_to_name(name: str) -> str:
    """Convert a dataset title into a slug-friendly package name."""

    name = substitute_ascii_equivalents(name)
    name = re.sub("[ .:/]", "-", name)
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    name = re.sub("-+", "-", name).strip("-")

    if len(name) > PACKAGE_NAME_MAX_LENGTH:
        year_match = re.match(r".*?[_-]((?:\d{2,4}[-/])?\d{2,4})$", name)
        if year_match:
            year = year_match.group(1)
            name = f"{name[: (PACKAGE_NAME_MAX_LENGTH - len(year) - 1)]}-{year}"
        else:
            name = name[:PACKAGE_NAME_MAX_LENGTH]

    return _munge_to_length(name, PACKAGE_NAME_MIN_LENGTH, PACKAGE_NAME_MAX_LENGTH)


# DCAT-US 3.0 record types other than "dataset". "identifier_field" is the key
# holding the record's own identifier: CatalogRecord and DatasetSeries have no
# "identifier" field, only a top-level "@id". "schema_ref" is the dcatus3.0
# definition each type is validated against.
DCATUS3_NON_DATASET_RECORD_TYPES = {
    "data_service": {
        "extractor": extract_dcatus3_catalog_services,
        "identifier_field": "identifier",
        "schema_ref": "dataservice",
    },
    "catalog_record": {
        "extractor": extract_dcatus3_catalog_records,
        "identifier_field": "@id",
        "schema_ref": "catalogrecord",
    },
    "data_series": {
        "extractor": extract_dcatus3_catalog_dataset_series,
        "identifier_field": "@id",
        "schema_ref": "datasetseries",
    },
}


# Record types persisted only as a HarvestRecord, no Dataset row. Excludes
# "data_series" and "data_service" on purpose: see Dataset.type in
# database/models.py.
RECORD_TYPES_WITHOUT_DATASET_ROW = {"catalog_record"}

# Record types the harvester drops when their identifier field is missing. The
# other types get an identifier resolved from the title instead.
IDENTIFIER_REQUIRED_RECORD_TYPES = {"dataset", "catalog_record"}


def split_dcatus3_catalog(catalog: dict) -> tuple[list, dict]:
    """Split a DCAT-US 3.0 Catalog into (datasets, records_by_type), exactly as
    the harvest runner processes it: every non-dataset record type, plus every
    dataset, including inline datasets nested in data services and series."""
    records_by_type = {
        record_type: config["extractor"](catalog)
        for record_type, config in DCATUS3_NON_DATASET_RECORD_TYPES.items()
    }
    records_by_type["catalog_record"] = backfill_catalog_record_identifiers(
        records_by_type["catalog_record"]
    )
    records_by_type["data_service"] = resolve_object_identifier(
        records_by_type["data_service"]
    )
    records_by_type["data_series"] = resolve_object_identifier(
        records_by_type["data_series"]
    )
    datasets = merge_dcatus3_datasets(
        extract_dcatus3_catalog_datasets(catalog),
        extract_dcatus3_nested_datasets(
            records_by_type["data_service"],
            "servesDataset",
            parent_identifier_field=DCATUS3_NON_DATASET_RECORD_TYPES["data_service"][
                "identifier_field"
            ],
        ),
        extract_dcatus3_nested_datasets(
            records_by_type["data_series"],
            "seriesMember",
            "first",
            "last",
            parent_identifier_field=DCATUS3_NON_DATASET_RECORD_TYPES["data_series"][
                "identifier_field"
            ],
        ),
    )
    return datasets, records_by_type


def record_identifier(record: dict, identifier_field: str = "identifier"):
    """The identifier the harvest runner tracks a record by."""
    return normalize_dataset_identifier(
        record.get(RESOLVED_IDENTIFIER_KEY)
    ) or normalize_dataset_identifier(record.get(identifier_field))
