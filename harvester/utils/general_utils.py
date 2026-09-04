import argparse
import ast
import hashlib
import http
import json
import logging
import mimetypes
import os
import re
import smtplib
import time
import uuid
import warnings
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, Set, Union
from urllib.parse import urljoin
from uuid import UUID

import geojson_validator
import requests
import shapely.wkt
from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry
from referencing.jsonschema import DRAFT202012
from shapely.geometry import mapping as shapely_geom_mapping

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

FREQUENCY_ENUM = {"daily": 1, "weekly": 7, "biweekly": 14, "monthly": 30}

# User-Agent header for all HTTP requests
USER_AGENT = "HarvesterBot/0.0 (https://data.gov; datagovhelp@gsa.gov) Data.gov/2.0"

DT_PLACEHOLDER = datetime.now()

PACKAGE_NAME_MAX_LENGTH = 90
PACKAGE_NAME_MIN_LENGTH = 2

SMTP_CONFIG = {
    "server": os.getenv("HARVEST_SMTP_SERVER"),
    "port": 587,
    "use_tls": os.getenv("HARVEST_SMTP_STARTTLS", "true").lower() == "true",
    "username": os.getenv("HARVEST_SMTP_USER"),
    "password": os.getenv("HARVEST_SMTP_PASSWORD"),
    "default_sender": os.getenv("HARVEST_SMTP_SENDER"),
    "base_url": os.getenv("REDIRECT_URI").rsplit("/", 1)[0],
    "recipient": os.getenv("HARVEST_SMTP_RECIPIENT"),
}

CATALOG_BASE_URL = os.getenv("CATALOG_BASE_URL") or ""

RESOURCE_MAPPING = {
    # ArcGIS File Types
    "esri rest": ("Esri REST", "Esri REST API Endpoint"),
    "arcgis_rest": ("Esri REST", "Esri REST API Endpoint"),
    "web map application": ("ArcGIS Online Map", "ArcGIS Online Map"),
    "arcgis map preview": ("ArcGIS Map Preview", "ArcGIS Map Preview"),
    "arcgis map service": ("ArcGIS Map Service", "ArcGIS Map Service"),
    "wms": ("WMS", "ArcGIS Web Mapping Service"),
    "wfs": ("WFS", "ArcGIS Web Feature Service"),
    "wcs": ("WCS", "Web Coverage Service"),
    # CSS File Types
    "css": ("CSS", "Cascading Style Sheet File"),
    "text/css": ("CSS", "Cascading Style Sheet File"),
    # CSV File Types
    "csv": ("CSV", "Comma Separated Values File"),
    "text/csv": ("CSV", "Comma Separated Values File"),
    # EXE File Types
    "exe": ("EXE", "Windows Executable Program"),
    "application/x-msdos-program": ("EXE", "Windows Executable Program"),
    # HyperText Markup Language (HTML) File Types
    "htx": ("HTML", "Web Page"),
    "htm": ("HTML", "Web Page"),
    "html": ("HTML", "Web Page"),
    "htmls": ("HTML", "Web Page"),
    "xhtml": ("HTML", "Web Page"),
    "text/html": ("HTML", "Web Page"),
    "application/xhtml+xml": ("HTML", "Web Page"),
    "application/x-httpd-php": ("HTML", "Web Page"),
    # Image File Types - BITMAP
    "bm": ("BMP", "Bitmap Image File"),
    "bmp": ("BMP", "Bitmap Image File"),
    "pbm": ("BMP", "Bitmap Image File"),
    "xbm": ("BMP", "Bitmap Image File"),
    "image/bmp": ("BMP", "Bitmap Image File"),
    "image/x-ms-bmp": ("BMP", "Bitmap Image File"),
    "image/x-xbitmap": ("BMP", "Bitmap Image File"),
    "image/x-windows-bmp": ("BMP", "Bitmap Image File"),
    "image/x-portable-bitmap": ("BMP", "Bitmap Image File"),
    # Image File Types - Graphics Interchange Format (GIF)
    "gif": ("GIF", "GIF Image File"),
    "image/gif": ("GIF", "GIF Image File"),
    # Image File Types - ICON
    "ico": ("ICO", "Icon Image File"),
    "image/x-icon": ("ICO", "Icon Image File"),
    # Image File Types - JPEG
    "jpe": ("JPEG", "JPEG Image File"),
    "jpg": ("JPEG", "JPEG Image File"),
    "jps": ("JPEG", "JPEG Image File"),
    "jpeg": ("JPEG", "JPEG Image File"),
    "pjpeg": ("JPEG", "JPEG Image File"),
    "image/jpeg": ("JPEG", "JPEG Image File"),
    "image/pjpeg": ("JPEG", "JPEG Image File"),
    "image/x-jps": ("JPEG", "JPEG Image File"),
    "image/x-citrix-jpeg": ("JPEG", "JPEG Image File"),
    # Image File Types - PNG
    "png": ("PNG", "PNG Image File"),
    "x-png": ("PNG", "PNG Image File"),
    "image/png": ("PNG", "PNG Image File"),
    "image/x-citrix-png": ("PNG", "PNG Image File"),
    # Image File Types - Scalable Vector Graphics (SVG)
    "svg": ("SVG", "SVG Image File"),
    "image/svg+xml": ("SVG", "SVG Image File"),
    # Image File Types - Tagged Image File Format (TIFF)
    "tif": ("TIFF", "TIFF Image File"),
    "tiff": ("TIFF", "TIFF Image File"),
    "image/tiff": ("TIFF", "TIFF Image File"),
    "image/x-tiff": ("TIFF", "TIFF Image File"),
    # JSON File Types
    "json": ("JSON", "JSON File"),
    "text/x-json": ("JSON", "JSON File"),
    "application/json": ("JSON", "JSON File"),
    # KML File Types
    "kml": ("KML", "KML File"),
    "kmz": ("KML", "KMZ File"),
    "application/vnd.google-earth.kml+xml": ("KML", "KML File"),
    "application/vnd.google-earth.kmz": ("KML", "KMZ File"),
    # MS Access File Types
    "mdb": ("ACCESS", "MS Access Database"),
    "access": ("ACCESS", "MS Access Database"),
    "application/mdb": ("ACCESS", "MS Access Database"),
    "application/msaccess": ("ACCESS", "MS Access Database"),
    "application/x-msaccess": ("ACCESS", "MS Access Database"),
    "application/vnd.msaccess": ("ACCESS", "MS Access Database"),
    "application/vnd.ms-access": ("ACCESS", "MS Access Database"),
    # MS Excel File Types
    "xl": ("EXCEL", "MS Excel File"),
    "xla": ("EXCEL", "MS Excel File"),
    "xlb": ("EXCEL", "MS Excel File"),
    "xlc": ("EXCEL", "MS Excel File"),
    "xld": ("EXCEL", "MS Excel File"),
    "xls": ("EXCEL", "MS Excel File"),
    "xlsx": ("EXCEL", "MS Excel File"),
    "xlsm": ("EXCEL", "MS Excel File"),
    "excel": ("EXCEL", "MS Excel File"),
    "openXML": ("EXCEL", "MS Excel File"),
    "application/excel": ("EXCEL", "MS Excel File"),
    "application/x-excel": ("EXCEL", "MS Excel File"),
    "application/x-msexcel": ("EXCEL", "MS Excel File"),
    "application/vnd.ms-excel": ("EXCEL", "MS Excel File"),
    "application/vnd.ms-excel.sheet.macroEnabled.12": ("EXCEL", "MS Excel File"),
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": (
        "EXCEL",
        "MS Excel File",
    ),
    # MS PowerPoint File Types
    "ppt": ("POWERPOINT", "MS PowerPoint File"),
    "pps": ("POWERPOINT", "MS PowerPoint File"),
    "pptx": ("POWERPOINT", "MS PowerPoint File"),
    "ppsx": ("POWERPOINT", "MS PowerPoint File"),
    "pptm": ("POWERPOINT", "MS PowerPoint File"),
    "ppsm": ("POWERPOINT", "MS PowerPoint File"),
    "sldx": ("POWERPOINT", "MS PowerPoint File"),
    "sldm": ("POWERPOINT", "MS PowerPoint File"),
    "application/powerpoint": ("POWERPOINT", "MS PowerPoint File"),
    "application/mspowerpoint": ("POWERPOINT", "MS PowerPoint File"),
    "application/x-mspowerpoint": ("POWERPOINT", "MS PowerPoint File"),
    "application/vnd.ms-powerpoint": ("POWERPOINT", "MS PowerPoint File"),
    "application/vnd.ms-powerpoint.presentation.macroEnabled.12": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    "application/vnd.ms-powerpoint.slideshow.macroEnabled.12": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    "application/vnd.ms-powerpoint.slide.macroEnabled.12": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    "application/vnd.openxmlformats-officedocument.presentationml.slide": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    "application/vnd.openxmlformats-officedocument.presentationml.slideshow": (
        "POWERPOINT",
        "MS PowerPoint File",
    ),
    # MS Word File Types
    "doc": ("DOC", "MS Word File"),
    "docx": ("DOC", "MS Word File"),
    "docm": ("DOC", "MS Word File"),
    "word": ("DOC", "MS Word File"),
    "application/msword": ("DOC", "MS Word File"),
    "application/vnd.ms-word.document.macroEnabled.12": ("DOC", "MS Word File"),
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": (
        "DOC",
        "MS Word File",
    ),
    # Network Common Data Form (NetCDF) File Types
    "nc": ("CDF", "NetCDF File"),
    "cdf": ("CDF", "NetCDF File"),
    "netcdf": ("CDF", "NetCDF File"),
    "application/x-netcdf": ("NETCDF", "NetCDF File"),
    # PDF File Types
    "pdf": ("PDF", "PDF File"),
    "application/pdf": ("PDF", "PDF File"),
    # PERL File Types
    "pl": ("PERL", "Perl Script File"),
    "pm": ("PERL", "Perl Module File"),
    "perl": ("PERL", "Perl Script File"),
    "text/x-perl": ("PERL", "Perl Script File"),
    # QGIS File Types
    "qgis": ("QGIS", "QGIS File"),
    "application/x-qgis": ("QGIS", "QGIS File"),
    # RAR File Types
    "rar": ("RAR", "RAR Compressed File"),
    "application/rar": ("RAR", "RAR Compressed File"),
    "application/vnd.rar": ("RAR", "RAR Compressed File"),
    "application/x-rar-compressed": ("RAR", "RAR Compressed File"),
    # Resource Description Framework (RDF) File Types
    "rdf": ("RDF", "RDF File"),
    "application/rdf+xml": ("RDF", "RDF File"),
    # Rich Text Format (RTF) File Types
    "rt": ("RICH TEXT", "Rich Text File"),
    "rtf": ("RICH TEXT", "Rich Text File"),
    "rtx": ("RICH TEXT", "Rich Text File"),
    "text/richtext": ("RICH TEXT", "Rich Text File"),
    "text/vnd.rn-realtext": ("RICH TEXT", "Rich Text File"),
    "application/rtf": ("RICH TEXT", "Rich Text File"),
    "application/x-rtf": ("RICH TEXT", "Rich Text File"),
    # SID File Types - Primary association: Commodore64 (C64)?
    "sid": ("SID", "SID File"),
    "mrsid": ("SID", "SID File"),
    "audio/psid": ("SID", "SID File"),
    "audio/x-psid": ("SID", "SID File"),
    "audio/sidtune": ("SID", "MID File"),
    "audio/x-sidtune": ("SID", "SID File"),
    "audio/prs.sid": ("SID", "SID File"),
    # Tab Separated Values (TSV) File Types
    "tsv": ("TSV", "Tab Separated Values File"),
    "text/tab-separated-values": ("TSV", "Tab Separated Values File"),
    # Tape Archive (TAR) File Types
    "tar": ("TAR", "TAR Compressed File"),
    "application/x-tar": ("TAR", "TAR Compressed File"),
    # Text File Types
    "txt": ("TEXT", "Text File"),
    "text/plain": ("TEXT", "Text File"),
    # Extensible Markup Language (XML) File Types
    "xml": ("XML", "XML File"),
    "text/xml": ("XML", "XML File"),
    "application/xml": ("XML", "XML File"),
    # XYZ File Format File Types
    "xyz": ("XYZ", "XYZ File"),
    "chemical/x-xyz": ("XYZ", "XYZ File"),
    # ZIP File Types
    "zip": ("ZIP", "Zip File"),
    "application/zip": ("ZIP", "Zip File"),
    "multipart/x-zip": ("ZIP", "Zip File"),
    "application/x-compressed": ("ZIP", "Zip File"),
    "application/x-zip-compressed": ("ZIP", "Zip File"),
}

URL_RESOURCE_MAPPING = {
    # OGC
    "wms": (
        "service=wms",
        "geoserver/wms",
        "mapserver/wmsserver",
        "com.esri.wms.Esrimap",
        "service/wms",
    ),
    "wfs": (
        "service=wfs",
        "geoserver/wfs",
        "mapserver/wfsserver",
        "com.esri.wfs.Esrimap",
    ),
    "wcs": (
        "service=wcs",
        "geoserver/wcs",
        "imageserver/wcsserver",
        "mapserver/wcsserver",
    ),
    "sos": ("service=sos",),
    "csw": ("service=csw",),
    # ESRI
    "kml": ("mapserver/generatekml",),
    "arcims": ("com.esri.esrimap.esrimap",),
    "arcgis_rest": ("arcgis/rest/services",),
}


def add_landing_page_as_distribution(dcatus_doc: dict) -> dict:
    """
    add the landing page as a distribution if it's not already there. this
    function is only called when "landingPage" is within the doc.
    """
    add_lp = True

    for dist in dcatus_doc["distribution"]:
        if "accessURL" in dist and dist["accessURL"] == dcatus_doc["landingPage"]:
            add_lp = False
    if add_lp:
        dcatus_doc["distribution"].append(
            {"accessURL": dcatus_doc["landingPage"], "mediaType": "text/html"}
        )

    return dcatus_doc


def determine_mimetype(url: str) -> Union[str, None]:
    """
    attempts to determine mimetype based on input url
    """
    mtype = mimetypes.guess_type(url)
    if mtype[0]:
        return mtype[0]

    for mimetype, url_parts in URL_RESOURCE_MAPPING.items():
        for url_part in url_parts:
            if url_part in url:
                return mimetype


def prepare_distributions(dcatus_doc: dict) -> list[dict]:
    """
    adds the landing page as a distribution if it's not already and
    tries to calculate mimetype and human legible format
    """

    if "distribution" not in dcatus_doc or dcatus_doc["distribution"] is None:
        return dcatus_doc

    if "landingPage" in dcatus_doc:
        dcatus_doc = add_landing_page_as_distribution(dcatus_doc)

    for dist in dcatus_doc["distribution"]:
        # don't guess if it's already there and not a placeholder value
        if "mediaType" in dist and dist["mediaType"] != "placeholder/value":
            continue

        url_key = next(
            (key for key in ["downloadURL", "accessURL"] if key in dist), None
        )

        # stopiteration shouldn't happen because a resource needs either a download or access url
        # and at this point the record will be valid but just in case...
        if not url_key:
            continue

        mtype = determine_mimetype(dist[url_key])

        if mtype:
            dist["mediaType"] = mtype
            if mtype in RESOURCE_MAPPING:
                dist["format"] = RESOURCE_MAPPING.get(mtype)[0]
            else:
                logger.warning(f"unknown mimetype '{mtype}' in distribution")

    return dcatus_doc


def prepare_transform_msg(transform_data):
    # ruff: noqa: E731
    mask_info = lambda s: "WARNING" in s or "ERROR" in s

    struct_msgs = ", ".join(
        filter(mask_info, transform_data["readerStructureMessages"])
    )
    valid_msgs = ", ".join(
        filter(mask_info, transform_data["readerValidationMessages"])
    )
    mdt_msgs = f"structure messages: {struct_msgs} \n"
    mdt_msgs += f"validation messages: {valid_msgs}"

    return mdt_msgs


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog="Harvest Runner", description="etl harvest sources"
    )
    parser.add_argument("jobId", help="job id for harvest job")
    parser.add_argument("jobType", help="job type for harvest job")

    return parser.parse_args(args)


def create_future_date(frequency):
    interval = FREQUENCY_ENUM[frequency]
    dt = datetime.now()
    td = timedelta(days=interval)
    return dt + td


def convert_set_to_list(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def _sort_dataset_list_item_key(item):
    """type-ranked sort key for an element of a list being canonicalized by
    sort_dataset, so elements are never compared to each other directly with
    python's `<`/`>` -- which raises for dicts, and for a list containing
    a mix of types (e.g. str and int).

    ranks put same-typed values through their natural ordering (so e.g.
    string lists sort the same way python's default `sorted()` would,
    rather than by their quoted json representation, which would sort
    "food" after "food safety" because '"' > ' ').
    """
    if item is None:
        return (0,)
    if isinstance(item, bool):
        return (1, item)
    if isinstance(item, (int, float)):
        return (2, item)
    if isinstance(item, str):
        return (3, item)
    if isinstance(item, list):
        return (4, json.dumps(item, sort_keys=True))
    return (5, json.dumps(item, sort_keys=True))  # dict


def _canonicalize_dict_keys(d):
    """sort dict keys recursively without reordering any list, for the parts
    of a record whose list order carries meaning.
    """
    if isinstance(d, dict):
        return {k: _canonicalize_dict_keys(d[k]) for k in sorted(d.keys())}
    if isinstance(d, list):
        return [_canonicalize_dict_keys(item) for item in d]
    return d


def sort_dataset(d):
    """recursively canonicalize a record's ordering so semantically identical
    records hash the same regardless of the order the source emits dict keys
    and list elements in.

    dict keys are always sorted. list elements are reordered only when the
    list looks like an unordered collection: an array whose elements are all
    arrays is treated as positional data (e.g. a GeoJSON ring or LineString,
    where reordering would move vertices and break the geometry), so its
    element order is preserved while dict keys nested inside it are still
    sorted.
    """
    if isinstance(d, dict):
        return {k: sort_dataset(d[k]) for k in sorted(d.keys())}
    if isinstance(d, list):
        if d and all(isinstance(item, list) for item in d):
            return [_canonicalize_dict_keys(item) for item in d]
        return sorted(
            (sort_dataset(item) for item in d),
            key=_sort_dataset_list_item_key,
        )
    return d


def dataset_to_hash(d):
    # TODO: check for sh1 or sha256?
    # https://github.com/GSA/ckanext-datajson/blob/a3bc214fa7585115b9ff911b105884ef209aa416/ckanext/datajson/datajson.py#L279
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


def open_json(file_path):
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    with open(file_path) as fp:
        return json.load(fp)


def download_file(url: str, file_type: str) -> Union[str, dict]:
    # ruff: noqa: E501
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(url, headers=headers)
        if 200 <= resp.status_code < 300:
            if file_type == ".xml":
                data = resp.content
                if isinstance(data, bytes):
                    data = data.decode()
                return data
            return resp.json()
    except (http.client.RemoteDisconnected, requests.exceptions.ConnectionError) as e:
        raise e
    except UnicodeDecodeError as e:
        raise e

    raise Exception


# DCAT-US 3.0 Catalog fields that are harvested as their own records rather
# than stored inline on the catalog metadata.
DCATUS3_CATALOG_HARVESTED_FIELDS = ("dataset", "service", "record", "datasetSeries")


def strip_dcatus3_catalog_objects(catalog: dict) -> dict:
    """
    return a shallow copy of a DCAT-US3 Catalog dict with "dataset", "service",
    "record", and "datasetSeries" removed, since those are harvested and stored
    as their own records. nested catalogs (the "catalog" field) are cleaned the
    same way, recursively, so their own metadata is preserved.
    """
    cleaned = {
        key: value
        for key, value in catalog.items()
        if key not in DCATUS3_CATALOG_HARVESTED_FIELDS
    }

    if cleaned.get("catalog"):
        cleaned["catalog"] = [
            strip_dcatus3_catalog_objects(sub_catalog)
            for sub_catalog in cleaned["catalog"]
        ]

    return cleaned


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

    The same dataset can appear in more than one of [fields] on the same
    parent (e.g. a series's "first" usually also appears in "seriesMember").
    That's redundant source data, so each parent contributes one copy per
    distinct identifier.
    """
    nested_datasets = []

    for parent in parents:
        parent_identifier = normalize_dataset_identifier(
            parent.get(parent_identifier_field)
        )
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


def make_record_mapping(record):
    """Helper to make a Harvest record dict"""

    return {
        "identifier": record.identifier,
        "harvest_job_id": record.harvest_source.job_id,
        "harvest_source_id": record.harvest_source.id,
        "source_hash": record.metadata_hash,
        "source_raw": record.source_raw,
        "action": record.action,
        "ckan_id": record.ckan_id,
        "parent_identifier": record.parent_identifier,
        "record_type": record.record_type,
    }


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
        identifier = normalize_dataset_identifier(records[i].get(identifier_field))
        if identifier in seen:
            output.append(i)
        seen.add(identifier)
    output.sort(reverse=True)  # avoid index shifting

    return output


def get_waf_datetimes(soup: BeautifulSoup, expected_length: int) -> list:
    """Return each WAF XML link's modification time in link order."""
    date_formats = [
        (r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", "%Y-%m-%d %H:%M"),
        (r"\d{2}-[A-Za-z]{3}-\d{4}\s\d{2}:\d{2}", "%d-%b-%Y %H:%M"),
        (
            r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s(?:AM|PM)",
            "%m/%d/%Y %I:%M %p",
        ),
        (
            (
                r"[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+"
                r"\d{1,2}:\d{2}\s+(?:AM|PM)"
            ),
            "%A, %B %d, %Y %I:%M %p",
        ),
    ]
    anchors = [
        anchor
        for anchor in soup.find_all("a", href=True)
        if anchor["href"].endswith(".xml")
    ]
    output = []
    parsed_count = 0

    for anchor in anchors:
        table_row = anchor.find_parent("tr")
        date_text = (
            table_row.get_text(" ", strip=True)
            if table_row is not None
            else f"{anchor.next_sibling or ''} {anchor.previous_sibling or ''}"
        )
        modified_date = None

        for date_pattern, date_format in date_formats:
            match = re.search(date_pattern, date_text)
            if match is not None:
                modified_date = datetime.strptime(match.group(0), date_format)
                parsed_count += 1
                break

        output.append(modified_date or DT_PLACEHOLDER)

    if len(anchors) != expected_length or parsed_count != expected_length:
        logger.warning(
            "Mismatching WAF datetimes (%s parsed) and files (%s expected)",
            parsed_count,
            expected_length,
        )

    output += [DT_PLACEHOLDER] * (expected_length - len(output))
    return output[:expected_length]


def traverse_waf(
    url,
    files=None,
    datetimes=None,
    file_ext=".xml",
    folder="/",
    filters=["../", "dcatus/"],
):
    """
    Transverses WAF
    Please add docstrings
    """
    # TODO: add exception handling
    parent = os.path.dirname(url.rstrip("/"))

    folders = []

    headers = {"User-Agent": USER_AGENT}
    res = requests.get(url, headers=headers)

    if files is None:
        files = []
    if datetimes is None:
        datetimes = []

    if not res.ok:
        res.raise_for_status()

    soup = BeautifulSoup(res.content, "html.parser")
    anchors = soup.find_all("a", href=True)

    page_files = []

    for anchor in anchors:
        if (
            anchor["href"].endswith(folder)  # is the anchor link a folder?
            and not parent.endswith(
                anchor["href"].rstrip("/")
            )  # it's not the parent folder right?
            and anchor["href"] not in filters  # and it's not on our exclusion list?
        ):
            folders.append(urljoin(url, anchor["href"]))

        if anchor["href"].endswith(file_ext):
            # standardize to dcatus v1.1
            page_files.append({"identifier": urljoin(url, anchor["href"])})

    datetimes += get_waf_datetimes(soup, len(page_files))
    files += page_files

    for folder in folders:
        traverse_waf(folder, files=files, datetimes=datetimes, filters=filters)

    for i in range(len(files)):
        files[i]["modified_date"] = datetimes[i]

    return files


def is_it_true(value):
    return value.lower() == "true"


def convert_to_int(value):
    return int(value)


def get_datetime():
    return datetime.now(timezone.utc)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Find if a line between 2 x coordinates would cross the meridian
def crosses_meridian(val1, val2):
    longs = [val1, val2]
    longs.sort()
    if longs[1] > 180 and longs[0] <= 180:
        return True, val1 - val2 > 0
    if longs[0] < -180 and longs[1] >= -180:
        return True, val1 - val2 > 0
    return False, None


# Change value to valid x coordinate, no matter how far afield
def fix_longitude(val):
    if val > 180:
        return fix_longitude(val - 360)
    if val < -180:
        return fix_longitude(val + 360)
    return val


# https://www.rfc-editor.org/rfc/rfc7946#section-3.1.9
# Changes polygon to multi-polygon around the antimeridian
# We can assume this is a counter-clockwise list creating a polygon
def spatial_wrap_around_meridian(geom):
    # Place our new geometry here
    new_geom = {"type": "MultiPolygon", "coordinates": [[[], []]]}
    # We will switch between the two polygons as we cross the meridian
    polygon_num = 0

    # Find where we cross, and fill the multi-polygon
    point_list = geom.get("coordinates")[0]
    for i, coord in enumerate(point_list):
        # If we are at the last point, then we can just save it and break
        if i == len(point_list) - 1:
            new_geom["coordinates"][0][polygon_num].append(
                [fix_longitude(coord[0]), coord[1]]
            )
            break
        # Am I going to cross the meridian on the next point?
        crossing_meridian, is_positive = crosses_meridian(
            coord[0], point_list[i + 1][0]
        )
        if crossing_meridian:
            new_long = fix_longitude(coord[0])
            new_geom["coordinates"][0][polygon_num].append([new_long, coord[1]])
            # Assume we start on the left side of the meridian
            longs = [180.0, -180.0]
            if is_positive:
                # If not, we started on the right side
                longs = [-180.0, 180.0]
            # Calculate the distance longitude between the 2 points
            x_dist = abs(coord[0] - point_list[i + 1][0])
            # Calculate the percentage of the longitude to the meridian
            x_perc = abs(abs(new_long) - 180.0) / x_dist
            # Calculate the height at the meridian
            height_at_meridian = (
                point_list[i + 1][1] - point_list[i][1]
            ) * x_perc + coord[1]
            # Add the point at the meridian
            new_geom["coordinates"][0][polygon_num].append(
                [longs[0], height_at_meridian]
            )
            # Wrap up the "middle" polygon, it ends where it begins.
            if polygon_num == 1:
                new_geom["coordinates"][0][polygon_num].append(
                    new_geom["coordinates"][0][polygon_num][0]
                )
            polygon_num = (polygon_num + 1) % 2
            # Start the next polygon at the same point, just on the other side.
            new_geom["coordinates"][0][polygon_num].append([longs[1], coord[1]])
        # If not, continue to add to the current polygon
        else:
            new_geom["coordinates"][0][polygon_num].append(
                [fix_longitude(coord[0]), coord[1]]
            )
    # Unclear why this is needed, but to work with the right hand rule.
    # https://medium.com/@jinagamvasubabu/solution-polygons-and-multipolygons-should-follow-the-right-hand-rule-27b96fa61c6
    new_geom["coordinates"][0][1].reverse()
    return new_geom


def validate_geojson(geojson_str: str) -> bool:
    try:
        res = geojson_validator.validate_geometries(json.loads(geojson_str))
        # If the geometry is valid, return the string
        if res.get("invalid") == {} and res.get("problematic") == {}:
            return geojson_str
        else:
            logger.warning(
                f"This spatial value has problems: {geojson_str}, we will attempt a fix and revalidate"
            )
    # ruff: noqa: E722
    except:
        logger.warning(f"This spatial value was unable be validated: {geojson_str}")
        pass

    # Try to fix the geometry
    try:
        geojson = json.loads(geojson_str)
        # Multi-polygon doesn't get cleaned up well, if it's actually a polygon then change to that
        if (
            geojson.get("type") == "MultiPolygon"
            and len(geojson.get("coordinates")) == 1
            and len(geojson.get("coordinates")[0]) == 1
        ):
            geojson["type"] = "Polygon"
            geojson["coordinates"] = geojson["coordinates"][0]
        fixed_geom = geojson_validator.fix_geometries(geojson)
        fixed_geom = fixed_geom.get("features")[0].get("geometry")
        res = geojson_validator.validate_geometries(fixed_geom)
        if res.get("invalid") == {} and res.get("problematic") == {}:
            return json.dumps(fixed_geom)
        elif (
            res.get("problematic").get("crosses_antimeridian") is not None
            and fixed_geom.get("type") == "Polygon"
        ):
            fixed_geom = spatial_wrap_around_meridian(fixed_geom)
            return json.dumps(fixed_geom)
        else:
            logger.warning(
                f"This spatial value will be ignored: {geojson_str}, for the following reasons: {res}"
            )
    # ruff: noqa: E722
    except:
        logger.warning(f"This spatial value was unable be fixed: {geojson_str}")
        pass
    return False


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


def add_uuid_to_package_name(name: str) -> str:
    """Append a short UUID segment to ensure slug uniqueness."""

    return f"{name}-{str(uuid.uuid4())[:5]}"


_WKT_GEOMETRY_RE = re.compile(
    r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION)\s*(?:[ZM]{1,2}\s*)?\(",
    re.IGNORECASE,
)


def translate_wkt_to_geojson(spatial_value: str) -> str:
    """Convert a WKT geometry string into a GeoJSON string, if possible."""

    if not _WKT_GEOMETRY_RE.match(spatial_value):
        return ""

    try:
        geom = shapely.wkt.loads(spatial_value.strip())
        # GeoJSON is 2D; drop any Z/M dimension rather than let the
        # 3d_coordinates check in validate_geojson silently discard it.
        geom = shapely.force_2d(geom)
        return json.dumps(shapely_geom_mapping(geom))
    except:  # noqa: E722
        logger.warning(
            f"This spatial value looked like WKT but failed to parse: {spatial_value}"
        )
        return ""


def munge_spatial(spatial_value: str) -> str:
    """Translate loose spatial inputs into GeoJSON strings when possible."""

    geojson_polygon_tpl = (
        '{{"type": "Polygon", "coordinates": [[[{minx}, {miny}], '
        "[{minx}, {maxy}], [{maxx}, {maxy}], [{maxx}, {miny}], [{minx}, {miny}]]]}}"
    )
    geojson_point_tpl = '{{"type": "Point", "coordinates": [{x}, {y}]}}'
    geojson_line_tpl = (
        '{{"type": "LineString", "coordinates": [[{x1}, {y1}], [{x2}, {y2}]]}}'
    )

    #  do some string munging to try to get to something parseable
    #  1. Remove "+" characters.
    #  2. Replace occurrences of "., " and ".]" to fix some malformed separators.
    #  3. Strip a trailing "." if present.
    #  4. Remove unnecessary leading zeros A regex call.
    spatial_value = spatial_value.replace("+", "")
    spatial_value = spatial_value.replace(".,", ",").replace(".]", "]")
    if spatial_value and spatial_value[-1] == ".":
        spatial_value = spatial_value[:-1]
    spatial_value = re.sub(
        r"(^|\s)(-?)0+((0|[1-9][0-9]*)(\.[0-9]*)?)",
        r"\1\2\3",
        spatial_value,
    )

    try:
        numbers_with_spaces = [int(i) for i in spatial_value.split(" ")]
        if all(isinstance(x, int) for x in numbers_with_spaces):
            spatial_value = spatial_value.replace(" ", ",")
    except ValueError:
        pass

    parts = [part.strip() for part in spatial_value.strip().split(",")]

    if len(parts) == 2 and all(is_number(x) for x in parts):
        x, y = parts
        return geojson_point_tpl.format(x=x, y=y)

    if len(parts) == 4 and all(is_number(x) for x in parts):
        coords = list(dict.fromkeys(parts))  # removes duplicates while preserving order
        # duplicate points situation (e.g. "-92.109, 15.132, -92.109, 15.132")
        if len(coords) == 2 and parts[:2] == parts[2:]:
            x, y = coords
            return geojson_point_tpl.format(x=x, y=y)
        if len(coords) == 3:  # only 3 unique lat/lons which means it's a straight line
            return geojson_line_tpl.format(
                x1=parts[0], y1=parts[1], x2=parts[2], y2=parts[3]
            )
        # e.g. '0,0,0,0'
        if len(coords) == 1:
            return geojson_point_tpl.format(x=coords[0], y=coords[0])

        minx, miny, maxx, maxy = parts
        return geojson_polygon_tpl.format(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    try:
        geometry = json.loads(spatial_value)
        if isinstance(geometry, list) and len(geometry) == 2:
            min_point, max_point = geometry
            return geojson_polygon_tpl.format(
                minx=min_point[0],
                miny=min_point[1],
                maxx=max_point[0],
                maxy=max_point[1],
            )
    except Exception:  # noqa: BLE001
        pass

    return ""


def _get_geo_lookup_interface():
    try:
        from harvester import db_interface  # type: ignore

        return db_interface
    except Exception:  # pragma: no cover - optional dependency
        return None


def _unwrap_location(input_value):
    """Extract the geometry-bearing value from a DCAT-US 3.0 Location object.

    v3.0 `spatial` is a Location object or a list of them; v1.1 is a plain
    string. A bare {type, coordinates} GeoJSON dict is passed through.
    """

    if isinstance(input_value, list):
        input_value = next((item for item in input_value if item), None)

    if (
        isinstance(input_value, dict)
        and not {
            "type",
            "coordinates",
        }
        <= input_value.keys()
    ):
        for field in ("geometry", "bbox", "centroid"):
            value = input_value.get(field)
            if value:
                return value
        return None

    return input_value


def translate_spatial(input_value) -> str:
    """Normalize spatial strings/dicts into GeoJSON strings when possible."""

    input_value = _unwrap_location(input_value)

    if isinstance(input_value, dict):
        spatial_value = json.dumps(input_value)
    elif isinstance(input_value, str):
        spatial_value = input_value
    else:
        return ""

    if isinstance(spatial_value, str):
        wkt_geojson = translate_wkt_to_geojson(spatial_value)
        if wkt_geojson:
            spatial_value = wkt_geojson

    validated_geojson = validate_geojson(spatial_value)
    if validated_geojson:
        return validated_geojson

    dbi = _get_geo_lookup_interface()
    if dbi is not None:
        try:
            resolved_geojson = dbi.get_geo_from_string(spatial_value)
            if resolved_geojson:
                return resolved_geojson
        except Exception:  # pragma: no cover - defensive lookup
            pass

    if isinstance(spatial_value, str):
        return munge_spatial(spatial_value)

    return ""


def translate_spatial_to_geojson(spatial_input):
    """Return a GeoJSON dict for the provided spatial input, if possible."""

    if not spatial_input:
        return None

    translated_value = translate_spatial(spatial_input)
    if not translated_value:
        return None

    try:
        return json.loads(translated_value)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        logger.warning(
            "Unable to decode translated spatial value %s: %s",
            translated_value,
            exc,
        )
        return None


def dynamic_map_list_items_to_dict(list, fields):
    """
    Accept a list of items and the fields to map and return a dict of lists with those values appended
    @param list [list] - a list of items to map over
    @param fields [list] - a list of fields to extract from our list
    returns data_dict [dict] - a dict of lists with values extracted from items
    """
    data_dict = {field: [] for field in fields}
    for item in list:
        for field in fields:
            value = item.get(field)
            # Format datetime objects to be more readable for charts
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M")
            data_dict[field].append(value)
    return data_dict


def process_job_complete_percentage(job):
    if "records_total" not in job or job["records_total"] == 0:
        return "0%"
    percent_val = int(
        (
            (
                job["records_added"]
                + job["records_updated"]
                + job["records_deleted"]
                + job["records_ignored"]
                + job["records_errored"]
            )
            / job["records_total"]
        )
        * 100
    )
    return f"{percent_val}%"


def get_server_type(server: str) -> str:
    """
    Takes in the the "SERVER" header from the response and returns the
    server type string we accomadate.
    Original code from https://github.com/ckan/ckanext-spatial/blob/master/ckanext/spatial/harvesters/waf.py#L277
    """
    if not server or "apache" in server.lower():
        return "apache"
    if "nginx" in server.lower():
        return "nginx"
    if "Microsoft-IIS" in server:
        return "iis"
    else:
        return server


class RetrySession(requests.Session):
    """
    Session made to handle more advanced logging and adds retry logic.
    """

    def __init__(
        self,
        status_forcelist: Optional[Set[int]] = None,
        max_retries: int = 3,
        backoff_factor: float = 4.0,
    ):
        """
        Initialize the RetrySession.

        status_forcelist: Set of HTTP status codes to retry on.
            Defaults to {404, 499, 500, 502}
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor for exponential backoff delay
        """
        super().__init__()

        self.status_forcelist = status_forcelist or {404, 499, 500, 502}
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Set default User-Agent header
        self.headers.update({"User-Agent": USER_AGENT})

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Override request method to add logging and manual retry logic.

        method: HTTP method
        url: Request URL
        **kwargs: Additional arguments passed to parent request method (
            needed since we override it)
        """
        last_response = None
        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                if attempt == 0:
                    logger.info(f"Making initial {method.upper()} request to {url}")

                response = super().request(method, url, **kwargs)
                # If status code should trigger retry and we have attempts left
                if (
                    response.status_code in self.status_forcelist
                    and attempt < self.max_retries
                ):
                    if attempt == 0:
                        logger.warning(
                            f"Received status code {response.status_code} for "
                            f"{method.upper()} {url}. Retrying..."
                        )
                    else:
                        logger.warning(
                            f"Attempt {attempt}: Received status code "
                            f"{response.status_code} for {method.upper()} {url}. "
                            f"Retrying..."
                        )
                    last_response = response
                    # Calculate backoff delay
                    # for the 3, 7, 15 countdown
                    delay = (self.backoff_factor * (2**attempt)) - 1
                    time.sleep(delay)
                    continue

                # Success or no more retries
                if response.status_code in self.status_forcelist:
                    logger.error(
                        f"Final attempt: Still received status code "
                        f"{response.status_code} for {method.upper()} {url}"
                    )

                return response

            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Attempt {attempt + 1}: Request failed for "
                    f"{method.upper()} {url}: {str(e)}"
                )
                last_exception = e

                if attempt < self.max_retries:
                    delay = self.backoff_factor * (2**attempt)
                    time.sleep(delay)
                    continue
                else:
                    raise

        # This should not be reached, but just in case
        if last_exception:
            raise last_exception

        return last_response


def create_retry_session() -> RetrySession:
    """
    Creates our desired RetrySession with default settings.
    """
    # Use the HARVEST_RETRY_ON_ERROR env var to determine if we should retry
    retry_enabled = os.getenv("HARVEST_RETRY_ON_ERROR", "true").lower() == "true"
    if retry_enabled:
        return RetrySession(max_retries=3, backoff_factor=4.0)
    else:
        return RetrySession(max_retries=0, backoff_factor=0)


def send_email_to_recipients(recipients, subject, body):
    """Send an email to a list of recipiencts.

    The subject and body stay the same and it sends messages to each address
    in the iterable `recipients`.
    """
    with smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"]) as server:
        if SMTP_CONFIG["use_tls"]:
            server.starttls()
        server.login(SMTP_CONFIG["username"], SMTP_CONFIG["password"])

        for recipient in recipients:
            msg = MIMEMultipart()
            msg["From"] = SMTP_CONFIG["default_sender"]
            msg["To"] = recipient
            msg["Reply-To"] = "no-reply@gsa.gov"
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            server.sendmail(SMTP_CONFIG["default_sender"], [recipient], msg.as_string())
            logger.info(f"Notification email sent to: {recipient}")


def get_format_from_str(validation_msg: str) -> str:
    """
    gets the format/rule used against the data (e.g. 'uri', 'string', some regex)
    """
    if "is too long" in validation_msg:
        match = re.search(r"\[maxLength=(\d+)\]", validation_msg)
        if match:
            return f"max string length of {match.group(1)} characters"
        match = re.search(r"\[maxItems=(\d+)\]", validation_msg)
        if match:
            return f"max {match.group(1)} items"
        return "max string length requirement"

    if "is too short" in validation_msg:
        match = re.search(r"\[minItems=(\d+)\]", validation_msg)
        if match:
            return f"min {match.group(1)} items"
        return "min items requirement"

    # Match jsonschema's full "has non-unique elements" wording, not just
    # "non-unique" -- an invalid value containing that text would hijack the match.
    if "has non-unique elements" in validation_msg:
        return "unique items"

    # for constants where a single value is acceptable
    if "was expected" in validation_msg:
        return f"constant value {validation_msg}"
    return validation_msg.split(" ")[-1]


_VALIDATION_ERROR_WRAPPER_RE = re.compile(
    r"^<(?:ValidationError|ValidationException):\s*(.*)>$", re.DOTALL
)
_ACCEPTABLE_FORMATS_PREFIX = "does not match any of the acceptable formats: "


def parse_validation_message(message: str) -> tuple[Optional[str], str]:
    """
    split a stored record error message into (field, rule).

    validation messages are stored as `repr()` of a jsonschema
    `ValidationError`, e.g.
        <ValidationError: "$.license, 'center' does not match any of the
        acceptable formats: 'uri', 'null'">
    other message types (TransformationException, DCAT warnings, etc.) don't
    have this shape and are returned as (None, message).
    """
    wrapper_match = _VALIDATION_ERROR_WRAPPER_RE.match(message)
    if not wrapper_match:
        return None, message

    try:
        with warnings.catch_warnings():
            # repr() embeds raw regex backslashes (e.g. the REDACTED format)
            # that aren't valid escapes; literal_eval still parses them fine.
            warnings.simplefilter("ignore", SyntaxWarning)
            inner = ast.literal_eval(wrapper_match.group(1))
    except (ValueError, SyntaxError):
        inner = wrapper_match.group(1).strip("'\"")

    json_path, _, rule_text = inner.partition(", ")
    if not rule_text:
        return None, message

    field = json_path[2:] if json_path.startswith("$.") else json_path
    if field in ("", "$"):
        field = "(root)"
    field = re.sub(r"\[\d+\]", "[]", field)

    if rule_text.endswith("is a required property"):
        rule = "required property"
    elif _ACCEPTABLE_FORMATS_PREFIX in rule_text:
        rule = rule_text.split(_ACCEPTABLE_FORMATS_PREFIX, 1)[1]
    else:
        rule = rule_text

    return field, rule


def group_record_error_fields(rows: list[tuple]) -> list[dict]:
    """
    group (severity, type, message, count) rows into per-field/rule summaries
    for the harvest job report. rows are pre-aggregated by exact message, so
    identical (field, rule) pairs from different messages are merged here.
    """
    severity_order = {"error": 0, "warning": 1}
    grouped: dict[tuple, dict] = {}

    for severity, error_type, message, count in rows:
        if error_type in ("ValidationError", "ValidationException"):
            field, rule = parse_validation_message(message)
        else:
            field, rule = None, error_type

        key = (severity, field, rule)
        entry = grouped.setdefault(
            key,
            {
                "severity": severity,
                "field": field,
                "rule": rule,
                "type": error_type,
                "count": 0,
                "examples": [],
            },
        )
        entry["count"] += count
        if len(entry["examples"]) < 3:
            entry["examples"].append(message)

    return sorted(
        grouped.values(),
        key=lambda entry: (
            severity_order.get(entry["severity"], 2),
            -entry["count"],
        ),
    )


def build_report_email_section(
    error_field_summary: list[dict], sample_datasets: list, catalog_base_url: str
) -> str:
    """
    Plain-text rendering of the /report page's issues-by-field and sample
    dataset sections, for inlining into the harvest job notification email.
    """
    lines = ["Issues by field:"]
    if not error_field_summary:
        lines.append("- No record errors or warnings found.")
    else:
        for row in error_field_summary:
            field = row["field"] or "(root)"
            lines.append(
                f"- {row['severity']}: {field} - {row['rule']} ({row['count']})"
            )

    lines.append("")
    lines.append("Sample datasets:")
    if not sample_datasets:
        lines.append("- No datasets have been harvested from this source yet.")
    else:
        for dataset in sample_datasets:
            if catalog_base_url:
                lines.append(
                    f"- {dataset.slug}: {catalog_base_url}/dataset/{dataset.slug}"
                )
            else:
                lines.append(f"- {dataset.slug}")

    return "\n".join(lines)


def found_simple_message(
    validation_error: ValidationError, forced: bool = False
) -> bool:
    """
    determine whether the input validation error represents the most
    succinct cause for error based on its json_path or dtype.

    `forced` is a last-resort override set by `_collect_validation_messages`.
    """
    # these are all the unique dtypes found in the
    # non-federal schema (no different than federal)
    # {"'boolean'", "'null'", "'array'", "'number'", "'object'", "'string'"}

    # the required field at the root is missing entirely
    if validation_error.json_path == "$":
        return True

    # we need to dig a little deeper when it's a list or dict
    if isinstance(validation_error.instance, (dict, list)):
        # if it's empty you'll get something like
        # ['$.keyword', '[] should be non-empty']
        # which is simple and what we want
        if len(validation_error.instance) == 0:
            return True

        # `type` errors have no `context`. Keep them when the allowed types are
        # a list, when this is a top-level error, or when the path is deeper
        # than the parent (a real cause inside a branch). Same-path single-type
        # errors are anyOf/oneOf branch noise unless `forced`.
        if validation_error.validator == "type":
            return bool(
                isinstance(validation_error.validator_value, list)
                or validation_error.parent is None
                or validation_error.json_path != validation_error.parent.json_path
                or forced
            )

        # Combinators are not a cause; their `.context` holds the per-branch errors.
        if validation_error.validator in ("anyOf", "oneOf", "allOf", "not"):
            return False

        # Other container validators (maxItems, minItems, uniqueItems, ...) are
        # already the specific cause.
        return True
    return True


def is_required_property(messages: str) -> bool:
    for message in messages:
        if "is a required property" in message:
            return True

    return False


def finalize_validation_messages(messages: defaultdict) -> list:
    """
    build the final validation messages either individually (root) or
    bundled by field. see tests for output.

    the input default dict is organized by { json_path: [errors...]} format
        { "$": [ "'a' is required", "'b' is required" ],
          "$.keyword": [ "[] should be non-empty", "[] is not of type 'string'" ],
          "$.contactPoint.hasEmail": [ format1, format2, format3, etc...]
        }

    the regex says: get me the first word(s) in single quotes or just empty brackets [].
    what's inside the single quotes represents the invalid data
    """

    output = []

    for json_path, formats in messages.items():
        # required property messages aren't based on format but simply
        # "[field] is a required property"
        if is_required_property(formats):
            output += map(
                lambda error: ValidationError(f"{json_path}, {error}"),
                messages[json_path],
            )
            continue

        # all other errors are bundled based on the formats/rules

        # constants like in "accrualPeriodicity" don't include the invalid data
        # but >1 format/rule is used against it so grabbing
        # the last one which is a regex and does include the invalid data
        # excluding constants [0] == [n]
        # jsonschema renders containers as repr; quoting an inner element (or a
        # const's expected value) would mislead, so name the kind of value.
        # "[]" already reads as itself.
        container = next(
            (f for f in formats if f[:1] in ("[", "{") and f[:2] != "[]"), None
        )
        if formats[-1].startswith("None"):
            invalid_value = "None"
        elif container is not None:
            invalid_value = "array value" if container[0] == "[" else "object value"
        else:
            # group(0): the `[]` alternative has no capture groups.
            match = re.search(r"'(.*?)'|\[\]", formats[-1])
            invalid_value = match.group(0) if match else None

        # if neither branch above found anything, none of them will
        if invalid_value is None:
            logger.warning(f"can't find invalid data from error message: {formats[0]}")
            continue

        # @type values are consts too
        if invalid_value in [
            "'dcat:Distribution'",
            "'org:Organization'",
            "'dcat:Dataset'",
            "'vcard:Contact'",
        ]:
            invalid_value = "@type value"

        formats = map(get_format_from_str, formats)

        # build the bundled error message by json_path
        msg = ValidationError(
            f"{json_path}, {invalid_value} does not match any of "
            "the acceptable formats: " + ", ".join(formats)
        )
        output.append(msg)

    return output


def assemble_validation_errors(validation_errors: list, messages=None) -> list:
    """
    given a list of errors, follow each one recursively through its context
    and get the simplest cause for error. store the error in a defaultdict
    such that { json_path: [errors...]}

    errors with lists or dicts (other than empty)
    will often return the entire object followed by 'is not valid under any
    of the given schemas' which isn't helpful.

    pass `messages` to accumulate across calls; the formatted list is returned
    either way.
    """

    if messages is None:
        # {'$.distribution[2].title' = ["'' should be non-empty", etc...]}
        messages = defaultdict(list)

    _collect_validation_messages(validation_errors, messages, forced=False)
    return finalize_validation_messages(messages)


def _collect_validation_messages(
    validation_errors: list, messages: defaultdict, *, forced: bool
) -> int:
    """
    fill `messages` and return how many were appended, nested walks included.
    Formatting is the caller's job; doing it on every recursive return, like
    re-counting the dict, made this quadratic in the number of errors.

    `forced` is a last-resort fallback. After an unforced context walk records
    nothing, we re-walk forced so a same-path type error is reported vaguely
    instead of silently. A walk that already recorded a specific cause is left
    alone.
    """

    recorded = 0

    for error in validation_errors:
        if found_simple_message(error, forced=forced):
            # these aren't specific enough which make them unhelpful
            generic_msg = "is not valid under any of the given schemas"
            is_generic_msg = error.message.endswith(generic_msg)
            if error.validator == "maxLength":
                formatted_message = (
                    f"{error.message} [maxLength={error.validator_value}]"
                )
            elif error.validator == "maxItems":
                formatted_message = (
                    f"{error.message} [maxItems={error.validator_value}]"
                )
            elif error.validator == "minItems" and "is too short" in error.message:
                # minItems: 1 already says "should be non-empty";
                # only "is too short" needs the count.
                formatted_message = (
                    f"{error.message} [minItems={error.validator_value}]"
                )
            else:
                formatted_message = error.message
            # if not the generic message, and if the message is not already
            # present in the list for the given path we skip to avoid duplicates
            # based on how messages are returned from the validator
            if (
                not is_generic_msg
                and formatted_message not in messages[error.json_path]
            ):
                messages[error.json_path].append(formatted_message)
                recorded += 1

        # Prefer a specific cause in context before falling back.
        from_context = _collect_validation_messages(
            error.context, messages, forced=False
        )
        recorded += from_context

        # Nothing recorded: re-walk forced so the defect is not dropped.
        # `forced` only flips `type` errors, which have no context to recurse.
        if error.context and from_context == 0:
            recorded += _collect_validation_messages(
                error.context, messages, forced=True
            )

    return recorded


def is_valid_uuid4(uuid_string) -> bool:
    """
    Checks if a given string is a valid UUID 4. All h20 db ids are version 4. see Base(DeclarativeBase)
    in models.
    """
    try:
        return str(UUID(uuid_string, version=4)) == uuid_string
    except ValueError:
        return False
    except (
        AttributeError
    ):  # Catches cases where input might not be a string (e.g., UUID(0))
        return False


def build_dcatus3_validator(
    definitions_dir,
    root_ref="https://resources.data.gov/dcat-us/3.0.0/definitions/catalog",
):
    """
    builds a dcatus v3.0 validator based on schema files in [definitions_dir].

    root_ref selects the entry point into the schema definitions. it defaults to
    the catalog definition (used by the validator web tool to validate a whole
    catalog), but can be pointed at the dataset definition so the validator can
    check a single dataset record at a time during harvest.
    """
    registry = Registry()

    schema_files = sorted(definitions_dir.glob("*.json"))
    if not schema_files:
        raise FileNotFoundError(
            f"no JSON Schema definitions found in {definitions_dir}. "
            "DCAT-US 3.0 definitions come from the GSA/dcat-us git submodule; "
            "run `git submodule update --init _external/dcat-us`."
        )

    for schema_file in schema_files:
        schema = open_json(schema_file)
        registry = registry.with_resource(
            uri=schema["$id"],
            resource=DRAFT202012.create_resource(schema),
        )

    return Draft202012Validator(
        schema={"$ref": root_ref},
        registry=registry,
        format_checker=FormatChecker(),
    )
