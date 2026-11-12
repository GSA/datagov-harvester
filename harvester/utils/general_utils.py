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
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, Set, Union
from urllib.parse import urljoin
from uuid import UUID

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
from dateutil.parser import ParserError

from dcatus_validation.fetch import USER_AGENT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

FREQUENCY_ENUM = {"daily": 1, "weekly": 7, "biweekly": 14, "monthly": 30}

DT_PLACEHOLDER = datetime.now()


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


def get_waf_datetimes(soup: BeautifulSoup, expected_length: int) -> list:
    """Return each WAF XML link's modification time in link order.

    Extracts datetime strings and uses python-dateutil to parse them flexibly.
    Based on ckanext-spatial approach: extract string, then dateutil.parser.parse().
    """
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

        if date_text:
            try:
                date_candidates = re.findall(
                    r"(?:\d{1,4}[-/]\d{1,2}[-/]\d{1,4}|\d{1,2}-[A-Za-z]{3}-\d{4}|"
                    r"[A-Za-z]+,?\s+[A-Za-z]+\s+\d{1,2},?\s+\d{4})"
                    r"\s+\d{1,2}:\d{2}(?:\s*(?:AM|PM))?",
                    date_text,
                )

                if date_candidates:
                    modified_date = dateutil_parser.parse(
                        date_candidates[0], default=datetime(1900, 1, 1)
                    )
                    parsed_count += 1
            except (ValueError, TypeError, ParserError, OverflowError):
                pass

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


def add_uuid_to_package_name(name: str) -> str:
    """Append a short UUID segment to ensure slug uniqueness."""

    return f"{name}-{str(uuid.uuid4())[:5]}"


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


def send_email_to_recipients(recipients, subject, body, html_body=None):
    """Send plain text and optional HTML versions to each recipient.

    The subject and body stay the same and it sends messages to each address
    in the iterable `recipients`.
    """
    with smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"]) as server:
        if SMTP_CONFIG["use_tls"]:
            server.starttls()
        server.login(SMTP_CONFIG["username"], SMTP_CONFIG["password"])

        for recipient in recipients:
            msg = MIMEMultipart("alternative" if html_body is not None else "mixed")
            msg["From"] = SMTP_CONFIG["default_sender"]
            msg["To"] = recipient
            msg["Reply-To"] = "no-reply@gsa.gov"
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))
            if html_body is not None:
                msg.attach(MIMEText(html_body, "html"))

            server.sendmail(SMTP_CONFIG["default_sender"], [recipient], msg.as_string())
            logger.info(f"Notification email sent to: {recipient}")


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


def format_recipient_list(notification_emails: list[str] | None) -> str:
    """
    Format notification emails as comma-separated string or 'None'.

    Args:
        notification_emails: List of email addresses or None

    Returns:
        Comma-separated string of emails or "None"
    """
    if not notification_emails:
        return "None"
    filtered = [str(email) for email in notification_emails if email]
    return ", ".join(filtered) if filtered else "None"


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
