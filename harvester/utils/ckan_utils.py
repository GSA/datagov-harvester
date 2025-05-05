import json
import logging
import mimetypes
import os
import re
import typing
import urllib
import uuid
from typing import Tuple, Union

from ckanapi import RemoteCKAN

from database.interface import HarvesterDBInterface
from harvester.exceptions import DCATUSToCKANException, SynchronizeException

if typing.TYPE_CHECKING:
    from harvester.harvest import HarvestSource

from harvester.utils.general_utils import get_datetime, is_number, validate_geojson

# all of these are copy/pasted from ckan core
# https://github.com/ckan/ckan/blob/master/ckan/lib/munge.py

PACKAGE_NAME_MAX_LENGTH = 90
PACKAGE_NAME_MIN_LENGTH = 2

MAX_TAG_LENGTH = 100
MIN_TAG_LENGTH = 2

# logging data
logger = logging.getLogger("ckan_utils")

db = HarvesterDBInterface()


class CKANSyncTool:
    """A helper class used for parallelization of CKAN network calls.

    Args:
        session: requires a pre-existing DB session connection

    Usage:
        ckan_sync_tool = CKANSyncTool(session=session)
        ckan_sync_tool.sync(record)
    """

    def __init__(self, session):
        if not session:
            raise ValueError("No session provided")
        self.ckan = self.open_connection(session)

    def open_connection(self, session):
        return RemoteCKAN(
            os.getenv("CKAN_API_URL"),
            apikey=os.getenv("CKAN_API_TOKEN"),
            session=session,
        )

    def sync(self, record):
        """General sync bus for all records to pass through.

        Args:
            record: An instance of HarvestRecord (Record) class

        Returns:
            True (bool): on record success

        Raises:
            DCATUSToCKANException: Any DCAT-US to CKAN metadata transform issue
            SynchronizeException: Any ckan sync issue
        """
        # pre-sync to ckanify record metadata
        try:
            if record.action == "create" or record.action == "update":
                self.ckanify_record(record)
        except Exception as e:
            record.status = "error"
            raise DCATUSToCKANException(
                repr(e), record.harvest_source.job_id, record.id
            )

        # begin ckanpi sync
        start = get_datetime()
        try:
            if record.action == "delete":
                self.delete_record(record)
            elif record.action == "create":
                res = self.create_record(record)
                record.ckan_id = res["id"]
                record.ckan_name = record.ckanified_metadata["name"]
            elif record.action == "update":
                self.update_record(record)
        except Exception as e:
            record.status = "error"
            raise SynchronizeException(
                f"failed to {record.action} for {record.identifier} :: {repr(e)}",
                record.harvest_source.job_id,
                record.id,
            )

        logger.info(
            f"time to {record.action} {record.identifier} \
                {get_datetime() - start}"
        )
        record.status = "success"

        # update harvest reporter
        record.harvest_source.reporter.update(record.action)

        # # update harvest job
        record.harvest_source.db_interface.update_harvest_job(
            record.harvest_source.job_id, record.harvest_source.reporter.report()
        )

        return True

    def create_record(self, record, retry=False) -> dict:
        try:
            return self.ckan.action.package_create(**record.ckanified_metadata)
        except Exception as e:
            if retry is False:
                record.ckanified_metadata["name"] = add_uuid_to_package_name(
                    record.ckanified_metadata["name"]
                )
                return self.create_record(record, retry=True)
            else:
                raise e
                # will be caught by outer SynchronizeException

    def update_record(self, record) -> dict:
        updated_metadata = {
            **record.ckanified_metadata,
            **{"id": record.ckan_id, "name": record.ckan_name},
        }
        return self.ckan.action.package_update(**updated_metadata)

    def delete_record(self, record) -> None:
        return self.ckan.action.dataset_purge(id=record.ckan_id)

    def ckanify_record(self, record) -> None:
        metadata = (
            record.metadata
            if record.transformed_data is None
            else record.transformed_data
        )

        record.ckanified_metadata = ckanify_dcatus(
            metadata, record.harvest_source, record.id
        )

    def close_conection(self) -> None:
        self.ckan.close()


def trim_tag(tag):
    # deal with something like
    # EARTH   SCIENCE > ATMOSPHERE > ATMOSPHERIC ELECTRICITY > ATMOSPHERIC CONDUCTIVITY
    # Truncate individual keywords to 100 characters since DB fields is varchar 100
    trimmed = re.split(r"[;,>]", tag)
    trimmed = [t.lower().strip() for t in trimmed]
    return set([" ".join(t.split())[:100] for t in trimmed if t != ""])


# mapping of file formats and their respective names
# taken from https://github.com/GSA/ckanext-geodatagov/blob/4510c5be2bb9ecc16de8bae082fef4d970f10f55/ckanext/geodatagov/plugin.py#L59-L282
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


def _munge_to_length(string: str, min_length: int, max_length: int) -> str:
    """Pad/truncates a string"""
    if len(string) < min_length:
        string += "_" * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string


def substitute_ascii_equivalents(text_unicode: str) -> str:
    # Method taken from: http://code.activestate.com/recipes/251871/
    """
    This takes a UNICODE string and replaces Latin-1 characters with something
    equivalent in 7-bit ASCII. It returns a plain ASCII string. This function
    makes a best effort to convert Latin-1 characters into ASCII equivalents.
    It does not just strip out the Latin-1 characters. All characters in the
    standard 7-bit ASCII range are preserved. In the 8th bit range all the
    Latin-1 accented letters are converted to unaccented equivalents. Most
    symbol characters are converted to something meaningful. Anything not
    converted is deleted.
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
        # 0xa1: '!', 0xa2: '{cent}', 0xa3: '{pound}', 0xa4: '{currency}',
        # 0xa5: '{yen}', 0xa6: '|', 0xa7: '{section}', 0xa8: '{umlaut}',
        # 0xa9: '{C}', 0xaa: '{^a}', 0xab: '<<', 0xac: '{not}',
        # 0xad: '-', 0xae: '{R}', 0xaf: '_', 0xb0: '{degrees}',
        # 0xb1: '{+/-}', 0xb2: '{^2}', 0xb3: '{^3}', 0xb4:"'",
        # 0xb5: '{micro}', 0xb6: '{paragraph}', 0xb7: '*', 0xb8: '{cedilla}',
        # 0xb9: '{^1}', 0xba: '{^o}', 0xbb: '>>',
        # 0xbc: '{1/4}', 0xbd: '{1/2}', 0xbe: '{3/4}', 0xbf: '?',
        # 0xd7: '*', 0xf7: '/'
    }

    r = ""
    for char in text_unicode:
        if ord(char) in char_mapping:
            r += char_mapping[ord(char)]
        elif ord(char) >= 0x80:
            pass
        else:
            r += str(char)
    return r


def munge_title_to_name(name: str) -> str:
    """Munge a package title into a package name."""
    name = substitute_ascii_equivalents(name)
    # convert spaces and separators
    name = re.sub("[ .:/]", "-", name)
    # take out not-allowed characters
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    # remove doubles
    name = re.sub("-+", "-", name)
    # remove leading or trailing hyphens
    name = name.strip("-")
    # if longer than max_length, keep last word if a year
    max_length = PACKAGE_NAME_MAX_LENGTH
    # (make length less than max, in case we need a few for '_' chars
    # to de-clash names.)
    if len(name) > max_length:
        year_match = re.match(r".*?[_-]((?:\d{2,4}[-/])?\d{2,4})$", name)
        if year_match:
            year = year_match.groups()[0]
            name = "%s-%s" % (name[: (max_length - len(year) - 1)], year)
        else:
            name = name[:max_length]
    name = _munge_to_length(name, PACKAGE_NAME_MIN_LENGTH, PACKAGE_NAME_MAX_LENGTH)
    return name


def munge_tag(tag: str) -> str:
    tag = substitute_ascii_equivalents(tag)
    tag = tag.lower().strip()
    tag = re.sub(r"[^a-zA-Z0-9\- ]", "", tag).replace(" ", "-")
    # remove doubles
    tag = re.sub("-+", "-", tag)
    # remove leading or trailing hyphens
    tag = tag.strip("-")
    tag = _munge_to_length(tag, MIN_TAG_LENGTH, MAX_TAG_LENGTH)
    return tag


def create_ckan_extras(
    metadata: dict, harvest_source: "HarvestSource", record_id: str
) -> list[dict]:
    extras = [
        "accessLevel",
        "bureauCode",
        "identifier",
        "modified",
        "programCode",
        "publisher",
        "spatial",
        "isPartOf",
    ]

    output = [
        {"key": "resource-type", "value": "Dataset"},
        {"key": "harvest_object_id", "value": record_id},
        {
            "key": "source_datajson_identifier",  # dataset is datajson format or not
            "value": True,
        },
        {
            "key": "harvest_source_id",
            "value": harvest_source.id,
        },
        {
            "key": "harvest_source_title",
            "value": harvest_source.name,
        },
    ]

    for extra in extras:
        if extra not in metadata:
            continue
        data = {"key": extra, "value": None}
        val = metadata[extra]
        if extra == "publisher":
            data["value"] = val["name"]

            output.append(
                {
                    "key": "publisher_hierarchy",
                    "value": create_ckan_publisher_hierarchy(val, []),
                }
            )
        elif extra == "spatial":
            output.append({"key": "old-spatial", "value": metadata["spatial"]})
            data["value"] = translate_spatial(metadata["spatial"])
        else:
            # TODO: confirm this is what we want.
            if isinstance(val, list) and len(val) > 0:
                val = val[0]
            data["value"] = val
        output.append(data)

    # TODO: update this
    # output.append(
    #     {
    #         "key": "dcat_metadata",
    #         "value": str(sort_dataset(self.metadata)),
    #     }
    # )

    # output.append(
    #     {
    #         "key": self.harvest_source.extra_source_name,
    #         "value": self.harvest_source.title,
    #     }
    # )

    output.append({"key": "identifier", "value": metadata["identifier"]})

    return output


def create_ckan_tags(keywords: list[str]) -> list:
    output = []

    for keyword in keywords:
        for tag in trim_tag(keyword):
            data = {"name": munge_tag(tag)}
            if data not in output:
                output.append(data)
    return output


def create_ckan_publisher_hierarchy(pub_dict: dict, data: list = []) -> str:
    for k, v in pub_dict.items():
        if k == "name":
            data.append(v)
        if isinstance(v, dict):
            create_ckan_publisher_hierarchy(v, data)

    return " > ".join(data[::-1])


def get_email_from_str(in_str: str) -> str:
    res = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", in_str)
    if res is not None:
        return res.group(0)


def get_filename_and_extension(resource: dict) -> Tuple[str, str]:
    """
    Attempt to extract a file name and extension from a provided resource.
    Original Code from:
    https://github.com/GSA/ckanext-geodatagov/blob/4510c5be2bb9ecc16de8bae082fef4d970f10f55/ckanext/geodatagov/plugin.py#L342
    """
    url = resource.get("url").rstrip("/")
    if "?" in url:
        return "", ""
    if "URL" in url:
        return "", ""
    url = urllib.parse.urlparse(url).path
    split = url.split("/")
    last_part = split[-1]
    ending = last_part.split(".")[-1].lower()
    if len(ending) in [2, 3, 4] and len(last_part) > 4 and len(split) > 1:
        return last_part, ending
    return "", ""


def change_resource_details(resource: dict) -> None:
    """
    Pull the provided file name, format, and description.
    Note this function works by manipulating the original input.
    Original Code from:
    https://github.com/GSA/ckanext-geodatagov/blob/4510c5be2bb9ecc16de8bae082fef4d970f10f55/ckanext/geodatagov/plugin.py#L357
    """
    formats = list(RESOURCE_MAPPING.keys())
    resource_format = resource.get("format", "").lower().lstrip(".")
    filename, extension = get_filename_and_extension(resource)
    if not resource_format:
        resource_format = extension
    if resource.get("name", "") in ["Unnamed resource", "", None]:
        resource["no_real_name"] = True
    if resource_format in formats:
        resource["format"] = RESOURCE_MAPPING[resource_format][0]
        if resource.get("name", "") in ["Unnamed resource", "", None]:
            resource["name"] = RESOURCE_MAPPING[resource_format][1]
            if filename:
                resource["name"] = resource["name"]
    elif resource.get("name", "") in ["Unnamed resource", "", None]:
        if extension and not resource_format:
            resource["format"] = extension.upper()
        resource["name"] = "Web Resource"

    if filename and not resource.get("description"):
        resource["description"] = filename


def guess_resource_format(url: str, use_mimetypes: bool = True) -> Union[str, None]:
    """
    Given a URL try to guess the best format to assign to the resource

    The function looks for common patterns in popular geospatial services and
    file extensions, so it may not be 100% accurate. It just looks at the
    provided URL, it does not attempt to perform any remote check.

    if 'use_mimetypes' is True (default value), the mimetypes module will be
    used if no match was found before.

    Returns None if no format could be guessed.

    Original Code from:
    https://github.com/GSA/ckanext-spatial/blob/418f0f9daaef4f5363525162fc42904ce954a467/ckanext/spatial/harvesters/base.py#L63
    """
    url = url.lower().strip()

    resource_types = {
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

    for resource_type, parts in resource_types.items():
        if any(part in url for part in parts):
            return resource_type

    file_types = {
        "kml": ("kml",),
        "kmz": ("kmz",),
        "gml": ("gml",),
    }

    for file_type, extensions in file_types.items():
        if any(url.endswith(extension) for extension in extensions):
            return file_type

    # to align with the comment I'm adding this code in case
    if use_mimetypes:
        resource_format, encoding = mimetypes.guess_type(url)
        return resource_format

    return None


def create_ckan_resources(metadata: dict) -> list[dict]:
    output = []

    if "distribution" not in metadata or metadata["distribution"] is None:
        return output

    for dist in metadata["distribution"]:
        resource = {}
        if "description" in dist:
            resource["description"] = dist["description"]
        if "title" in dist:
            resource["name"] = dist["title"]
        url_keys = ["downloadURL", "accessURL"]
        for url_key in url_keys:
            if dist.get(url_key, None) is None:
                continue
            resource["url"] = dist[url_key]
            # set mimetype if provided or discover it
            if "mimetype" in dist:
                resource["mimetype"] = dist["mediaType"]
            else:
                resource["mimetype"] = guess_resource_format(dist[url_key])

            # if we know the mimetype add the other details
            if resource["mimetype"]:
                change_resource_details(resource=resource)
        output.append(resource)

    return output


def simple_transform(metadata: dict, owner_org: str) -> dict:
    output = {
        "name": munge_title_to_name(metadata["title"]),
        "owner_org": owner_org,
        "identifier": metadata["identifier"],
        "author": None,  # TODO: CHANGE THIS!
        "author_email": None,  # TODO: CHANGE THIS!
    }

    mapping = {
        "contactPoint": {"fn": "maintainer", "hasEmail": "maintainer_email"},
        "description": "notes",
        "title": "title",
    }

    for k, v in metadata.items():
        if k not in mapping:
            continue
        if isinstance(mapping[k], dict):
            temp = {}
            to_skip = ["@type"]
            for k2, v2 in v.items():
                if k2 == "hasEmail":
                    v2 = get_email_from_str(v2)
                if k2 in to_skip:
                    continue
                temp[mapping[k][k2]] = v2
            output = {**output, **temp}
        else:
            output[mapping[k]] = v

    return output


def ckanify_dcatus(
    metadata: dict, harvest_source: "HarvestSource", record_id: str
) -> dict:
    ckanified_metadata = simple_transform(metadata, harvest_source.organization_id)

    ckanified_metadata["resources"] = create_ckan_resources(metadata)
    ckanified_metadata["tags"] = (
        create_ckan_tags(metadata["keyword"]) if "keyword" in metadata else []
    )
    ckanified_metadata["extras"] = create_ckan_extras(
        metadata, harvest_source, record_id
    )

    return ckanified_metadata


def add_uuid_to_package_name(name: str) -> str:
    return name + "-" + str(uuid.uuid4())[:5]


def munge_spatial(spatial_value: str) -> str:
    # This function originally came from
    # https://github.com/GSA/ckanext-geodatagov/blob/ac752b30fbd916e9a078d732231edb8f81914d9c/ckanext/geodatagov/logic.py#L445
    geojson_polygon_tpl = (
        '{{"type": "Polygon", '
        '"coordinates": [[[{minx}, {miny}], [{minx}, {maxy}], '
        "[{maxx}, {maxy}], [{maxx}, {miny}], [{minx}, {miny}]]]}}"
    )
    geojson_point_tpl = '{{"type": "Point", "coordinates": [{x}, {y}]}}'

    # Replace all things that create bad JSON, https://github.com/GSA/data.gov/issues/3549
    # all instances of '+', '[+23, -1]' is not valid, but '[23, -1]' is valid
    spatial_value = spatial_value.replace("+", "")
    # all trailing decimals, '[34., 2]' is not valid, but '[34.0, 2]' and '[34, 2]'
    # are valid
    spatial_value = spatial_value.replace(".,", ",").replace(".]", "]")
    # '-98, 29, -83, 35.' is not valid
    if spatial_value != "" and spatial_value[-1] == ".":
        spatial_value = spatial_value[0:-1]
    # all leading 0s, '[-089.63,  30.36]' is not valid, '[-89.63,  30.36]' is valid
    spatial_value = re.sub(
        r"(^|\s)(-?)0+((0|[1-9][0-9]*)(\.[0-9]*)?)", r"\1\2\3", spatial_value
    )

    # if spatial is a space-separated number list, try replacing with commas
    # for parsing later
    try:
        numbers_with_spaces = [int(i) for i in spatial_value.split(" ")]
        if all(isinstance(x, int) for x in numbers_with_spaces):
            spatial_value = spatial_value.replace(" ", ",")
    except ValueError:
        pass

    # If we have 4 numbers separated by commas, transform them as GeoJSON
    parts = spatial_value.strip().split(",")
    if len(parts) == 4 and all(is_number(x) for x in parts):
        minx, miny, maxx, maxy = parts
        params = {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}
        new_spatial = geojson_polygon_tpl.format(**params)
        return new_spatial
    # If we have 2 numbers separated by commas, transform them as GeoJSON
    elif len(parts) == 2 and all(is_number(x) for x in parts):
        x, y = parts
        new_spatial = geojson_point_tpl.format(**{"x": x, "y": y})
        return new_spatial

    # Analyze with type of data is JSON valid
    try:
        geometry = json.loads(spatial_value)  # NOQA F841
        # If we have 2 lists of 2 numbers, transform them as GeoJSON
        if isinstance(geometry, list) and len(geometry) == 2:
            min, max = geometry
            params = {"minx": min[0], "miny": min[1], "maxx": max[0], "maxy": max[1]}
            spatial_value = geojson_polygon_tpl.format(**params)
            return spatial_value
    # ruff: noqa: E722
    except:
        pass

    return ""


def translate_spatial(input) -> str:
    # is it already JSON? If so stringify it
    if isinstance(input, dict):
        spatial_value = json.dumps(input)
    elif isinstance(input, str):
        spatial_value = input
    else:
        # This shouldn't happen due to validation, but just in case
        return ""
    # Is it already valid geojson (or geojson that can be cleaned up)?
    # If so, return it.
    validated_geojson = validate_geojson(spatial_value)
    if validated_geojson:
        return validated_geojson

    # is it a name in the locations database?
    res = db.get_geo_from_string(spatial_value)
    if res is not None:
        return res

    # can we reasonably create a geojson from the string input?
    if isinstance(spatial_value, str):
        return munge_spatial(spatial_value)

    # If unable to create a valid geojson, return an empty string
    return ""
