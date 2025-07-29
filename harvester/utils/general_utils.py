import argparse
import hashlib
import json
import logging
import os
import re
import smtplib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Union
from urllib.parse import urljoin

import geojson_validator
import requests
import sansjson
from bs4 import BeautifulSoup
from jsonschema.exceptions import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

FREQUENCY_ENUM = {"daily": 1, "weekly": 7, "biweekly": 14, "monthly": 30}


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


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


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
    headers = {
        "User-Agent": "HarvesterBot/0.0 (https://data.gov; datagovhelp@gsa.gov) Data.gov/2.0"
    }
    resp = requests.get(url, headers=headers)
    if 200 <= resp.status_code < 300:
        if file_type == ".xml":
            data = resp.content
            if isinstance(data, bytes):
                data = data.decode()
            return data
        return resp.json()

    raise Exception


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
        "ckan_name": record.ckan_name,
    }


def find_indexes_for_duplicates(records: list):
    """
    output is a list of integers representing element positions of
    duplicates records. this list is then used to record duplicate
    record errors in the db and to remove from self.external_records.
    sorting it in reverse (.sort edits in place) places the largest
    numbers first. this is necessary to avoid index shifting
    when you're deleting from a list.

    scenario without sorting output
        positions = [ 1, 3 ]
        data = [ 0, 0, 1, 1 ]

        deleting data[1] can shift the data since we're mutating the list.
        what happens when we try to del data[3] on [0, 1, 1]?
    """
    seen = set()
    output = []
    for i in range(len(records)):
        identifier = records[i]["identifier"]
        if identifier in seen:
            output.append(i)
        seen.add(identifier)
    output.sort(reverse=True)  # avoid index shifting

    return output


def get_waf_datetimes(soup: BeautifulSoup, expected_length: int) -> list:
    output = []

    dt_placeholder = datetime(1900, 1, 1, 0, 0)
    dt_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}"
    dt_format = "%Y-%m-%d %H:%M"

    for td in soup.find_all("td"):
        res = re.search(dt_pattern, td.text)
        if res is not None:
            output.append(datetime.strptime(res.group(0), dt_format))

    if len(output) != expected_length:
        logger.warning(
            f"mismatching datetime ({len(output)}) and file ({expected_length} counts"
        )

    # pad with placeholder when more files than datetimes
    output += [dt_placeholder] * (expected_length - len(output))

    # when more datetimes than files
    output = output[:expected_length]

    return output


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

    res = requests.get(url)

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


def query_filter_builder(base, facets):
    """Builds filter strings from base and comma separated string of filters
    :param base str - base filter query
    :param facets str - extra facets

    """
    facets = facets.removeprefix(", ")
    if base is None:
        facet_string = facets.split(",")[0]
        facet_list = facets.split(",")[1:]
    else:
        facet_string = base
        facet_list = facets.split(",")
    for facet in facet_list:
        if facet != "":
            facet_string += f" AND {facet}"
    return facet_string


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
                value = value.strftime('%Y-%m-%d %H:%M')
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


def create_retry_session() -> requests.Session:
    """Creates a requests session with retry logic for HTTP/S requests."""
    session = requests.Session()
    # 3 Retries with exponential backoff (of 2 seconds) for server errors
    # we want to retry on 500, 502, 503, and 504 errors (CKAN Down)
    retry_strategy = Retry(
        total=3,
        backoff_factor=3,
        backoff_max=15,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
        allowed_methods={"GET", "POST", "PUT", "DELETE"},
    )
    # pass retry strategy to HTTPAdapter
    adapter = HTTPAdapter(max_retries=retry_strategy)
    # tell session which protocols to use the adapter with
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


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


def found_simple_message(validation_error: ValidationError) -> bool:
    """
    determine whether the input validation error represents the most
    succinct cause for error based on its json_path or dtype
    """
    # these are all the unique dtypes found in the
    # non-federal schema (no different than federal)
    # {"'boolean'", "'null'", "'array'", "'number'", "'object'", "'string'"}

    # the required field at the root is missing entirely
    if validation_error.json_path == "$":
        return True

    # dict/object and list/array are the only non-primitives in the schema
    if isinstance(validation_error.instance, (dict, list)):
        # if it's empty you'll get something like
        # ['$.keyword', '[] should be non-empty']
        # which is simple and what we want
        if len(validation_error.instance) == 0:
            return True
        return False
    return True


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
        # required root-level errors are processed individually not bundled by format
        # "'a' is required"
        if json_path == "$":
            output += map(lambda error: ValidationError(f"$, {error}"), messages["$"])
            continue

        # all other errors are bundled based on the formats/rules

        # [0] is the same as [n]
        invalid_value = re.search(r"'(.*?)'|\[\]", formats[0])

        # if the 0th doesn't work none of them will
        if invalid_value is None:
            logger.warning(f"can't find invalid data from error message: {formats[0]}")
            continue

        # here's the exact format (e.g 'uri', 'string', some regex, etc... )
        formats = map(lambda format: format.split(" ")[-1], formats)

        # build the bundled error message by json_path
        msg = ValidationError(
            f"{json_path}, {invalid_value.group(0)} does not match any of "
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
    """

    if messages is None:
        # {'$.distribution[2].title' = ["'' should be non-empty", etc...]}
        messages = defaultdict(list)

    for error in validation_errors:
        if found_simple_message(error):
            # these aren't specific enough which make them unhelpful
            generic_msg = "is not valid under any of the given schemas"
            is_generic_msg = error.message.endswith(generic_msg)
            if not is_generic_msg:
                messages[error.json_path].append(error.message)
        assemble_validation_errors(error.context, messages)

    return finalize_validation_messages(messages)
