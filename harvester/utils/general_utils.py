import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Union

import requests
import sansjson
from bs4 import BeautifulSoup

FREQUENCY_ENUM = {"daily": 1, "weekly": 7, "biweekly": 14, "monthly": 30}


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
            return resp.content
        return resp.json()


def traverse_waf(
    url, files=[], file_ext=".xml", folder="/", filters=["../", "dcatus/"]
):
    """Transverses WAF
    Please add docstrings
    """
    # TODO: add exception handling
    parent = os.path.dirname(url.rstrip("/"))

    folders = []

    res = requests.get(url)
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, "html.parser")
        anchors = soup.find_all("a", href=True)

        for anchor in anchors:
            if (
                anchor["href"].endswith(folder)  # is the anchor link a folder?
                and not parent.endswith(
                    anchor["href"].rstrip("/")
                )  # it's not the parent folder right?
                and anchor["href"] not in filters  # and it's not on our exclusion list?
            ):
                folders.append(os.path.join(url, anchor["href"]))

            if anchor["href"].endswith(file_ext):
                # TODO: just download the file here
                # instead of storing them and returning at the end.
                files.append(os.path.join(url, anchor["href"]))

    for folder in folders:
        traverse_waf(folder, files=files, filters=filters)

    return files


def download_waf(files):
    """Downloads WAF
    Please add docstrings
    """
    output = []
    for file in files:
        output.append({"url": file, "content": download_file(file, ".xml")})

    return output


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
            data_dict[field].append(item.get(field))
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
