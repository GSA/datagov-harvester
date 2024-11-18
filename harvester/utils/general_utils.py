import argparse
import hashlib
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Union

import requests
import sansjson
from bs4 import BeautifulSoup

FREQUENCY_ENUM = {"daily": 1, "weekly": 7, "biweekly": 14, "monthly": 30}


def get_title_from_fgdc(xml_str: str) -> str:
    tree = ET.ElementTree(ET.fromstring(xml_str))
    return tree.find(".//title").text


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog="Harvest Runner", description="etl harvest sources"
    )
    parser.add_argument("jobId", help="job id for harvest job")

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
    resp = requests.get(url)
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
