import logging
import os

import requests
from bs4 import BeautifulSoup
from requests.exceptions import JSONDecodeError, RequestException

logger = logging.getLogger("harvester")


def download_dcatus_catalog(url):
    """download file and pull json from response
    url (str)   :   path to the file to be downloaded.
    """
    try:
        resp = requests.get(url)
    except RequestException as e:
        return Exception(e)
    except JSONDecodeError as e:
        return Exception(e)

    if resp.status_code != 200:
        return Exception("non-200 status code")

    try:
        return resp.json()
    except JSONDecodeError as e:
        return Exception(e)


def traverse_waf(url, files=[], file_ext=".xml", folder="/", filters=[]):
    """Transverses WAF
    Please add docstrings
    """
    # TODO: add exception handling
    parent = os.path.dirname(url.rstrip("/"))

    res = requests.get(url)
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, "html.parser")
        anchors = soup.find_all("a", href=True)

        folders = []
        for anchor in anchors:
            if (
                anchor["href"].endswith(folder)
                and not parent.endswith(anchor["href"].rstrip("/"))
                and anchor["href"] not in filters
            ):
                folders.append(os.path.join(url, anchor["href"]))

            if anchor["href"].endswith(file_ext):
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
        data = {}
        data["url"] = file
        res = requests.get(file)
        if res.status_code == 200:
            data["content"] = res.content
            output.append(data)

    return output


def extract(harvest_source) -> list:
    """Extracts all records from a harvest_source"""
    logger.info("Hello from harvester.extract()")

    datasets = []

    # stub

    return datasets
