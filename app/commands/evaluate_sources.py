import csv
import logging
import xml.etree.ElementTree as ET
from typing import Dict, List

import requests
from requests.exceptions import JSONDecodeError, Timeout
from requests.models import Response

from harvester.utils.general_utils import traverse_waf

logger = logging.getLogger(__name__)

CATALOG_SOURCE_URL = "https://catalog.data.gov/api/action/package_search"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0 Safari/537.36"
    )
}


def get_source_urls(rows_per_page: int = 100) -> List[Dict[str, str]]:
    """
    Retrieves all of the harvest sources then parses the data
    to return a list dictionaries with: a source's id, title, source_type,
    url, org, org state, and owner org id.
    """
    sources_list = []
    start = 0

    while True:
        # Scrape through the API end point that provides the list of harvest sources.
        params = {"start": start, "rows": rows_per_page, "fq": "dataset_type:harvest"}
        response = requests.get(
            CATALOG_SOURCE_URL, params=params, headers=HEADERS, timeout=10
        )

        data = response.json()
        sources = data["result"]["results"]

        # loop through the return and break out the infomation we want
        for source in sources:
            sources_list.append(
                {
                    "source_id": source["id"],
                    "source_title": source["title"],
                    "source_type": source["source_type"],
                    "source_url": source["url"],
                    "organization_id": source["organization"]["id"],
                    "organization_title": source["organization"]["title"],
                    "organization_state": source["organization"]["state"],
                }
            )

        logger.info(
            "Fetched %s sources (total so far: %s)", len(sources), len(sources_list)
        )
        break
        # Stop if we've fetched all available sources
        if len(sources) < rows_per_page:
            break

        # increment the starting point for the pagination
        start += rows_per_page

    return sources_list


def determine_metadata_type(response: Response) -> str:
    """Determin metadata type based on the response."""
    # does some minor clean up wher `utf-8` will be a part of the header
    content_type = response.headers.get("Content-Type", "").split(";")[0]

    # if there's no connection we can skip and return "n/a"
    if not response.ok:
        return "N/A"

    # sometimes we may get an octetstream in json format (so far)
    # the attempt to parse it is used to check if it's json.
    json = None
    try:
        json = response.json()
    except JSONDecodeError:
        pass

    # if it's JSON it's going to be DCAT-US
    if content_type == "application/json" or json:
        return "DCAT-US"
    if content_type in ["application/xml", "text/xml"]:
        xml_tree = ET.fromstring(response.content)
        # for non iso formats
        if xml_tree.tag == "metadata":
            # potentially FGDC
            if xml_tree.find("metainfo"):
                if (
                    xml_tree.find("metainfo").find("metstdv").text
                    == "FGDC-STD-001-1998"
                ):
                    return "FGDC"
        # means it may be an ISO19115-X format
        tag = xml_tree.tag.split("}")[1]
        if tag == "MI_Metadata":
            return "ISO19115-2"
        elif tag == "MD_Metadata":
            return "ISO19115-1"
    else:
        return ""


def evaluate_sources():
    """
    Function used to gather and then evaluate
    harvest resources to check that they're healthy.
    """
    fieldnames = [
        "source_id",
        "source_title",
        "source_type",
        "source_url",
        "organization_id",
        "organization_title",
        "organization_state",
        "status_code",
        "metadata_type",
    ]
    sources = get_source_urls()[:36]
    timed_out_sources = []
    count = 0

    logger.info("Starting Harvest Source Evaluation!")
    with open("harvest-sources.csv", mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for source in sources:
            response = None
            try:
                response = requests.get(
                    source["source_url"],
                    headers=HEADERS,
                    timeout=10,
                )
            except Timeout:
                logger.error(
                    "Timeout Error: %s took to long to respond.", source["source_url"]
                )

            metadata_type = "N/A"
            status_code = "Timeout Error"

            if response:
                status_code = response.status_code
                if response.ok:
                    if source["source_type"] in ["waf", "waf-collection"]:
                        # because the metadata type is not exposed in
                        # the waf we need to look at one of the xml files
                        xml_file_url = traverse_waf(response.url)[0]
                        xml_response = requests.get(xml_file_url, headers=HEADERS)
                        metadata_type = determine_metadata_type(xml_response)
                    else:
                        metadata_type = determine_metadata_type(response)

            source_data = {
                "source_id": source["source_id"],
                "source_title": source["source_title"],
                "source_type": source["source_type"],
                "source_url": source["source_url"],
                "organization_id": source["organization_id"],
                "organization_title": source["organization_title"],
                "organization_state": source["organization_state"],
                "status_code": status_code,
                "metadata_type": metadata_type,
            }

            # capture timed out sources
            if status_code == "Timeout Error":
                timed_out_sources.append(source_data)

            writer.writerow(source_data)
            count += 1
            logger.info("Completed %s/%s", count, len(sources))

        # After the script is done output the Timed Out sources as a dict
        if timed_out_sources:
            logger.error("%s Sources Timed out", len(timed_out_sources))
            logger.error("The following sources timed out: %s", timed_out_sources)
