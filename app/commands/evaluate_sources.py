import csv
import logging
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Dict, List, Union
from xml.etree.ElementTree import ParseError

import requests
from requests.exceptions import ConnectionError, JSONDecodeError, Timeout
from requests.models import Response

from harvester.utils.general_utils import get_server_type, traverse_waf

logger = logging.getLogger(__name__)

CATALOG_SOURCE_URL = "https://catalog.data.gov/api/action/package_search"
HEADERS = {
    "User-Agent": (
        "HarvesterBot/0.0 (https://data.gov; datagovhelp@gsa.gov) Data.gov/2.0"
    )
}


def handled_request(
    url: str,
    headers: Dict[str, str],
    timeout: int = 10,
    params: Union[Dict[str, str]] = None,
) -> Union[Response, None]:
    """
    Wrapper function to handle requests.
    """
    response = None
    try:
        if params:
            response = requests.get(
                url, params=params, headers=headers, timeout=timeout
            )
        else:
            response = requests.get(url, headers=headers, timeout=timeout)
    except Timeout:
        logger.error("Timeout Error: %s took to long to respond.", url)
    except ConnectionError:
        logger.error("Connection Error: %s could not be reached.", url)
    return response


def get_sources(rows_per_page: int = 100) -> List[Dict[str, str]]:
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
        response = handled_request(
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
        # Stop if we've fetched all available sources
        if len(sources) < rows_per_page:
            break

        # increment the starting point for the pagination
        start += rows_per_page

    return sources_list


def determine_metadata_type(response: Response) -> str:
    """
    Determine metadata type based on the response.
    Returns: DCAT-US, FGDC, ISO19115-2, ISO19115-1,
    INDETERMINATE (if a parsing error is occured),
    N/A (if the file is not reachable)
    """
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
        # handle broken or "not well-formed" XML
        try:
            xml_tree = ET.fromstring(response.content)
        except ParseError as e:
            logger.error("XML Parsing error: %s", e)
            return "INDETERMINATE"
        # for non iso formats
        if xml_tree.tag == "metadata":
            # potentially FGDC
            if xml_tree.find("metainfo"):
                try:
                    if (
                        xml_tree.find("metainfo").find("metstdv").text
                        == "FGDC-STD-001-1998"
                    ):
                        return "FGDC"
                except AttributeError:
                    # possibly there is no "metainfo"
                    return "N/A"
        # means it may be an ISO19115-X format
        if len(xml_tree.tag.split("}")) > 1:
            tag = xml_tree.tag.split("}")[1]
            if tag == "MI_Metadata":
                return "ISO19115-2"
            elif tag == "MD_Metadata":
                return "ISO19115-1"
        return "N/A"


def evaluate_sources() -> None:
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
        "metadata_types",
        "last_modified",
        "server_type",
    ]
    sources = get_sources()
    timed_out_sources = []
    count = 0

    logger.info("Starting Harvest Source Evaluation!")
    with open("harvest-sources.csv", mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for source in sources:
            response = None
            response = handled_request(
                url=source["source_url"],
                headers=HEADERS,
                timeout=10,
            )
            metadata_types = ["N/A"]
            status_code = "Timeout Error"
            timestamps = []
            last_modified = "N/A"
            server_type = "N/A"

            if response:
                status_code = response.status_code
                server_type = get_server_type(response.headers.get("Server", ""))
                if response.ok:
                    if source["source_type"] in ["waf", "waf-collection"]:
                        # because the metadata type is not exposed in
                        # the waf we need to look at one of the xml files
                        xml_file_urls = traverse_waf(response.url)
                        metadata_types = []
                        for xml_file_url in xml_file_urls[:10]:
                            xml_response = handled_request(
                                url=xml_file_url, headers=HEADERS, timeout=10
                            )
                            if xml_response and xml_response.ok:
                                if xml_response.headers.get("Last-Modified"):
                                    timestamps.append(
                                        xml_response.headers.get("Last-Modified")
                                    )
                                metadata_type = determine_metadata_type(xml_response)
                                metadata_types.append(metadata_type)
                            else:
                                logger.error(
                                    "Failed to fetch %s from %s",
                                    xml_file_url,
                                    response.url,
                                )

                        # get the most recent last modified date
                        if timestamps:
                            last_modified = sorted(
                                timestamps,
                                key=lambda x: parsedate_to_datetime(x),
                                reverse=True,
                            )[0]
                    else:
                        metadata_types = [determine_metadata_type(response)]
                        if metadata_types[0] == "DCAT-US":
                            data = response.json()
                            try:
                                timestamps = [
                                    dataset["modified"] for dataset in data["dataset"]
                                ]
                            except TypeError:
                                pass
                            timestamps.sort(reverse=True)
                            # note that dcat-us does not specify timezone, or time
                            try:
                                last_modified = timestamps[0]
                            except IndexError:
                                response.headers.get("Last-Modified", "N/A")

                        else:
                            last_modified = response.headers.get("Last-Modified", "N/A")

            source_data = {
                "source_id": source["source_id"],
                "source_title": source["source_title"],
                "source_type": source["source_type"],
                "source_url": source["source_url"],
                "organization_id": source["organization_id"],
                "organization_title": source["organization_title"],
                "organization_state": source["organization_state"],
                "status_code": status_code,
                "metadata_types": set(metadata_types),
                "last_modified": last_modified,
                "server_type": server_type,
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
