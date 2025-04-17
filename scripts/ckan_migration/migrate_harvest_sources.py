#!/usr/bin/env python3
"""Migrate harvest sources from CKAN to Harvester.

The CKAN URL is hardcoded here as a constant.  The harvester instance that is
being targeted can be specified with the `--harvester-url` option and defaults
to "https://datagov-harvest-admin-dev.app.cloud.gov/".  The harvest admin API
and new CKAN  API require API tokens which must be provided with the
`--harvester-api-token` and `--new-catalog-api-token` arguments.

"""

import logging
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from types import SimpleNamespace

import click
from requests import get, post
from requests.exceptions import RequestException

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# global config object here so we can set the values from the CLI and then use
# them in our methods.
CONFIG = SimpleNamespace()

CONFIG.ckan_harvest_source_query = (
    "https://catalog.data.gov/api/action/package_search?fq=(dataset_type:harvest)"
)
CONFIG.ckan_organization_show = "https://catalog.data.gov/api/action/organization_show"
CONFIG.harvester_url = "https://datagov-harvest-admin-dev.app.cloud.gov/"
CONFIG.new_catalog_url = "https://catalog-next-dev-datagov.app.cloud.gov/"


def get_count():
    """Get the count of harvest sources."""
    click.echo("Getting number of harvest sources...")
    data = get(CONFIG.ckan_harvest_source_query + "&rows=0").json()
    return data["result"]["count"]


def get_sources(count):
    """Get the harvest source information."""
    click.echo("Getting harvest source information...")
    data = get(CONFIG.ckan_harvest_source_query + f"&rows={count}&start=0").json()
    return data["result"]["results"]


def _org_to_upload(org_data):
    return {
        "id": org_data["id"],
        "name": org_data["title"],
        "logo": org_data["image_url"],
    }


def _ensure_org_harvester(org_data):
    """Ensure organization exists in harvester."""
    result = get(
        CONFIG.harvester_url + f"organization/{org_data['id']}",
        headers=CONFIG.harvester_auth_headers | {"Content-type": "application/json"},
    )
    logger.debug("%s: %s", result.status_code, result.text)
    if result.status_code == 200:
        # organization existed in harvester
        return True
    else:
        upload_data = _org_to_upload(org_data)
        logger.debug("Creating harvester organization with data %s", upload_data)
        result = post(
            CONFIG.harvester_url + "organization/add",
            json=upload_data,
            headers=CONFIG.harvester_auth_headers,
        )
        logger.debug("%s: %s", result.status_code, result.text)
        if result.status_code == 200:
            # add succeeded, return the data we used
            return True
        else:
            # add failed, figure out what's going on
            logger.debug("Failed to add organization.")
            return False


def _ensure_org_new_catalog(org_data):
    """Ensure organization exists in new CKAN catalog."""
    result = post(
        CONFIG.new_catalog_url + "api/action/organization_show",
        headers=CONFIG.new_catalog_auth_headers,
        json={"id": org_data["id"]},
    )
    logger.debug("%s: %s", result.status_code, result.text)
    if result.status_code == 200:
        # organization existed in new catalog
        return True
    else:
        logger.debug("Creating new CKAN organization with data %s", org_data)
        result = post(
            CONFIG.new_catalog_url + "api/action/organization_create",
            json=org_data,
            headers=CONFIG.new_catalog_auth_headers,
        )
        logger.debug("%s: %s", result.status_code, result.text)
        if result.status_code == 200:
            # add succeeded, return the data we used
            return True
        else:
            # add failed, figure out what's going on
            logger.debug("Failed to add organization to new catalog.")
            return False


def ensure_organization(org_data):
    """Ensure that the organization from a harvest source exists.

    There are many sources with the same organization, so check first if
    the org exists and only add it if it doesn't exist. Organizations need
    to exist in both the harvester and the new CKAN catalog.

    Returns True if the organization now exists or False if something went
    wrong.
    """
    logger.debug(
        "Ensuring organization %s(%s) exists", org_data["title"], org_data["id"]
    )
    return _ensure_org_harvester(org_data) and _ensure_org_new_catalog(org_data)


def _get_extra_named(org_dict, name):
    """Get the value of an extra by name.

    Extras are a list of dicts inside of org_dict each with a "key" and a
    corresponding "value". This gets the value for a corresponding key
    if it exists. If the key occurs multiple times we return the first value.

    If the key doesn't exist, we return None.
    """
    values = [
        extra["value"] for extra in org_dict.get("extras", []) if extra["key"] == name
    ]
    if values:
        return values[0]
    else:
        return None


def _derive_source_fields():
    """Dict of functions that derive harvester fields from CKAN data.

    The keys of the dict are the field names in Harvester and the values
    are functions that take the source's CKAN dict and return
    the value of that field. Mapper functions can take either one argument
    with just the source information or two for source and organization
    information.
    """

    def _source_type(source):
        if source["source_type"].startswith("waf"):
            return "waf"
        return "document"

    def _schema_type(source, org):
        organization_type = _get_extra_named(org, "organization_type")

        if source["source_type"] == "datajson":
            if organization_type == "Federal Government":
                return "dcatus1.1: federal"
            return "dcatus1.1: non-federal"
        elif source["source_type"].startswith("waf"):
            # WAF sources have auto-detected schemas in CKAN, so just choose
            # this is our most common WAF schema
            return "iso19115_2"
        else:
            # this cannot be empty, so choose a default
            return "dcatus1.1: non-federal"

    def _notification_emails(source, org):
        email_list = _get_extra_named(org, "email_list")
        if email_list is None:
            return []
        # email_list is whitespace (\r\n) delimited in CKAN. We want a real
        # list.
        return email_list.split()

    return {
        "source_type": _source_type,
        "schema_type": _schema_type,
        "notification_emails": _notification_emails,
    }


def _source_to_upload(source, org=None):
    """Transform CKAN data into appropriate data for Harvester creation.

    We need information about the organization to make the correct harvest
    source. `org` is a dict from CKAN's organization_show.
    """

    mapper = _derive_source_fields()

    return {
        "organization_id": source["owner_org"],
        "name": source["name"],
        "url": source["url"],
        "frequency": source["frequency"].lower(),
        "source_type": mapper["source_type"](source),
        "schema_type": mapper["schema_type"](source, org),
        "notification_emails": mapper["notification_emails"](source, org),
        # no equivalent in CKAN, choose a sane default
        "notification_frequency": "always",
    }


@cache
def _get_org_details(org_id):
    """Get additional information from CKAN for an organization.

    We memo-ize this function so we don't have to repeat slow calls to CKAN.
    """
    try:
        org_details = post(CONFIG.ckan_organization_show, json={"id": org_id}).json()[
            "result"
        ]  # if this fails CKAN data has problems, let the exception happen
        logger.debug("CKAN organization info: %s", org_details)
    except Exception as e:
        logger.error("Failed to get CKAN organization info: %s", e)
        org_details = {}
    return org_details


def _upload_source(source):
    """Upload a single source to the harvester.

    Returns True if upload was successful, False if not.
    """
    click.echo(f"Uploading source data for url {source['url']}")
    logger.debug("Source data: %s", source)

    # sources need organizations to exist in order to be created
    org_exists = ensure_organization(source["organization"])
    if not org_exists:
        return False

    # We need information about the organization to create the correct harvest
    # source (federal or not and email notification list)
    org_details = _get_org_details(source["owner_org"])

    upload_data = _source_to_upload(source, org=org_details)
    logger.debug("Creating harvest source with data: %s", upload_data)

    try:
        result = post(
            CONFIG.harvester_url + "harvest_source/add",
            json=upload_data,
            headers=CONFIG.harvester_auth_headers,
        )
    except RequestException as e:
        logger.error(e)
        return False

    logger.debug(f"{result.status_code}: {result.text}")
    if result.status_code != 200:
        return False

    return True


def upload_sources(sources):
    """Upload sources to the new harvester.

    Returns a list of the same length as sources with True/False success
    markers.
    """
    click.echo("Uploading sources...")
    with ThreadPoolExecutor(max_workers=CONFIG.workers) as executor:
        results = list(executor.map(_upload_source, sources))
    return results


@click.command()
@click.option("-l", "--limit", default=0, help="Migrate only this many sources")
@click.option("-d", "--debug", is_flag=True, default=False)
@click.option(
    "--harvester-url", default=None, help="Base URL for the Harvester to migrate to"
)
@click.option(
    "--new-catalog-url", default=None, help="Base URL for the new CKAN catalog"
)
@click.option(
    "--harvester-api-token", required=True, help="API token for the Harvester"
)
@click.option(
    "--new-catalog-api-token", required=True, help="API token for the new CKAN catalog"
)
@click.option(
    "-w", "--workers", default=None, type=int, help="Number of thread workers to use"
)
def migrate_source(
    harvester_api_token,
    new_catalog_api_token,
    limit=0,
    debug=False,
    harvester_url=None,
    new_catalog_url=None,
    workers=None,
):
    if harvester_api_token:
        CONFIG.harvester_auth_headers = {"Authorization": harvester_api_token}
    else:
        logger.error("Please set a non-empty api_token.")
        return 1

    if new_catalog_api_token:
        CONFIG.new_catalog_auth_headers = {"Authorization": new_catalog_api_token}
    else:
        logger.error("Please set a non-empty api_token.")
        return 1

    if debug:
        logger.setLevel(logging.DEBUG)

    if harvester_url is not None:
        CONFIG.harvester_url = harvester_url
    if not CONFIG.harvester_url.endswith("/"):
        CONFIG.harvester_url += "/"

    if new_catalog_url is not None:
        CONFIG.new_catalog_url = new_catalog_url
    if not CONFIG.new_catalog_url.endswith("/"):
        CONFIG.new_catalog_url += "/"

    if not limit:
        count = get_count()
    else:
        count = limit

    CONFIG.workers = workers

    click.echo(f"Looking for {count} harvest sources.")
    sources = get_sources(count)
    click.echo(f"Got {len(sources)} harvest sources.")
    results = upload_sources(sources)
    click.echo(f"Uploaded {sum(results)}/{len(results)} sources")


if __name__ == "__main__":
    migrate_source()
