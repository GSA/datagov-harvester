"""Migrate harvest sources from CKAN to Harvester.

CKAN query URL and the harvest admin URL are saved here as constants.
The harvest admin API requires an API token. It's loaded here from an
environment variable API_TOKEN.

The harvester instance that is being targeted can be set with the environment
variable HARVESTER_BASE_URL with a default value of "https://datagov-harvest-admin-dev.app.cloud.gov/"
"""

import logging
import os

import click
from requests import get, post
from requests.exceptions import RequestException

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

HARVEST_QUERY = (
    "https://catalog.data.gov/api/action/package_search?fq=(dataset_type:harvest)"
)
HARVESTER_BASE_URL = os.environ.get(
    "HARVESTER_BASE_URL", "https://datagov-harvest-admin-dev.app.cloud.gov/"
)
UPLOAD_QUERY = HARVESTER_BASE_URL + "harvest_source/add"
ORGANIZATION_ADD_QUERY = HARVESTER_BASE_URL + "organization/add"
ORGANIZATION_QUERY = HARVESTER_BASE_URL + "organization/"

_API_TOKEN = os.environ.get("API_TOKEN")
AUTH_HEADERS = {"Authorization": _API_TOKEN}


def get_count():
    """Get the count of harvest sources."""
    click.echo("Getting number of harvest sources...")
    data = get(HARVEST_QUERY + "&rows=0").json()
    return data["result"]["count"]


def get_sources(count):
    """Get the harvest source information."""
    click.echo("Getting harvest source information...")
    data = get(HARVEST_QUERY + f"&rows={count}&start=0").json()
    return data["result"]["results"]


def _org_to_upload(org_data):
    return {
        "id": org_data["id"],
        "name": org_data["title"],
        "logo": org_data["image_url"],
    }


def ensure_organization(org_data):
    """Ensure that the organization from a harvest source exists.

    We won't look before we leap. Try to add the organization and
    handle any failures.

    Return any organization information we got from queries.
    """
    logger.debug(
        "Ensuring organization %s(%s) exists", org_data["title"], org_data["id"]
    )
    upload_data = _org_to_upload(org_data)
    logger.debug("Creating organization with data %s", upload_data)
    result = post(ORGANIZATION_ADD_QUERY, json=upload_data, headers=AUTH_HEADERS)
    logger.debug("%s: %s", result.status_code, result.text)
    if result.status_code == 200:
        # add succeeded, return the data we used
        return result.json()
    else:
        # add failed, figure out what's going on
        logger.debug("Failed to add organization...")
        result = get(
            ORGANIZATION_QUERY + f"{upload_data['id']}",
            headers=AUTH_HEADERS | {"Content-type": "application/json"},
        )
        logger.debug("%s: %s", result.status_code, result.text)
        return result.json()


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
        # TODO: can we actually determine this now?
        organization_types = [
            extra["value"]
            for extra in org.get("extras", [])
            if extra["key"] == "organization_type"
        ]
        if organization_types:
            organization_type = organization_types[0]
        else:
            organization_type = None

        if organization_type == "Federal Government":
            return "dcatus1.1: federal"
        return "dcatus1.1: non-federal"

    return {
        "source_type": _source_type,
        "schema_type": _schema_type,
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
        # no equivalent in CKAN, choose a sane default
        "notification_frequency": "always",
    }


def _upload_source(source):
    """Upload a single source to the harvester."""
    click.echo(f"Uploading source data for url {source['url']}")
    logger.debug("Source data: %s", source)

    # sources need organizations to exist in order to be created
    # save the organization details we got so that we can use them
    org_details = ensure_organization(source["organization"])

    # We need information about the organization to create the correct harvest
    # source (federal or not and email notification list)
    upload_data = _source_to_upload(source, org=org_details)
    logger.debug("Creating harvest source with data: %s", upload_data)

    try:
        result = post(UPLOAD_QUERY, json=upload_data, headers=AUTH_HEADERS)
    except RequestException as e:
        logger.error(e)
        return False

    click.echo(f"{result.status_code}: {result.text}")
    if result.status_code != 200:
        return False

    return True


def upload_sources(sources):
    """Upload sources to the new harvester.

    Returns a list of the same length as sources with True/False success
    markers.
    """
    click.echo("Uploading sources...")
    results = list(map(_upload_source, sources))
    return results


@click.command()
@click.option("-l", "--limit", default=0)
@click.option("-d", "--debug", is_flag=True, default=False)
def migrate_source(limit=0, debug=False):
    if debug:
        logger.setLevel(logging.DEBUG)

    if not limit:
        count = get_count()
    else:
        count = limit

    click.echo(f"Looking for {count} harvest sources.")
    sources = get_sources(count)
    click.echo(f"Got {len(sources)} harvest sources.")
    results = upload_sources(sources)
    click.echo(f"Uploaded {sum(results)}/{len(results)} sources")


if __name__ == "__main__":
    migrate_source()
