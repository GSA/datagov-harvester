from jsonschema import Draft202012Validator, FormatChecker
import socket
import ipaddress
import requests
from urllib.parse import urlparse
from pathlib import Path
import logging
import os
from harvester.utils.general_utils import (
    open_json,
    assemble_validation_errors,
    USER_AGENT,
)

logger = logging.getLogger("harvest_admin_utils")

BASE_DIR = Path(__file__).parents[1]
IS_PROD = os.getenv("FLASK_ENV") == "production"


# Helper Functions
def make_new_source_contract(form):
    return {
        "organization_id": form.organization_id.data,
        "name": form.name.data,
        "url": form.url.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "notification_frequency": form.notification_frequency.data,
    }


def make_new_record_error_contract(error: tuple) -> dict:
    """
    convert the record error row tuple into a dict. splits the validation message
    value into an array
    """
    fields = [
        "harvest_record_id",
        "harvest_job_id",
        "date_created",
        "type",
        "message",
        "id",
    ]

    # identifier and source_raw are the last 2 and kept the same
    record_error = dict(zip(fields, error[:-2]))
    error_type = error[3]
    if error_type in ["ValidationException", "ValidationError"]:
        record_error["message"] = record_error["message"].split("::")  # turn into array

    return record_error


def make_new_org_contract(form):
    return {
        "name": form.name.data,
        "slug": form.slug.data,
        "logo": form.logo.data,
        "description": form.description.data or None,
        "organization_type": form.organization_type.data or None,
        "aliases": [alias.strip() for alias in (form.aliases.data or "").split(",")],
    }


def is_public_ip(hostname: str) -> bool:
    """
    Resolve hostname and ensure all IPs are public.
    Prevents access to:
    - localhost
    - 127.0.0.1
    - 10.x.x.x
    - 192.168.x.x
    - 172.16-31.x.x
    - link-local
    - metadata services
    """
    try:
        addresses = socket.getaddrinfo(hostname, None)
        for addr in addresses:
            ip = addr[4][0]
            ip_obj = ipaddress.ip_address(ip)

            if (
                ip_obj.is_private
                or ip_obj.is_loopback
                or ip_obj.is_reserved
                or ip_obj.is_link_local
                or ip_obj.is_multicast
            ):
                return False
        return True
    except Exception:
        return False


def fetch_json_from_url(url: str) -> dict:

    MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10MB limit

    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError("Only HTTP/HTTPS URLs are allowed.")

    if not parsed.hostname:
        raise ValueError("Invalid URL.")

    if not is_public_ip(parsed.hostname) and IS_PROD:
        raise ValueError("Access to private/internal addresses is not allowed.")

    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
    )

    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise ValueError("URL did not return JSON.")

    content = response.raw.read(MAX_CONTENT_LENGTH + 1)
    if len(content) > MAX_CONTENT_LENGTH:
        raise ValueError("JSON payload too large.")

    return response.json()


def validate_records(dcatus_catalog: dict, schema_name: str) -> list:
    """
    validates records from the input dcatus catalog based on the provided schema_name
    """

    output = []

    schemas = {
        "dcatus1.1: federal dataset": BASE_DIR / "schemas" / "federal_dataset.json",
        "dcatus1.1: non-federal dataset": BASE_DIR
        / "schemas"
        / "non-federal_dataset.json",
    }
    schema = open_json(schemas[schema_name])

    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    for idx, record in enumerate(dcatus_catalog["dataset"]):
        errors = validator.iter_errors(record)
        errors = [e.message for e in assemble_validation_errors(errors)]
        identifier = idx if "identifier" not in record else record["identifier"]
        output += list(zip([identifier] * len(errors), errors))

    return output
