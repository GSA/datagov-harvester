import ipaddress
import json
import logging
import os
import socket
import time
from urllib.parse import urljoin, urlparse

import requests
from jsonschema import Draft202012Validator, FormatChecker

from app.constants import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB
from harvester.utils.general_utils import (
    USER_AGENT,
    assemble_validation_errors,
    build_dcatus3_validator,
    open_json,
)
from harvester.utils.schema_paths import DCATUS1_1_DIR, DCATUS3_DEFINITIONS_DIR

logger = logging.getLogger("harvest_admin_utils")

IS_PROD = os.getenv("FLASK_ENV") == "production"


# Helper Functions
def make_new_source_contract(form):
    collection_parent_url = None
    if form.source_type.data == "waf-collection":
        collection_parent_url = form.collection_parent_url.data

    return {
        "organization_id": form.organization_id.data,
        "name": form.name.data,
        "url": form.url.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "collection_parent_url": collection_parent_url,
        "notification_frequency": form.notification_frequency.data,
        "send_report_email": form.send_report_email.data == "True",
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
        "severity",
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
    # Convert empty string to None for code_repo_url
    code_repo_url = form.code_repo_url.data
    if code_repo_url:
        code_repo_url = code_repo_url.strip() or None
    else:
        code_repo_url = None

    return {
        "name": form.name.data,
        "slug": form.slug.data,
        "logo": form.logo.data or None,
        "description": form.description.data or None,
        "organization_type": form.organization_type.data or None,
        "aliases": [alias.strip() for alias in (form.aliases.data or "").split(",")],
        "code_repo_url": code_repo_url,
        "code_repo_exempt": form.code_repo_exempt.data or False,
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


# The URL path is fetched server-side, so MAX_CONTENT_LENGTH never sees it. Same
# limit, enforced here by hand.
PAYLOAD_TOO_LARGE_MESSAGE = (
    f"JSON payload too large - must be {MAX_UPLOAD_MB}MB or less."
)

# Bounded well inside every ceiling upstream of us: nginx's proxy_read_timeout
# (110s, proxy/nginx-common.conf), gunicorn's 120s worker timeout, and a
# client-facing limit somewhere near 30s that shows up as 499s in the proxy
# log (whether that one is CloudFront or the CF router is unconfirmed).
# Answering well before any of them means a slow or hung target produces a
# clean, logged rejection instead of us being disconnected mid-request.
#
# It is also a capacity limit, not just a latency one: gunicorn runs 3 workers
# with 1 thread each, so each in-flight fetch holds one of only 3 concurrent
# slots per instance. The connect phase gets a tighter budget of its own
# because a dead, typo'd, or firewalled host is almost always a connect
# failure, and there's no reason to hold a slot for the full budget to learn
# that.
FETCH_TIMEOUT_SECONDS = 10
FETCH_CONNECT_TIMEOUT_SECONDS = 5

# requests.get(..., allow_redirects=True) (the default) follows redirects
# without re-checking the target, so a URL that itself resolves to a public IP
# could still redirect us to an internal address the check below would have
# blocked. Each hop is re-validated by hand instead; capped so a redirect loop
# can't hang the request.
MAX_FETCH_REDIRECTS = 5
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}

UNEXPECTED_FETCH_ERROR_MESSAGE = (
    "Could not retrieve the catalog from that URL. Check the URL and try again."
)


class InvalidCatalogSource(ValueError):
    """A submission was refused for a reason the submitter can act on:
    unsupported scheme, internal address, oversized body, not JSON, unparseable
    JSON, too many redirects, or a timeout.

    Both callers render `str()` of this straight back to the submitter, so the
    message must stay safe to disclose - never build one from an underlying
    exception's text or a stack trace (CodeQL py/stack-trace-exposure).
    Anything we *didn't* anticipate should stay an ordinary exception so
    callers answer with UNEXPECTED_FETCH_ERROR_MESSAGE and log the detail
    instead.

    Subclasses ValueError so existing callers catching ValueError still do.
    """


def _validate_fetch_target(url: str) -> None:
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise InvalidCatalogSource("Only HTTP/HTTPS URLs are allowed.")

    if not parsed.hostname:
        raise InvalidCatalogSource("Invalid URL.")

    if not is_public_ip(parsed.hostname) and IS_PROD:
        raise InvalidCatalogSource(
            "Access to private/internal addresses is not allowed."
        )


def fetch_json_from_url(url: str) -> dict:
    # The URL itself isn't sensitive - it's a pointer to a public DCAT catalog,
    # which is the whole point of this feature - so it's safe to log, unlike
    # pasted/uploaded catalog content. Logged up front so a hung or rejected
    # fetch still shows which URL was responsible.
    logger.info("Validator fetching url=%s", url)

    _validate_fetch_target(url)

    # One deadline for the whole operation, not a fresh FETCH_TIMEOUT_SECONDS
    # per redirect hop - otherwise a chain of MAX_FETCH_REDIRECTS redirects,
    # each just under the timeout, could run for
    # MAX_FETCH_REDIRECTS x FETCH_TIMEOUT_SECONDS in total, defeating the
    # point of bounding this at all.
    deadline = time.monotonic() + FETCH_TIMEOUT_SECONDS

    def _remaining_budget() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise requests.exceptions.Timeout(
                f"Exceeded the {FETCH_TIMEOUT_SECONDS}s fetch budget"
            )
        return remaining

    response = None
    try:
        for _ in range(MAX_FETCH_REDIRECTS + 1):
            remaining = _remaining_budget()
            response = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                stream=True,
                timeout=(
                    min(FETCH_CONNECT_TIMEOUT_SECONDS, remaining),
                    remaining,
                ),
                allow_redirects=False,
            )
            if response.status_code not in _REDIRECT_STATUS_CODES:
                break

            location = response.headers.get("Location")
            response.close()
            if not location:
                raise InvalidCatalogSource("Redirected without a Location header.")

            url = urljoin(url, location)
            _validate_fetch_target(url)
        else:
            raise InvalidCatalogSource("Too many redirects.")

        response.raise_for_status()

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_UPLOAD_BYTES:
            raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)

        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            raise InvalidCatalogSource("URL did not return JSON.")

        chunks = []
        total_size = 0
        for chunk in response.iter_content(chunk_size=8192):
            # A response trickling in just under requests' own per-read
            # timeout could otherwise stay within budget on every individual
            # read while still blowing past FETCH_TIMEOUT_SECONDS overall.
            _remaining_budget()
            if chunk:
                total_size += len(chunk)
                if total_size > MAX_UPLOAD_BYTES:
                    raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
                chunks.append(chunk)
    except requests.exceptions.Timeout:
        logger.warning(
            "Validator URL fetch timed out after %ss url=%s",
            FETCH_TIMEOUT_SECONDS,
            url,
        )
        raise InvalidCatalogSource(
            f"The URL took longer than {FETCH_TIMEOUT_SECONDS} seconds to "
            "respond. Check that the URL is correct and the server is up."
        )
    except InvalidCatalogSource:
        raise
    except Exception as e:
        # Connection refused, DNS failure, TLS error, bad status - the
        # submitter can act on these, but requests' own text can carry
        # internals, so log it and answer with a fixed message.
        logger.warning("Validator URL fetch failed url=%s error=%s", url, repr(e))
        raise InvalidCatalogSource(UNEXPECTED_FETCH_ERROR_MESSAGE)
    finally:
        if response is not None:
            response.close()

    content = b"".join(chunks)

    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        # The one exception to keeping underlying exception text out of these
        # messages: a decode error describes only the submitted document
        # ("Expecting ',' delimiter: line 5 column 3"), which is exactly what
        # the submitter needs and reveals nothing about us.
        raise InvalidCatalogSource(f"Invalid JSON: {str(e)}")


class CatalogTooDeeplyNested(ValueError):
    """
    Catalog's `catalog` and `hasPart` are `items: {"$ref": "#"}`, which jsonschema
    resolves by recursion, so a chain of nested catalogs exhausts the stack at ~17KB
    (depth 200 fails, 150 does not). Raised so callers can say so instead of 500ing.
    """


NESTING_TOO_DEEP_MESSAGE = (
    "Catalog is nested too deeply to validate. "
    "Flatten the nested catalog or hasPart chains and try again."
)


def _validation_messages(validator, document: dict) -> list:
    try:
        errors = assemble_validation_errors(validator.iter_errors(document))
    except RecursionError:
        # `from None`: the stack trace is jsonschema's ref resolution, not a cause
        # the submitter can act on.
        raise CatalogTooDeeplyNested(NESTING_TOO_DEEP_MESSAGE) from None

    return [e.message for e in errors]


def validate_records(dcatus_catalog: dict, schema_name: str) -> list:
    """
    validates records from the input dcatus catalog based on the provided schema_name

    raises CatalogTooDeeplyNested if the document is too deeply nested to walk.
    """

    output = []

    schemas = {
        "dcatus1.1: federal dataset": DCATUS1_1_DIR / "federal_dataset.json",
        "dcatus1.1: non-federal dataset": DCATUS1_1_DIR / "non-federal_dataset.json",
        "dcatus3.0 catalog": DCATUS3_DEFINITIONS_DIR,
    }

    schema = schemas[schema_name]

    if schema_name.startswith("dcatus1.1"):
        validator = Draft202012Validator(
            open_json(schema), format_checker=FormatChecker()
        )

        for idx, record in enumerate(dcatus_catalog["dataset"]):
            errors = _validation_messages(validator, record)
            identifier = idx if "identifier" not in record else record["identifier"]
            output += list(zip([identifier] * len(errors), errors))
    else:
        validator = build_dcatus3_validator(schema)
        errors = _validation_messages(validator, dcatus_catalog)
        # not going to pull the record identifier from the error message for now.
        # the json path will clearly indicate which dataset is
        # wrong (e.g. $.dataset[0] )
        output += list(zip([""] * len(errors), errors))

    return output
