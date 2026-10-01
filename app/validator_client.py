"""Client for the datagov-validator API (GSA/datagov-validator).

The /validate/ page renders the form and its results; fetching and validating
the catalog happen in that service, so its server-side URL fetching and
validation load stay out of this app.
"""

import logging
import os

import requests

logger = logging.getLogger("harvest_admin")

# The validator spends at most 10s fetching a URL submission, and validating a
# document at the 10MB cap takes seconds, not tens of seconds. Bounded inside
# the proxy's 110s proxy_read_timeout and gunicorn's 120s worker timeout, so a
# hung validator produces a clean error on the page rather than a 504.
VALIDATOR_CONNECT_TIMEOUT_SECONDS = 5
VALIDATOR_TIMEOUT_SECONDS = 60

VALIDATOR_UNAVAILABLE_MESSAGE = (
    "The validator is unavailable right now. Please try again later."
)


class ValidatorRefused(Exception):
    """The validator refused the submission and said why.

    `str()` is the validator's own message - a timeout, an internal address, an
    oversized or unparseable document. It builds those from literals and
    numbers only so they can be shown to the submitter.
    """


class ValidatorUnavailable(Exception):
    """The validator couldn't be reached or answered unexpectedly. Log `str()`,
    show VALIDATOR_UNAVAILABLE_MESSAGE."""


def _request_errors(body: dict) -> str | None:
    """APIFlask's 422 for a request the validator's schema rejected, e.g. a URL
    this app's form accepts but the validator doesn't. Its messages are
    marshmallow's own literals ("Not a valid URL.")."""
    detail = body.get("detail")
    if not isinstance(detail, dict) or not isinstance(detail.get("json"), dict):
        return None

    messages = [
        message
        for field_messages in detail["json"].values()
        if isinstance(field_messages, list)
        for message in field_messages
        if isinstance(message, str)
    ]
    return " ".join(messages) or None


def validate_catalog(
    schema: str, fetch_method: str, url: str = None, json_text: str = None
) -> list:
    """
    Validate a catalog with the validator API and return its
    [identifier, message] pairs.

    fetch_method is "url" (the validator fetches `url`) or "paste" (the
    document is `json_text`). Raises ValidatorRefused or ValidatorUnavailable.
    """
    api_url = os.getenv("VALIDATOR_API_URL")
    if not api_url:
        raise ValidatorUnavailable("VALIDATOR_API_URL is not set")

    payload = {"schema": schema, "fetch_method": fetch_method}
    if fetch_method == "url":
        payload["url"] = url
    else:
        payload["json_text"] = json_text

    try:
        response = requests.post(
            api_url,
            json=payload,
            timeout=(VALIDATOR_CONNECT_TIMEOUT_SECONDS, VALIDATOR_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise ValidatorUnavailable(f"request failed :: {repr(e)}") from e

    try:
        body = response.json()
    except ValueError:
        body = None

    if not isinstance(body, dict):
        raise ValidatorUnavailable(f"non-JSON response status={response.status_code}")

    if response.status_code == 200 and isinstance(body.get("validation_errors"), list):
        return body["validation_errors"]

    if response.status_code in (400, 413, 422):
        message = body.get("error")
        if isinstance(message, str):
            raise ValidatorRefused(message)

        message = _request_errors(body)
        if message:
            raise ValidatorRefused(message)

    raise ValidatorUnavailable(f"unexpected response status={response.status_code}")
