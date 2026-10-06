"""Server-side fetching of catalogs submitted by URL.

The validator is public and unauthenticated, so every URL it fetches is
attacker-chosen: the checks here (scheme, private/internal addresses on every
redirect hop, size, one overall time budget) are what keep that from being used
to reach internal services.
"""

import ipaddress
import json
import logging
import os
import socket
import time
from urllib.parse import urljoin, urlparse

import requests

from dcatus_validation.limits import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB

logger = logging.getLogger("dcatus_validation")

USER_AGENT = "HarvesterBot/0.0 (https://data.gov; datagovhelp@gsa.gov) Data.gov/2.0"

# Private/internal addresses are refused unless explicitly allowed, so a
# public, unauthenticated fetcher fails closed wherever it runs. Local
# development (docker-compose.yml) sets this so the sample catalogs served from
# the compose network can be fetched.
ALLOW_PRIVATE_ADDRESSES = (
    os.getenv("ALLOW_PRIVATE_ADDRESSES", "false").lower() == "true"
)


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

            # `is_private` does not include every non-public range. In
            # particular, RFC 6598 shared address space (100.64.0.0/10) may be
            # used inside hosting networks and must not be reachable here.
            if not ip_obj.is_global:
                return False
        return True
    except Exception:
        return False


# The URL path is fetched server-side, so MAX_CONTENT_LENGTH never sees it. Same
# limit, enforced here by hand (and in the route, for pasted documents).
PAYLOAD_TOO_LARGE_MESSAGE = (
    f"JSON payload too large - must be {MAX_UPLOAD_MB}MB or less."
)

# The binding ceiling is CloudFront's hard 30s for the whole request
# (harvest.data.gov/api/v1/validate), and validation only starts once the
# fetch is done: roughly 0.5-1.2 s/MB, so up to ~12s for a 10MB catalog. 12s
# here keeps that worst case near 25s, leaving padding for proxy hops.
# Answering inside the ceiling means a slow or hung target produces a clean,
# logged rejection instead of us being disconnected mid-request.
#
# It is also a capacity limit, not just a latency one: gunicorn runs 3 workers
# with 1 thread each, so each in-flight fetch holds one of only 3 concurrent
# slots per instance.
#
# From cloud.gov, some hosts (data.nola.gov) lose a handshake packet on 10-25%
# of connections. TCP's own retransmits back off (handshakes measured at 3s,
# 9s, 17s), so waiting longer on one connection can't fit the budget, but a
# fresh connection almost always completes in well under a second. So each
# connect attempt is short - long enough for a single quick retransmit - and
# a connect-phase timeout is retried on a new connection, all inside the one
# deadline. A dead host still fails by FETCH_TIMEOUT_SECONDS.
FETCH_TIMEOUT_SECONDS = 12
FETCH_CONNECT_TIMEOUT_SECONDS = 4
FETCH_CONNECT_ATTEMPTS = 3

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

    The API renders `str()` of this straight back to the submitter (and the
    harvester's validator page shows it beside the form field), so
    every message here must be built only from literals and numbers. No
    exceptions to that rule: never interpolate another exception's text, even
    one that looks harmless, because the objects carrying it also carry things
    that are not (json.JSONDecodeError.doc is the whole submitted document,
    which for a URL submission may be content the submitter cannot otherwise
    read). Keeping the rule absolute is what makes it reviewable, and keeps
    CodeQL py/stack-trace-exposure honest rather than suppressed.

    Anything we *didn't* anticipate should stay an ordinary exception so
    callers answer with UNEXPECTED_FETCH_ERROR_MESSAGE and log the detail
    instead.

    Subclasses ValueError so callers catching ValueError still do.
    """


def invalid_json_message(error: json.JSONDecodeError, source: str = "") -> str:
    """Describe a JSON parse failure by position only.

    The single place that turns a decode error into something a submitter
    sees, so the "literals and numbers only" rule in InvalidCatalogSource has
    one place to hold rather than every call site. lineno/colno are ints;
    error.msg and str(error) are deliberately unused.
    """
    where = f" in the {source}" if source else ""
    return f"Invalid JSON{where} at line {error.lineno}, column {error.colno}."


def _validate_fetch_target(url: str) -> None:
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise InvalidCatalogSource("Only HTTP/HTTPS URLs are allowed.")

    if not parsed.hostname:
        raise InvalidCatalogSource("Invalid URL.")

    if not ALLOW_PRIVATE_ADDRESSES and not is_public_ip(parsed.hostname):
        raise InvalidCatalogSource(
            "Access to private/internal addresses is not allowed."
        )


def fetch_json_from_url(url: str) -> dict:
    # The URL itself isn't sensitive - it's a pointer to a public DCAT catalog,
    # which is the whole point of this feature - so it's safe to log, unlike
    # pasted/uploaded catalog content. Logged up front so a hung or rejected
    # fetch still shows which URL was responsible.
    logger.info("Validator fetching url=%s", url)

    # DNS validation is part of the fetch, so time spent resolving the
    # submitted hostname counts against the same deadline as the request.
    deadline = time.monotonic() + FETCH_TIMEOUT_SECONDS
    _validate_fetch_target(url)

    # One deadline for the whole operation, not a fresh FETCH_TIMEOUT_SECONDS
    # per redirect hop - otherwise a chain of MAX_FETCH_REDIRECTS redirects,
    # each just under the timeout, could run for
    # MAX_FETCH_REDIRECTS x FETCH_TIMEOUT_SECONDS in total, defeating the
    # point of bounding this at all.
    def _remaining_budget() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise requests.exceptions.Timeout(
                f"Exceeded the {FETCH_TIMEOUT_SECONDS}s fetch budget"
            )
        return remaining

    def _set_stream_timeout(response: requests.Response, timeout: float) -> None:
        """Bound the next body read by the remaining overall fetch budget."""
        connection = getattr(response.raw, "_connection", None)
        sock = getattr(connection, "sock", None)
        if sock is None:
            try:
                sock = response.raw._fp.fp.raw._sock
            except AttributeError:
                sock = None
        if sock is None:
            # The network connection can close while iter_content still has
            # decoded bytes buffered. Those reads cannot block on the network.
            if getattr(response.raw, "closed", False) is True:
                return
            raise RuntimeError("Could not set validator response timeout")
        sock.settimeout(timeout)

    connect_timeouts = 0
    connecting = False

    def _get(target: str) -> requests.Response:
        nonlocal connect_timeouts, connecting
        connecting = True
        while True:
            remaining = _remaining_budget()
            try:
                # requests.get opens a new connection every call, so a retry
                # doesn't reuse the stalled one.
                result = requests.get(
                    target,
                    headers={"User-Agent": USER_AGENT},
                    stream=True,
                    timeout=(
                        min(FETCH_CONNECT_TIMEOUT_SECONDS, remaining),
                        remaining,
                    ),
                    allow_redirects=False,
                )
            except requests.exceptions.Timeout:
                # A stalled TLS handshake is bounded by the connect timeout but
                # raised as ReadTimeout, so the exception type can't tell the
                # phases apart. A timeout well before the deadline can only be
                # the connect limit; one at the deadline is the budget running
                # out, which retrying can't help.
                if time.monotonic() >= deadline - 0.5:
                    raise
                connect_timeouts += 1
                if connect_timeouts >= FETCH_CONNECT_ATTEMPTS:
                    raise
                logger.info(
                    "Validator URL fetch connect attempt %s timed out, retrying url=%s",
                    connect_timeouts,
                    target,
                )
                continue
            connecting = False
            return result

    response = None
    try:
        for _ in range(MAX_FETCH_REDIRECTS + 1):
            response = _get(url)
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
        chunk_iterator = iter(response.iter_content(chunk_size=8192))
        while True:
            # requests' read timeout is fixed when the request starts. Reduce
            # the socket timeout before every body read so a late stall cannot
            # consume a second full FETCH_TIMEOUT_SECONDS.
            _set_stream_timeout(response, _remaining_budget())
            try:
                chunk = next(chunk_iterator)
            except StopIteration:
                break
            except requests.exceptions.ConnectionError:
                # iter_content wraps urllib3's body ReadTimeoutError in
                # ConnectionError. Reclassify it when the deadline caused it.
                _remaining_budget()
                raise
            if chunk:
                total_size += len(chunk)
                if total_size > MAX_UPLOAD_BYTES:
                    raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
                chunks.append(chunk)
    except requests.exceptions.Timeout:
        if connecting and connect_timeouts:
            logger.warning(
                "Validator URL fetch could not connect in time "
                "connect_timeouts=%s url=%s",
                connect_timeouts,
                url,
            )
            raise InvalidCatalogSource(
                "Could not connect to the URL's server in time. Check that the "
                "URL is correct and the server is up, then try again."
            )
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
        raise InvalidCatalogSource(invalid_json_message(e))
