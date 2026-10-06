"""Server-side fetching of catalogs submitted by URL.

The validator is public and unauthenticated, so every URL it fetches is
attacker-chosen: the checks here (scheme, private/internal addresses on every
redirect hop, size, one overall time budget) are what keep that from being used
to reach internal services.
"""

import hashlib
import ipaddress
import json
import logging
import math
import os
import socket
import time
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.poolmanager import SSL_KEYWORDS

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

_IPV4_COMPATIBLE_NETWORK = ipaddress.ip_network("::/96")
_IPV4_PROTOCOL_ASSIGNMENTS_NETWORK = ipaddress.ip_network("192.0.0.0/24")
_IPV4_DEPRECATED_6TO4_RELAY_NETWORK = ipaddress.ip_network("192.88.99.0/24")
_NAT64_WELL_KNOWN_NETWORK = ipaddress.ip_network("64:ff9b::/96")
_NAT64_LOCAL_NETWORK = ipaddress.ip_network("64:ff9b:1::/48")
_TEREDO_NETWORK = ipaddress.ip_network("2001::/32")
_SIX_TO_FOUR_NETWORK = ipaddress.ip_network("2002::/16")
_IPV6_DOCUMENTATION_NETWORK = ipaddress.ip_network("3fff::/20")


def _is_public_ipv4_address(ip: ipaddress.IPv4Address) -> bool:
    return (
        ip.is_global
        and not ip.is_multicast
        and not ip.is_reserved
        and ip not in _IPV4_PROTOCOL_ASSIGNMENTS_NETWORK
        and ip not in _IPV4_DEPRECATED_6TO4_RELAY_NETWORK
    )


def _resolve_addresses(hostname: str, port: int | None = None) -> list[str]:
    addresses = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(addr[4][0] for addr in addresses))


def _is_public_address(address: str) -> bool:
    """Reject non-global addresses, including IPv4 hidden inside IPv6."""
    ip = ipaddress.ip_address(address)
    if not ip.is_global or ip.is_multicast or getattr(ip, "is_site_local", False):
        return False
    if not isinstance(ip, ipaddress.IPv6Address):
        return _is_public_ipv4_address(ip)

    if ip.ipv4_mapped is not None:
        return _is_public_ipv4_address(ip.ipv4_mapped)
    if ip in _NAT64_WELL_KNOWN_NETWORK:
        embedded_ipv4 = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        return _is_public_ipv4_address(embedded_ipv4)
    if ip.is_reserved:
        return False

    # Python 3.12 considers these globally routable even when they encode
    # loopback or private IPv4. Transition and local-use ranges are not valid
    # public web origins; inspect the embedded IPv4 in the public NAT64 /96.
    if (
        ip in _IPV4_COMPATIBLE_NETWORK
        or ip in _NAT64_LOCAL_NETWORK
        or ip in _TEREDO_NETWORK
        or ip in _SIX_TO_FOUR_NETWORK
        or ip in _IPV6_DOCUMENTATION_NETWORK
    ):
        return False
    return True


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
        addresses = _resolve_addresses(hostname)
        return bool(addresses) and all(_is_public_address(ip) for ip in addresses)
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
# with 2 threads each, so each in-flight fetch holds one of only 6 concurrent
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
INVALID_JSON_ENCODING_MESSAGE = "Invalid JSON encoding. Use UTF-8, UTF-16, or UTF-32."
INVALID_JSON_NUMBER_MESSAGE = "JSON contains a number outside the supported range."


def _is_json_media_type(content_type: str) -> bool:
    media_type = content_type.partition(";")[0].strip().lower()
    return media_type == "application/json" or (
        media_type.startswith("application/") and media_type.endswith("+json")
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


def _parse_json_integer(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise InvalidCatalogSource(INVALID_JSON_NUMBER_MESSAGE) from None


def _parse_json_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise InvalidCatalogSource(INVALID_JSON_NUMBER_MESSAGE)
    return parsed


def _reject_nonfinite_json_constant(_value: str):
    # Python accepts NaN and Infinity by default even though JSON does not.
    raise InvalidCatalogSource(INVALID_JSON_NUMBER_MESSAGE)


def parse_json_document(document: str | bytes | bytearray):
    """Parse strict JSON and normalize unsupported encoding/numeric failures."""
    try:
        return json.loads(
            document,
            parse_int=_parse_json_integer,
            parse_float=_parse_json_float,
            parse_constant=_reject_nonfinite_json_constant,
        )
    except UnicodeDecodeError:
        raise InvalidCatalogSource(INVALID_JSON_ENCODING_MESSAGE) from None


class _PinnedConnectionMixin:
    """Connect to the approved IP while retaining the hostname for Host/TLS."""

    def __init__(self, *args, pinned_ip: str, **kwargs):
        self._pinned_ip = pinned_ip
        super().__init__(*args, **kwargs)

    def _new_conn(self):
        dns_host = self._dns_host
        try:
            self._dns_host = self._pinned_ip
            return super()._new_conn()
        finally:
            self._dns_host = dns_host


class _PinnedHTTPConnection(_PinnedConnectionMixin, HTTPConnection):
    pass


class _PinnedHTTPSConnection(_PinnedConnectionMixin, HTTPSConnection):
    pass


class _PinnedHTTPConnectionPool(HTTPConnectionPool):
    ConnectionCls = _PinnedHTTPConnection


class _PinnedHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _PinnedHTTPSConnection


class _PinnedAddressAdapter(HTTPAdapter):
    """A one-address adapter used for one request and then closed."""

    def __init__(self, pinned_ip: str):
        self._pinned_ip = pinned_ip
        self._pools = []
        super().__init__()

    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        if proxies:
            raise RuntimeError("Proxies are not supported by the pinned URL fetcher")

        host_params, pool_kwargs = self.build_connection_pool_key_attributes(
            request, verify, cert
        )
        pool_class = (
            _PinnedHTTPSConnectionPool
            if host_params["scheme"] == "https"
            else _PinnedHTTPConnectionPool
        )
        if host_params["scheme"] == "http":
            for keyword in SSL_KEYWORDS:
                pool_kwargs.pop(keyword, None)
        pool = pool_class(
            host_params["host"],
            host_params["port"],
            pinned_ip=self._pinned_ip,
            **pool_kwargs,
        )
        self._pools.append(pool)
        return pool

    def close(self):
        for pool in self._pools:
            pool.close()
        self._pools.clear()
        super().close()


def _pinned_get(target: str, ip: str, timeout: tuple[float, float]):
    """
    Fetch target through the IP that passed validation. A fresh Session avoids
    ambient proxy and netrc credentials, either of which would hand hostname
    resolution back to another component after the security check.
    """
    session = requests.Session()
    session.trust_env = False
    session.mount("http://", _PinnedAddressAdapter(ip))
    session.mount("https://", _PinnedAddressAdapter(ip))
    verify = os.getenv("REQUESTS_CA_BUNDLE") or True

    try:
        response = session.get(
            target,
            headers={"User-Agent": USER_AGENT},
            stream=True,
            timeout=timeout,
            allow_redirects=False,
            verify=verify,
        )
    except Exception:
        session.close()
        raise

    response._validator_session = session
    return response


def _close_response(response: requests.Response) -> None:
    session = getattr(response, "_validator_session", None)
    try:
        response.close()
    finally:
        if session is not None:
            session.close()


def _target_for_log(url: str) -> str:
    """Log an origin and correlation hash, never credentials or signed paths."""
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.hostname or '<invalid>'}"
    try:
        if parsed.port is not None:
            origin = f"{origin}:{parsed.port}"
    except ValueError:
        pass
    digest = hashlib.sha256(url.encode("utf-8", errors="replace")).hexdigest()[:12]
    return f"{origin} url_hash={digest}"


def _validate_fetch_target(url: str) -> list[str]:
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise InvalidCatalogSource("Only HTTP/HTTPS URLs are allowed.")

    if not parsed.hostname:
        raise InvalidCatalogSource("Invalid URL.")

    if parsed.username is not None or parsed.password is not None:
        raise InvalidCatalogSource("URLs containing credentials are not allowed.")

    try:
        port = parsed.port
    except ValueError:
        raise InvalidCatalogSource("Invalid URL.") from None

    try:
        addresses = _resolve_addresses(parsed.hostname, port)
    except Exception:
        addresses = []

    if not addresses:
        raise InvalidCatalogSource(UNEXPECTED_FETCH_ERROR_MESSAGE)

    if not ALLOW_PRIVATE_ADDRESSES and any(
        not _is_public_address(ip) for ip in addresses
    ):
        raise InvalidCatalogSource(
            "Access to private/internal addresses is not allowed."
        )
    return addresses


def fetch_json_from_url(url: str) -> dict:
    # DNS validation is part of the fetch, so time spent resolving the
    # submitted hostname counts against the same deadline as the request.
    deadline = time.monotonic() + FETCH_TIMEOUT_SECONDS
    approved_addresses = _validate_fetch_target(url)
    logger.info("Validator fetching target=%s", _target_for_log(url))

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

    def _get(target: str, addresses: list[str]) -> requests.Response:
        nonlocal connect_timeouts, connecting
        connecting = True
        connect_timeouts = 0
        attempts = 0
        while True:
            remaining = _remaining_budget()
            ip = addresses[attempts % len(addresses)]
            try:
                # _pinned_get opens a fresh connection to the address that
                # passed validation, so DNS cannot change underneath us.
                result = _pinned_get(
                    target,
                    ip,
                    timeout=(
                        min(FETCH_CONNECT_TIMEOUT_SECONDS, remaining),
                        remaining,
                    ),
                )
            except requests.exceptions.SSLError:
                raise
            except requests.exceptions.Timeout:
                # A stalled TLS handshake is bounded by the connect timeout but
                # raised as ReadTimeout, so the exception type can't tell the
                # phases apart. A timeout well before the deadline can only be
                # the connect limit; one at the deadline is the budget running
                # out, which retrying can't help.
                if time.monotonic() >= deadline - 0.5:
                    raise
                connect_timeouts += 1
                attempts += 1
                if attempts >= FETCH_CONNECT_ATTEMPTS:
                    raise
                logger.info(
                    "Validator URL fetch connect attempt %s timed out, retrying url=%s",
                    attempts,
                    _target_for_log(target),
                )
                continue
            except requests.exceptions.ConnectionError:
                # One unusable address (commonly an IPv6 address on an
                # IPv4-only network) must not hide another approved address.
                attempts += 1
                if attempts >= FETCH_CONNECT_ATTEMPTS:
                    raise
                logger.info(
                    "Validator URL fetch connect attempt %s failed, retrying target=%s",
                    attempts,
                    _target_for_log(target),
                )
                continue
            connecting = False
            return result

    response = None
    try:
        for _ in range(MAX_FETCH_REDIRECTS + 1):
            response = _get(url, approved_addresses)
            if response.status_code not in _REDIRECT_STATUS_CODES:
                break

            location = response.headers.get("Location")
            _close_response(response)
            if not location:
                raise InvalidCatalogSource("Redirected without a Location header.")

            url = urljoin(url, location)
            approved_addresses = _validate_fetch_target(url)
        else:
            raise InvalidCatalogSource("Too many redirects.")

        response.raise_for_status()

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_UPLOAD_BYTES:
            raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)

        content_type = response.headers.get("Content-Type", "")
        if not _is_json_media_type(content_type):
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
                "connect_timeouts=%s target=%s",
                connect_timeouts,
                _target_for_log(url),
            )
            raise InvalidCatalogSource(
                "Could not connect to the URL's server in time. Check that the "
                "URL is correct and the server is up, then try again."
            )
        logger.warning(
            "Validator URL fetch timed out after %ss target=%s",
            FETCH_TIMEOUT_SECONDS,
            _target_for_log(url),
        )
        raise InvalidCatalogSource(
            f"The URL took longer than {FETCH_TIMEOUT_SECONDS} seconds to "
            "respond. Check that the URL is correct and the server is up."
        )
    except InvalidCatalogSource:
        raise
    except Exception as e:
        # Connection refused, TLS error, bad status - requests' exception text
        # can repeat signed query strings or credentials, so log only its type.
        logger.warning(
            "Validator URL fetch failed target=%s error_type=%s",
            _target_for_log(url),
            type(e).__name__,
        )
        raise InvalidCatalogSource(UNEXPECTED_FETCH_ERROR_MESSAGE)
    finally:
        if response is not None:
            _close_response(response)

    content = b"".join(chunks)

    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)

    try:
        return parse_json_document(content)
    except json.JSONDecodeError as e:
        raise InvalidCatalogSource(invalid_json_message(e))
