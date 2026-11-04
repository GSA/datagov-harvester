import re
from pathlib import Path

import pytest

from dcatus_validation.fetch import PAYLOAD_TOO_LARGE_MESSAGE
from dcatus_validation.limits import (
    MAX_REQUEST_BYTES,
    MAX_REQUEST_OVERHEAD_BYTES,
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_MB,
)

NGINX_SIZE_UNITS = {"": 1, "k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}


def _validate_location_body_limit(config_path: str) -> int:
    """client_max_body_size inside the validate API location block, in bytes."""
    config = Path(config_path).read_text()
    location = re.search(
        r"location ~ \^/api\(/v1\)\?/validate\(/document\)\?\$ \{(?P<body>[^}]*)\}",
        config,
    )
    assert location, f"no validate API location in {config_path}"
    size = re.search(
        r"client_max_body_size\s+(\d+)([kKmMgG]?);", location.group("body")
    )
    assert size, f"no client_max_body_size in {config_path}'s validate location"
    return int(size.group(1)) * NGINX_SIZE_UNITS[size.group(2).lower()]


def test_size_limit_is_150mb():
    """GSA/data.gov#6460: 150MB, not the 600MB one Census file asked for."""
    assert MAX_UPLOAD_MB == 150
    assert MAX_UPLOAD_BYTES == 150 * 1024 * 1024
    assert PAYLOAD_TOO_LARGE_MESSAGE == (
        "JSON payload too large - must be 150MB or less."
    )


def test_request_limit_fits_a_json_escaped_document_at_the_limit():
    """1.5x the document limit, for JSON escaping, plus the other API fields:
    225MB + 1KB, so nginx's 226M sits just above it (GSA/data.gov#6460)."""
    assert MAX_REQUEST_BYTES == MAX_UPLOAD_BYTES * 3 // 2 + MAX_REQUEST_OVERHEAD_BYTES
    assert MAX_REQUEST_BYTES == 225 * 1024 * 1024 + 1024


@pytest.mark.parametrize(
    "config_path", ["proxy/nginx-common.conf", "nginx-local-proxy.conf"]
)
def test_proxy_body_limit_sits_just_above_the_validator_limit(config_path):
    """Above MAX_REQUEST_BYTES, so the validator's own JSON 413 is what callers
    see rather than nginx's bare HTML one; within a few MB of it, so nginx
    still refuses anything far larger before buffering it to disk."""
    proxy_limit = _validate_location_body_limit(config_path)

    assert MAX_REQUEST_BYTES < proxy_limit <= MAX_REQUEST_BYTES + 2 * 1024 * 1024
