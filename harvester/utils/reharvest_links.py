"""Build and verify signed links for requesting another harvest.

The token carries the email recipient, source ID, and job ID. It is
signed, not encrypted. It has no time limit; the receiving route must verify
that the job is still the latest finished job before queuing a harvest.
"""

import hashlib
from urllib.parse import quote

from itsdangerous import BadData, URLSafeSerializer

# Separate these signatures from other tokens that use the same Flask secret.
LINK_SALT = "harvest-embedded-link-v1"


def _serializer(secret_key):
    """Use one signing configuration for both token creation and verification."""
    if not secret_key:
        raise ValueError("FLASK_APP_SECRET_KEY is required for reharvest links")
    return URLSafeSerializer(
        secret_key,
        salt=LINK_SALT,
        signer_kwargs={"digest_method": hashlib.sha256},
    )


def create_reharvest_token(secret_key, recipient, source_id, job_id):
    """Sign the recipient, source, and job associated with this link."""
    return _serializer(secret_key).dumps(
        {
            "email": recipient,
            "source_id": source_id,
            "job_id": job_id,
        }
    )


def create_reharvest_url(base_url, secret_key, recipient, source_id, job_id):
    """Put the signed token in a URL for the confirmation page."""
    token = create_reharvest_token(secret_key, recipient, source_id, job_id)
    return f"{base_url.rstrip('/')}/harvest_source/refresh?token={quote(token)}"


def read_reharvest_token(secret_key, token):
    """Return verified token fields, or None for a bad signature or payload."""
    try:
        data = _serializer(secret_key).loads(token)
    except (BadData, TypeError):
        return None
    # A valid signature alone does not guarantee the expected payload shape.
    if not isinstance(data, dict) or any(
        not isinstance(data.get(key), str) or not data[key]
        for key in ("email", "source_id", "job_id")
    ):
        return None
    return data
