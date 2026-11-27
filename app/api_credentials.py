"""Per-client API credentials, so API requests can be attributed in audit logs.

`HARVEST_API_CLIENTS` is an optional JSON object that maps a client ID to the
SHA-256 digest of that client's API key (or a list of digests, so a key can be
rotated without downtime):

    {"ckan-migration": "sha256:<64 hex chars>",
     "ops-scripts": ["sha256:<new>", "sha256:<old>"]}

Only digests are configured, never keys. Keys must be high-entropy random
values (see docs/developer.md): an unsalted digest is safe only for those, and
digests are still treated as sensitive configuration. The client ID is a
stable, non-secret name used as the audit actor. Named keys carry the same
authorization as the shared token; they change attribution, not access.

The shared `HARVEST_API_TOKEN` keeps working unchanged and is still logged as
`<api_token>`, so its requests stay distinguishable from attributable ones.
"""

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from types import MappingProxyType

from app.startup_validation import NULL_LIKE_PLACEHOLDER_VALUES, StartupValidationError

API_CLIENTS_ENV_VAR = "HARVEST_API_CLIENTS"

# Lowercase so two IDs can't differ only by case, and no whitespace, "=", "<"
# or "@", so an ID can't inject log fields or look like an email/legacy actor.
CLIENT_ID_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?")
DIGEST_PATTERN = re.compile(r"sha256:([0-9a-f]{64})")

LEGACY_ACTOR = "<api_token>"
LEGACY_AUTH_TYPE = "api_token"
CLIENT_AUTH_TYPE = "api_client"
CLIENT_ACTOR_PREFIX = "api_client:"


@dataclass(frozen=True)
class APIIdentity:
    actor: str
    auth_type: str
    client_id: str | None = None


def _digest(api_key: str) -> bytes:
    return hashlib.sha256(api_key.encode("utf-8")).digest()


def _reject_duplicate_keys(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            # json.loads would otherwise silently keep the last value.
            raise StartupValidationError(
                f"{API_CLIENTS_ENV_VAR} lists a client ID more than once."
            )
        seen[key] = value
    return seen


def parse_api_clients(
    raw_value: str | None, legacy_token: str | None = None
) -> MappingProxyType:
    """Parse and validate `HARVEST_API_CLIENTS`.

    Returns a read-only mapping of digest bytes -> client ID. Raises
    StartupValidationError on anything ambiguous or malformed; error messages
    are written to leave out digests and keys.
    """
    if raw_value is None:
        return MappingProxyType({})
    stripped = raw_value.strip()
    if not stripped or stripped.lower() in NULL_LIKE_PLACEHOLDER_VALUES:
        return MappingProxyType({})

    try:
        config = json.loads(stripped, object_pairs_hook=_reject_duplicate_keys)
    except (ValueError, RecursionError):
        raise StartupValidationError(
            f"{API_CLIENTS_ENV_VAR} is not valid JSON."
        ) from None

    if not isinstance(config, dict):
        raise StartupValidationError(
            f"{API_CLIENTS_ENV_VAR} must be a JSON object of client ID to digest."
        )

    legacy_digest = _digest(legacy_token) if legacy_token else None
    clients: dict[bytes, str] = {}
    for client_id, digests in config.items():
        if not isinstance(client_id, str) or not CLIENT_ID_PATTERN.fullmatch(client_id):
            raise StartupValidationError(
                f"{API_CLIENTS_ENV_VAR} has an invalid client ID; use 1-64 "
                "lowercase letters, digits, '.', '_' or '-'."
            )
        if isinstance(digests, str):
            digests = [digests]
        if not isinstance(digests, list) or not digests:
            raise StartupValidationError(
                f"{API_CLIENTS_ENV_VAR} client '{client_id}' needs a digest "
                "or a non-empty list of digests."
            )
        for entry in digests:
            match = DIGEST_PATTERN.fullmatch(entry) if isinstance(entry, str) else None
            if not match:
                raise StartupValidationError(
                    f"{API_CLIENTS_ENV_VAR} client '{client_id}' has a malformed "
                    "digest; expected 'sha256:' and 64 lowercase hex characters."
                )
            digest = bytes.fromhex(match.group(1))
            if digest in clients:
                raise StartupValidationError(
                    f"{API_CLIENTS_ENV_VAR} assigns the same key to more than "
                    f"one entry (client '{client_id}')."
                )
            if legacy_digest is not None and hmac.compare_digest(digest, legacy_digest):
                raise StartupValidationError(
                    f"{API_CLIENTS_ENV_VAR} client '{client_id}' reuses the "
                    "shared HARVEST_API_TOKEN; issue it its own key."
                )
            clients[digest] = client_id
    return MappingProxyType(clients)


def authenticate_api_key(
    provided_key: str, api_clients, legacy_token: str | None
) -> APIIdentity | None:
    """Return the identity for a presented X-API-Key, or None if it's invalid.

    The identity comes only from which configured credential matched; nothing
    else in the request is consulted.

    The presented key is hashed once and compared, with hmac.compare_digest,
    against every configured client digest and the legacy token's digest
    before an identity is chosen, so the number of comparisons does not depend
    on which entry (if any) matched. That is the extent of the timing claim:
    hashing, the size of the configuration and interpreter overhead are not
    constant-time.
    """
    if not provided_key:
        return None

    provided_digest = _digest(provided_key)
    matched_client = None
    for digest, client_id in (api_clients or {}).items():
        if hmac.compare_digest(provided_digest, digest):
            matched_client = client_id
    legacy_matched = bool(legacy_token) and hmac.compare_digest(
        provided_digest, _digest(legacy_token)
    )

    # parse_api_clients rejects a client key equal to the legacy token, so at
    # most one of these can be true for a validated configuration.
    if matched_client is not None:
        return APIIdentity(
            actor=f"{CLIENT_ACTOR_PREFIX}{matched_client}",
            auth_type=CLIENT_AUTH_TYPE,
            client_id=matched_client,
        )
    if legacy_matched:
        return APIIdentity(actor=LEGACY_ACTOR, auth_type=LEGACY_AUTH_TYPE)
    return None
