"""Deterministic fingerprint of the OpenSearch index schema.

An index has to be rebuilt when the schema it was built with no longer matches
the schema this revision declares. Hashing the mapping and settings that
``OpenSearchClient._ensure_index`` sends to OpenSearch turns that question into
a string comparison, so a release can decide whether to migrate by looking at
the state of the target space rather than at the commits that reached it.

Keys are sorted before hashing, so reordering a mapping or renaming a local
variable does not move the fingerprint. Only a change OpenSearch would actually
apply to the index does.

Imports stay limited to the standard library and ``search.config`` so CI can run
``python -m search.fingerprint`` without installing application dependencies.
"""

import hashlib
import json

from search.config import SETTINGS
from search.mappings import MAPPINGS


def mapping_fingerprint(mappings=None, settings=None) -> str:
    """Return the SHA-256 hex digest of the index mapping and settings."""
    payload = {
        "mappings": MAPPINGS if mappings is None else mappings,
        "settings": SETTINGS if settings is None else settings,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    print(mapping_fingerprint())
