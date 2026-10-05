"""Translate DCAT-US metadata into legacy OpenSearch index field shapes.

The OpenSearch ``datasets`` index stores a handful of fields derived from a
dataset's DCAT metadata (``title``, ``identifier``, ``theme``, ...). DCAT-US
1.1 and DCAT-US 3.0 describe several of these fields with different shapes,
such as ``theme`` moving from a list of strings to a list of Concept objects.

OpenSearch cannot index a field as two different data types, so this module
normalizes each derived field into the existing (legacy) index shapes so both
schema versions can share one mapping:

* ``identifier``      -> string (from plain string or object ``@id``)
* ``theme``           -> list of strings (from strings or Concept ``prefLabel``)
* ``publisher``       -> organization display name (string)
* ``keyword``         -> list of strings
* ``title``/``description`` -> string
* ``distribution_titles``   -> list of distribution titles
* ``accessLevel``   -> best-effort normalized lowercase access level
                        string. Returns one of ``public``, ``restricted
                        public``, or ``non-public`` when the value is
                        recognized (via exact match or keyword match
                        against known accessRights phrasing); otherwise
                        returns the original value, lowercased and
                        cleaned. Returns an empty string if no value was
                        provided.

To add a new indexed field, register it in :data:`INDEX_FIELDS`. The module
performs no I/O and knows nothing about OpenSearch itself.

``inSeries`` is intentionally ignored (legacy ``isPartOf`` is left as harvested).
"""

from __future__ import annotations

from typing import Any, Callable

__all__ = [
    "INDEX_FIELDS",
    "DcatIndexTransformer",
]


def _clean_string(value: Any) -> str | None:
    """Return a non-empty, stripped string or ``None``."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def coerce_text(value: Any) -> str:
    """Return a plain string, or ``""`` for any non-string value."""
    return value if isinstance(value, str) else ""


def coerce_identifier(value: Any) -> str:
    """Coerce a DCAT identifier into the legacy scalar index field.

    * DCAT-US 1.1: a plain string ``"cftc-dc1"`` -> ``"cftc-dc1"``.
    * DCAT-US 3.0: an ``Identifier`` object contributes only its ``@id``.

    Returns ``""`` when there is nothing indexable.
    """
    text = _clean_string(value)
    if text is not None:
        return text

    if isinstance(value, dict):
        return _clean_string(value.get("@id")) or ""

    return ""


def coerce_theme_labels(value: Any) -> list[str]:
    """Coerce theme values into a list of searchable label strings.

    * DCAT-US 1.1: ``["climate", "weather"]`` -> unchanged (trimmed).
    * DCAT-US 3.0: Concept objects contribute ``prefLabel``.

    A single string or single object is accepted and wrapped in a list.
    """
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    labels = []
    for item in items:
        if isinstance(item, str):
            label = _clean_string(item)
        elif isinstance(item, dict):
            label = _clean_string(item.get("prefLabel"))
        else:
            label = None
        if label is not None:
            labels.append(label)
    return labels


def coerce_keywords(value: Any) -> list[str]:
    """Coerce keywords into a list of non-empty strings."""
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    return [item.strip() for item in items if isinstance(item, str) and item.strip()]


def coerce_publisher_name(value: Any) -> str:
    """Extract the publisher display name.

    Both DCAT-US 1.1 and 3.0 publishers expose ``name``; DCAT-US 3.0 may also
    carry ``prefLabel``. ``name`` is preferred to preserve the existing facet
    contract.
    """
    if not isinstance(value, dict):
        return ""
    name = _clean_string(value.get("name"))
    return name or _clean_string(value.get("prefLabel")) or ""


def distribution_titles(value: Any) -> list[str]:
    """Extract non-empty distribution titles."""
    if not isinstance(value, list):
        return []
    titles = []
    for dist in value:
        if isinstance(dist, dict):
            title = _clean_string(dist.get("title"))
            if title is not None:
                titles.append(title)
    return titles


_NON_PUBLIC_MARKERS = ("non-public", "not available", "not for public")
_RESTRICTED_MARKERS = ("restricted",)
_PUBLIC_MARKERS = ("public",)
_CANONICAL_LEVELS = {"public", "restricted public", "non-public"}


def coerce_access_level(value: Any) -> str:
    """Normalize a raw ``accessLevel`` or ``accessRights`` value.

    Returns one of ``public``, ``restricted public``, or ``non-public``
    when the value is recognized via exact match or keyword-based
    inference. If the value doesn't match any known pattern, returns the
    original value (lowercased and cleaned) unchanged, so callers retain
    visibility into unresolved/unexpected data rather than losing it
    silently. Returns an empty string if no value was provided at all.

    Background: DCAT-US v3.0 does not include `accessLevel`, but agencies
    may keep it during the transition. Per the DCAT-US v3.0 Implementation
    Guide (Step 3 categorization guidance and Step 5 migration notes),
    `accessRights` may either directly replace `accessLevel` with the same
    three canonical values, or contain descriptive free text explaining the
    access restriction (mirroring v1.1's separate `rights` field). This
    function tolerates both shapes via keyword matching, since agencies are
    not required to converge on identical wording.

    References (retrieved 2026-09; content may be revised without notice):
      - DCAT-US v3.0 Implementation Guide (authoritative for field
        definitions): resources.data.gov/assets/documents/
        dcat-us-3-implementation-guide.pdf
      - DCAT-US v3.0 Migration Guide (technical guide only; explicitly
        NOT authoritative for field definitions or M-25-05 guidance):
        resources.data.gov/resources/dcat-us-3-migration/
    """
    text = _clean_string(value)
    if text is None:
        return ""

    normalized = text.lower()

    if normalized in _CANONICAL_LEVELS:
        return normalized

    if any(marker in normalized for marker in _NON_PUBLIC_MARKERS):
        return "non-public"
    if any(marker in normalized for marker in _RESTRICTED_MARKERS):
        return "restricted public"
    if any(marker in normalized for marker in _PUBLIC_MARKERS):
        return "public"

    return (
        normalized  # Return the original string if it doesn't match any known markers
    )


# dest_field -> (source_dcat_key, coercer)
INDEX_FIELDS: dict[str, tuple[str, Callable[[Any], Any]]] = {
    "title": ("title", coerce_text),
    "description": ("description", coerce_text),
    "publisher": ("publisher", coerce_publisher_name),
    "keyword": ("keyword", coerce_keywords),
    "theme": ("theme", coerce_theme_labels),
    "identifier": ("identifier", coerce_identifier),
    "distribution_titles": ("distribution", distribution_titles),
    "access_level": ("accessLevel", coerce_access_level),
}


class DcatIndexTransformer:
    """Map a DCAT metadata dict into legacy OpenSearch index fields."""

    def transform(self, dcat: dict | None) -> dict:
        """Return the index-field dict for ``dcat`` using :data:`INDEX_FIELDS`."""
        dcat = dcat or {}
        return {
            dest: coercer(dcat.get(source_key))
            for dest, (source_key, coercer) in INDEX_FIELDS.items()
        }
