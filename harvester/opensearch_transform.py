"""Translate DCAT-US metadata into canonical OpenSearch index fields.

The OpenSearch ``datasets`` index stores a handful of fields derived from a
dataset's DCAT metadata (``title``, ``identifier``, ``theme``, ...). DCAT-US
1.1 and DCAT-US 3.0 describe several of these fields with *different shapes*,
such as ``theme`` moving from a list of strings to a list of
:class:`Concept` objects.

OpenSearch cannot index a field as two different data types, so this module
normalizes each derived field into a single, stable shape no matter which
schema version produced it:

* ``identifier``      -> string (legacy search-field contract)
* ``theme``           -> list of DCAT-US 3.0 ``Concept`` objects
* ``publisher``       -> organization display name (string, unchanged contract)
* ``keyword``         -> list of strings (unchanged)
* ``title``/``description`` -> string (unchanged)
* ``distribution_titles``   -> list of distribution titles (unchanged)

The module intentionally exposes a *narrow* interface:
:class:`DcatIndexTransformer` plus a couple of small helpers used by downstream
search logic. Coercion helpers live in this module for unit testing but are not
part of the public API. The module performs no I/O and knows nothing about
OpenSearch itself.

Only the fields currently stored in the OpenSearch mapping are handled here.
Fields that live solely inside the free-form ``dcat`` blob (e.g.
``contactPoint``, ``accessRights``) are deliberately out of scope.
"""

from __future__ import annotations

from typing import Any, Iterable

__all__ = [
    "DcatIndexTransformer",
    "concept_labels",
    "distribution_titles",
]


# Scalar / simple-list sub-fields of a DCAT-US 3.0 Concept worth searching on.
# "inScheme" (a nested ConceptScheme object) and "@type" are dropped.
_CONCEPT_KEYS = ("@id", "prefLabel", "altLabel", "notation", "definition")

def _clean_string(value: Any) -> str | None:
    """Return a non-empty, stripped string or ``None``."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def coerce_text(value: Any) -> str:
    """Return a plain string, or ``""`` for any non-string value.

    ``title`` and ``description`` are strings in both DCAT-US 1.1 and 3.0, so
    this is a light guard rather than a translation.
    """
    return value if isinstance(value, str) else ""


def coerce_identifier(value: Any) -> str | None:
    """Coerce a DCAT identifier into the legacy scalar index field.

    * DCAT-US 1.1: a plain string ``"cftc-dc1"`` -> ``"cftc-dc1"``.
    * DCAT-US 3.0: an ``Identifier`` object contributes only its ``@id``.

    Returns ``None`` when there is nothing indexable.
    """
    text = _clean_string(value)
    if text is not None:
        return text

    if isinstance(value, dict):
        return _clean_string(value.get("@id"))

    return None


def _coerce_concept(value: Any) -> dict | None:
    """Coerce a single theme/concept value into a DCAT-US 3.0 ``Concept``."""
    text = _clean_string(value)
    if text is not None:
        return {"prefLabel": text}

    if isinstance(value, dict):
        reduced: dict[str, Any] = {}
        for key in _CONCEPT_KEYS:
            if key not in value:
                continue
            field_value = value[key]
            if isinstance(field_value, list):
                items = [
                    item.strip()
                    for item in field_value
                    if isinstance(item, str) and item.strip()
                ]
                if items:
                    reduced[key] = items
            else:
                scalar = _clean_string(field_value)
                if scalar is not None:
                    reduced[key] = scalar
        return reduced or None

    return None


def coerce_concepts(value: Any) -> list[dict]:
    """Coerce a theme value into a list of DCAT-US 3.0 ``Concept`` objects.

    * DCAT-US 1.1: ``["climate", "weather"]`` -> ``[{"prefLabel": "climate"},
      {"prefLabel": "weather"}]``.
    * DCAT-US 3.0: a list of ``Concept`` objects, kept but reduced to their
      scalar search fields.

    A single string or single object is accepted and wrapped in a list.
    """
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    concepts = []
    for item in items:
        concept = _coerce_concept(item)
        if concept is not None:
            concepts.append(concept)
    return concepts


def concept_labels(concepts: Iterable[Any] | None) -> list[str]:
    """Extract searchable label strings from canonical concept objects.

    Accepts the output of :func:`coerce_concepts` as well as bare strings so it
    can be reused for "geospatial theme" style checks.
    """
    if not concepts:
        return []
    labels = []
    for concept in concepts:
        if isinstance(concept, str):
            label = _clean_string(concept)
        elif isinstance(concept, dict):
            label = _clean_string(concept.get("prefLabel"))
        else:
            label = None
        if label is not None:
            labels.append(label)
    return labels


def coerce_keywords(value: Any) -> list[str]:
    """Coerce keywords into a list of non-empty strings.

    ``keyword`` is a list of strings in both DCAT-US 1.1 and 3.0; a single
    string is tolerated and wrapped.
    """
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    return [item.strip() for item in items if isinstance(item, str) and item.strip()]


def coerce_publisher_name(value: Any) -> str:
    """Extract the publisher display name.

    Both DCAT-US 1.1 (``foaf:Agent``) and DCAT-US 3.0 (``Organization``)
    publishers expose ``name``; DCAT-US 3.0 may additionally carry
    ``prefLabel``. ``name`` is preferred to preserve the existing
    ``publisher``/``publisher.raw``/``publisher.normalized`` facet contract
    that downstream search relies on.
    """
    if not isinstance(value, dict):
        return ""
    name = _clean_string(value.get("name"))
    return name or _clean_string(value.get("prefLabel")) or ""


def distribution_titles(value: Any) -> list[str]:
    """Extract distribution titles.

    The ``title`` field on a distribution is compatible between DCAT-US 1.1 and
    3.0, so this simply gathers the non-empty titles.
    """
    if not isinstance(value, list):
        return []
    titles = []
    for dist in value:
        if isinstance(dist, dict):
            title = _clean_string(dist.get("title"))
            if title is not None:
                titles.append(title)
    return titles


class DcatIndexTransformer:
    """Map a DCAT metadata dict into canonical OpenSearch index fields."""

    def transform(self, dcat: dict | None) -> dict:
        """Return the canonical index-field dict for ``dcat``."""
        dcat = dcat or {}
        return {
            "title": coerce_text(dcat.get("title")),
            "description": coerce_text(dcat.get("description")),
            "publisher": coerce_publisher_name(dcat.get("publisher")),
            "keyword": coerce_keywords(dcat.get("keyword")),
            "theme": coerce_concepts(dcat.get("theme")),
            "identifier": coerce_identifier(dcat.get("identifier")),
            "distribution_titles": distribution_titles(dcat.get("distribution")),
        }
