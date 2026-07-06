"""Translate DCAT-US metadata into canonical OpenSearch index fields.

The OpenSearch ``datasets`` index stores a handful of fields derived from a
dataset's DCAT metadata (``title``, ``identifier``, ``theme``, ...). DCAT-US
1.1 and DCAT-US 3.0 describe several of these fields with *different shapes*:
``identifier`` went from a plain string to an :class:`Identifier` object,
``theme`` from a list of strings to a list of :class:`Concept` objects, and so
on.

OpenSearch cannot index a field as two different data types, so this module
maps DCAT-US 1.1 values *up* into the DCAT-US 3.0 structure. Every field ends
up with a single, stable shape no matter which schema version produced it:

* ``identifier``      -> DCAT-US 3.0 ``Identifier`` object ``{"@id": ...}``
* ``theme``           -> list of DCAT-US 3.0 ``Concept`` objects
* ``publisher``       -> organization display name (string, unchanged contract)
* ``keyword``         -> list of strings (unchanged)
* ``title``/``description`` -> string (unchanged)
* ``distribution_titles``   -> list of distribution titles (unchanged)
* ``inSeries``          -> list of DCAT-US 3.0 ``DatasetSeries`` objects
* DCAT-US 1.1 ``isPartOf`` strings map up to ``[{"@id": ...}]``

The module intentionally exposes a *narrow* interface:
:class:`DcatIndexTransformer` (configured with a list of :class:`FieldSpec`)
plus a set of small, individually testable coercion helpers. It performs no
I/O and knows nothing about OpenSearch itself, which keeps it trivial to unit
test in isolation.

Only the fields currently stored in the OpenSearch mapping are handled here.
Fields that live solely inside the free-form ``dcat`` blob (e.g.
``contactPoint``, ``accessRights``) are deliberately out of scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

__all__ = [
    "FieldSpec",
    "DcatIndexTransformer",
    "DEFAULT_FIELD_SPECS",
    "coerce_identifier",
    "coerce_concepts",
    "concept_labels",
    "coerce_keywords",
    "coerce_publisher_name",
    "coerce_text",
    "distribution_titles",
    "coerce_in_series",
]


# Scalar sub-fields of a DCAT-US 3.0 Identifier that are worth searching on.
# Nested objects (creator, issued-as-object) and the redundant "@type" marker
# are dropped so the OpenSearch object mapping stays flat and predictable.
_IDENTIFIER_SCALAR_KEYS = ("@id", "schemaAgency", "notation", "version")

# Scalar / simple-list sub-fields of a DCAT-US 3.0 Concept worth searching on.
# "inScheme" (a nested ConceptScheme object) and "@type" are dropped.
_CONCEPT_KEYS = ("@id", "prefLabel", "altLabel", "notation", "definition")

# Scalar sub-fields of a DCAT-US 3.0 DatasetSeries worth searching on.
# Nested members (seriesMember, first, last, publisher, ...) and "@type"
# are dropped so the OpenSearch object mapping stays flat and predictable.
_IN_SERIES_KEYS = ("@id", "title", "description")


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


def coerce_identifier(value: Any) -> dict | None:
    """Coerce a DCAT identifier into a DCAT-US 3.0 ``Identifier`` object.

    * DCAT-US 1.1: a plain string ``"cftc-dc1"`` -> ``{"@id": "cftc-dc1"}``.
    * DCAT-US 3.0: an ``Identifier`` object, kept but reduced to its scalar
      search fields (``@id``, ``schemaAgency``, ``notation``, ``version``).

    Returns ``None`` when there is nothing indexable.
    """
    text = _clean_string(value)
    if text is not None:
        return {"@id": text}

    if isinstance(value, dict):
        reduced: dict[str, Any] = {}
        for key in _IDENTIFIER_SCALAR_KEYS:
            scalar = _clean_string(value.get(key))
            if scalar is not None:
                reduced[key] = scalar
        return reduced or None

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


def _coerce_dataset_series(value: Any) -> dict | None:
    """Coerce a single series value into a DCAT-US 3.0 ``DatasetSeries``."""
    text = _clean_string(value)
    if text is not None:
        return {"@id": text}

    if isinstance(value, dict):
        reduced: dict[str, Any] = {}
        for key in _IN_SERIES_KEYS:
            scalar = _clean_string(value.get(key))
            if scalar is not None:
                reduced[key] = scalar
        return reduced or None

    return None


def coerce_in_series(dcat: Any) -> list[dict]:
    """Coerce collection membership into a list of DCAT-US 3.0 ``DatasetSeries``.

    * DCAT-US 1.1: ``isPartOf`` string -> ``[{"@id": <string>}]``.
    * DCAT-US 3.0: ``inSeries`` list of ``DatasetSeries`` objects, kept but
      reduced to scalar search fields (``@id``, ``title``, ``description``).

    When ``inSeries`` is present and non-empty, it takes precedence over
    ``isPartOf``.
    """
    if not isinstance(dcat, dict):
        return []

    in_series = dcat.get("inSeries")
    if isinstance(in_series, list) and in_series:
        series_list = []
        for item in in_series:
            series = _coerce_dataset_series(item)
            if series is not None:
                series_list.append(series)
        if series_list:
            return series_list

    part_of = _clean_string(dcat.get("isPartOf"))
    if part_of is not None:
        return [{"@id": part_of}]

    return []


@dataclass(frozen=True)
class FieldSpec:
    """Configuration for a single derived index field.

    Attributes:
        name: The key produced in the transformed document.
        coerce: A callable that turns the source value (or the whole dcat dict
            when ``from_document`` is ``True``) into the indexed value.
        source: The dcat key to read. Defaults to ``name``. Ignored when
            ``from_document`` is ``True``.
        from_document: When ``True``, ``coerce`` receives the entire dcat dict
            rather than a single field value (used for cross-field derivations
            such as ``inSeries``).
    """

    name: str
    coerce: Callable[[Any], Any]
    source: str | None = None
    from_document: bool = False

    @property
    def source_key(self) -> str:
        return self.source if self.source is not None else self.name


# The default field table. This is the single place to configure which DCAT
# fields are indexed and how each DCAT-US 1.1 value maps up into DCAT-US 3.0.
DEFAULT_FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec(name="title", coerce=coerce_text),
    FieldSpec(name="description", coerce=coerce_text),
    FieldSpec(name="publisher", coerce=coerce_publisher_name),
    FieldSpec(name="keyword", coerce=coerce_keywords),
    FieldSpec(name="theme", coerce=coerce_concepts),
    FieldSpec(name="identifier", coerce=coerce_identifier),
    FieldSpec(
        name="distribution_titles",
        source="distribution",
        coerce=distribution_titles,
    ),
    FieldSpec(
        name="inSeries",
        coerce=coerce_in_series,
        from_document=True,
    ),
)


class DcatIndexTransformer:
    """Map a DCAT metadata dict into canonical OpenSearch index fields.

    The transformer is driven entirely by its :class:`FieldSpec` table, so
    callers can add, remove, or re-map fields without touching the coercion
    helpers. With no arguments it uses :data:`DEFAULT_FIELD_SPECS`.
    """

    def __init__(self, field_specs: Iterable[FieldSpec] | None = None):
        self.field_specs = (
            tuple(field_specs) if field_specs is not None else DEFAULT_FIELD_SPECS
        )

    def transform(self, dcat: dict | None) -> dict:
        """Return the canonical index-field dict for ``dcat``."""
        dcat = dcat or {}
        document: dict[str, Any] = {}
        for spec in self.field_specs:
            if spec.from_document:
                document[spec.name] = spec.coerce(dcat)
            else:
                document[spec.name] = spec.coerce(dcat.get(spec.source_key))
        return document
