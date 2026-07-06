# OpenSearch DCAT-US 3.0 Index Migration Guide

This document describes how the `datasets` OpenSearch index stores DCAT metadata
after the DCAT-US 3.0 indexing work. Use it to refactor consumer applications
(search UIs, APIs, faceting logic) that query the index.

**Index name:** `datasets`

**Implementation:** `harvester/opensearch.py`, `harvester/opensearch_transform.py`

---

## Why the index changed

DCAT-US 1.1 and 3.0 describe several of the same fields with **different JSON
shapes**. OpenSearch requires a single, stable type per field, so the harvester
now **maps all source data up into the DCAT-US 3.0 structure at index time**.

Both schema versions can coexist in one index:

- A DCAT-US 1.1 dataset with `"theme": "Geospatial"` is stored as
  `"theme": [{"prefLabel": "Geospatial"}]`.
- A DCAT-US 3.0 dataset keeps its richer object structure (reduced to searchable
  scalar sub-fields).

This is a **breaking change** for top-level fields that changed from `text` to
`object`. Consumer queries must be updated. A full **reindex** is required after
deploying the new mapping.

---

## Architecture: two layers in every document

Each indexed dataset document has two representations of DCAT metadata:

| Layer | Purpose | Shape |
|-------|---------|-------|
| **Top-level search fields** | Typed search, filtering, faceting | Canonical DCAT-US 3.0 objects |
| **`dcat` nested blob** | Full metadata reference, broad text search | Flattened strings / serialized JSON |

**Rule of thumb for consumer apps:** use **top-level fields** for filters,
facets, and structured display. Use the `dcat` blob only for fields not promoted
to the top level, or for debugging / raw metadata views.

---

## Fields that changed (action required)

These top-level fields changed type or query paths. Update consumer queries
accordingly.

### `identifier`

| | Before (DCAT 1.1 index) | After (DCAT 3.0 index) |
|-|-------------------------|-------------------------|
| **Type** | `text` | `object` (Identifier) |
| **1.1 source** | `"identifier": "cftc-dc1"` | `{"@id": "cftc-dc1"}` |
| **3.0 source** | N/A (was flattened) | `{"@id": "...", "notation": "...", "schemaAgency": "...", "version": "..."}` |

**Indexed sub-fields:**

| Sub-field | Type | Use for |
|-----------|------|---------|
| `identifier.@id` | text (+ `.keyword`) | Free-text and exact ID search |
| `identifier.notation` | keyword | Alternate notation (e.g. agency code) |
| `identifier.schemaAgency` | text | Issuing agency name |
| `identifier.version` | keyword | Identifier version |

**Query migration:**

```json
// OLD — exact match on plain string
{ "term": { "identifier": "cftc-dc1" } }

// NEW — exact match on Identifier @id
{ "term": { "identifier.@id.keyword": "cftc-dc1" } }

// NEW — free-text search on @id
{ "match": { "identifier.@id": "cftc-dc1" } }

// NEW — match on notation
{ "term": { "identifier.notation": "DATASET-1" } }
```

**Display in hit `_source`:**

```json
"identifier": {
  "@id": "https://example.gov/identifiers/dataset-1",
  "notation": "DATASET-1"
}
```

For DCAT-US 1.1 datasets that only had a string identifier, expect
`{"@id": "<original-string>"}` with no other sub-fields.

---

### `theme`

| | Before | After |
|-|--------|-------|
| **Type** | `text` (or list of strings in `_source`) | `object` (list of Concept objects) |
| **1.1 source** | `"theme": ["Geospatial"]` or `"theme": "Geospatial"` | `[{"prefLabel": "Geospatial"}]` |
| **3.0 source** | N/A (was flattened) | `[{"@id": "...", "prefLabel": "Climate Science", ...}]` |

**Indexed sub-fields (per concept):**

| Sub-field | Type | Use for |
|-----------|------|---------|
| `theme.@id` | keyword | Concept URI |
| `theme.prefLabel` | text (+ `.keyword`, normalized) | Display label; **primary facet field** |
| `theme.altLabel` | text | Alternate label search |
| `theme.definition` | text | Definition text search |
| `theme.notation` | keyword | Concept code |

**Query migration:**

```json
// OLD — term or match on plain string
{ "term": { "theme": "Geospatial" } }

// NEW — facet / exact match on label (case-insensitive via normalizer)
{ "term": { "theme.prefLabel.keyword": "geospatial" } }

// NEW — match concept URI
{ "term": { "theme.@id": "https://example.gov/concepts/geospatial" } }

// NEW — full-text search on label
{ "match": { "theme.prefLabel": "climate science" } }
```

**Aggregation migration:**

```json
// OLD
{ "terms": { "field": "theme" } }

// NEW
{ "terms": { "field": "theme.prefLabel.keyword" } }
```

**Display in hit `_source`:**

```json
"theme": [
  { "prefLabel": "Geospatial" }
]
```

or for full DCAT-US 3.0 data:

```json
"theme": [
  {
    "@id": "https://example.gov/concepts/climate-science",
    "prefLabel": "Climate Science",
    "altLabel": "Climatology"
  }
]
```

---

### `inSeries` (new top-level field; replaces `dcat.isPartOf` for search)

Collection / series membership moved from DCAT-US 1.1 `isPartOf` (string) to
DCAT-US 3.0 `inSeries` (list of DatasetSeries objects).

| | Before | After |
|-|--------|-------|
| **Query field** | `dcat.isPartOf` (nested keyword) | `inSeries` (top-level object array) |
| **1.1 source** | `"isPartOf": "https://catalog.data.gov/dataset/my-collection"` | `"inSeries": [{"@id": "https://catalog.data.gov/dataset/my-collection"}]` |
| **3.0 source** | N/A | `"inSeries": [{"@id": "...", "title": "...", "description": "..."}]` |

**Indexed sub-fields (per series):**

| Sub-field | Type | Use for |
|-----------|------|---------|
| `inSeries.@id` | keyword | Series URI — **primary filter field** |
| `inSeries.title` | text (+ `.keyword`, normalized) | Series title search / facet |
| `inSeries.description` | text | Series description search |

**Query migration:**

```json
// OLD — filter by collection via nested dcat blob
{
  "nested": {
    "path": "dcat",
    "query": {
      "term": { "dcat.isPartOf": "https://catalog.data.gov/dataset/my-collection" }
    }
  }
}

// NEW — filter by series URI (no nested query needed)
{
  "term": {
    "inSeries.@id": "https://example.gov/series/annual-climate-observations"
  }
}

// NEW — facet by series title
{ "terms": { "field": "inSeries.title.keyword" } }
```

**Notes:**

- `dcat.isPartOf` may still appear in the `dcat` blob for legacy datasets, but
  **do not rely on it for filtering** — use `inSeries` instead.
- A dataset can belong to **multiple** series (`inSeries` is always a list).
- When both `inSeries` and `isPartOf` are present in source DCAT, `inSeries`
  takes precedence at index time.

**Display in hit `_source`:**

```json
"inSeries": [
  {
    "@id": "https://example.gov/series/annual-climate-observations",
    "title": "Annual Climate Observations Series",
    "description": "An annual series of national climate observations datasets."
  }
]
```

---

## Fields unchanged (no query changes)

These top-level fields keep the same type and query paths as before.

| Field | Type | Notes |
|-------|------|-------|
| `title` | text | Full-text search, unchanged |
| `description` | text | Full-text search, unchanged |
| `keyword` | text (+ `.raw`, `.normalized`) | Facet on `keyword.normalized` |
| `publisher` | text (+ `.raw`, `.normalized`) | Display name string extracted from DCAT publisher object; facet on `publisher.normalized` |
| `distribution_titles` | text | List of distribution title strings |
| `slug` | keyword | Exact match |
| `last_harvested_date` | date | Range queries |
| `has_spatial` | boolean | Filter on geospatial datasets |
| `popularity` | integer | Sort / filter |
| `organization` | nested | Harvester org metadata (not DCAT publisher) |
| `spatial_shape` | geo_shape | Geo queries |
| `spatial_centroid` | geo_point | Geo queries |
| `harvest_record` | keyword/url | Harvest record links (when present) |

### `publisher` — intentionally still a string

DCAT-US 3.0 models publisher as an `Organization` object, but the index
continues to store a **single display name string** at the top level to preserve
the existing facet contract:

```json
"publisher": "National Climate Data Center"
```

Queries on `publisher`, `publisher.raw`, and `publisher.normalized` are
unchanged.

---

## The `dcat` nested blob

The `dcat` field stores the full dataset metadata, normalized for indexing:

- **Dates** (`modified`, `issued`) are ISO strings.
- **DCAT-US 3.0 objects** in fields like `landingPage`, `status`, `temporal`,
  `conformsTo`, etc. are **flattened to human-readable strings** inside the
  blob — not structured objects.
- Explicit nested sub-mappings exist for `dcat.modified`, `dcat.issued`, and
  `dcat.isPartOf` (legacy).

**Do not use the blob for fields that now have typed top-level equivalents:**

| Blob field | Use top-level field instead |
|------------|----------------------------|
| `dcat.identifier` (flattened string) | `identifier` (Identifier object) |
| `dcat.theme` (flattened strings) | `theme` (Concept objects) |
| `dcat.isPartOf` | `inSeries` (DatasetSeries objects) |

The blob remains useful for fields not promoted to the top level (e.g.
`dcat.modified`, `dcat.issued`) and for displaying raw-ish metadata.

---

## Complete before / after field reference

| Field | Old `_source` shape | New `_source` shape | Query impact |
|-------|---------------------|---------------------|--------------|
| `identifier` | `"id-1"` | `{"@id": "id-1"}` | **Breaking** — use sub-fields |
| `theme` | `["Geospatial"]` | `[{"prefLabel": "Geospatial"}]` | **Breaking** — use sub-fields |
| `inSeries` | *(not indexed)* | `[{"@id": "..."}]` | **New field** — migrate from `dcat.isPartOf` |
| `title` | string | string | None |
| `description` | string | string | None |
| `keyword` | string[] | string[] | None |
| `publisher` | string | string | None |
| `distribution_titles` | string[] | string[] | None |
| `dcat.isPartOf` | string (in blob) | string (in blob, legacy only) | **Deprecated for queries** |

---

## Example: full document shape after migration

```json
{
  "_index": "datasets",
  "_id": "dataset-uuid",
  "_source": {
    "title": "Dataset Title",
    "description": "Dataset description",
    "slug": "dataset-slug",
    "publisher": "Publisher Name",
    "keyword": ["kw-1", "kw-2"],
    "identifier": {
      "@id": "id-1"
    },
    "theme": [
      { "prefLabel": "Geospatial" }
    ],
    "inSeries": [
      { "@id": "https://catalog.data.gov/dataset/my-collection" }
    ],
    "distribution_titles": ["CSV download", "API endpoint"],
    "has_spatial": true,
    "last_harvested_date": "2024-01-03T04:05:06.000Z",
    "organization": { "id": "org-1", "name": "Test Org", "slug": "test-org" },
    "dcat": {
      "modified": "2024-01-02",
      "issued": "2024-01-03T04:05:06",
      "isPartOf": "https://catalog.data.gov/dataset/my-collection",
      "title": "Dataset Title",
      "theme": ["Geospatial"],
      "identifier": "id-1"
    }
  }
}
```

Note the intentional difference: top-level fields use DCAT-US 3.0 object shapes;
the `dcat` blob retains flattened text for reference.

---

## Consumer app refactor checklist

### Queries

- [ ] Replace `term` / `match` on `identifier` → `identifier.@id` or
      `identifier.@id.keyword`
- [ ] Replace `term` / `match` on `theme` → `theme.prefLabel.keyword` (facets)
      or `theme.prefLabel` (search)
- [ ] Replace nested `dcat.isPartOf` filters → `inSeries.@id`
- [ ] Update any `terms` aggregations on `theme` → `theme.prefLabel.keyword`
- [ ] Add `inSeries.title.keyword` aggregation if faceting by series name

### Result parsing / display

- [ ] Read `identifier` as an object; display `identifier.@id` or
      `identifier.notation`
- [ ] Read `theme` as an array of objects; display `theme[].prefLabel`
- [ ] Read `inSeries` as an array of objects; display `inSeries[].title` or
      `inSeries[].@id`
- [ ] Do not assume `theme` or `identifier` are plain strings in `_source`

### Unchanged

- [ ] `publisher`, `keyword`, `title`, `description`, `distribution_titles` —
      no changes
- [ ] `publisher.normalized`, `keyword.normalized` facets — no changes
- [ ] Geo queries on `spatial_shape` / `spatial_centroid` — no changes
- [ ] `has_spatial` boolean filter — no changes

### Deployment

- [ ] Coordinate with index reindex (`flask search reset-mapping` + full reindex)
      before switching consumer queries in production
- [ ] During transition, old and new index shapes cannot coexist in the same
      index — deploy harvester and reindex atomically from the consumer's
      perspective

---

## Quick reference: primary query paths

| Use case | Field path |
|----------|------------|
| Exact dataset ID | `identifier.@id.keyword` |
| Search dataset ID (analyzed) | `identifier.@id` |
| Agency notation | `identifier.notation` |
| Facet by theme | `theme.prefLabel.keyword` |
| Search theme text | `theme.prefLabel` |
| Theme concept URI | `theme.@id` |
| Filter by collection / series | `inSeries.@id` |
| Facet by series name | `inSeries.title.keyword` |
| Facet by publisher | `publisher.normalized` |
| Facet by keyword | `keyword.normalized` |
| Filter geospatial datasets | `has_spatial` |
| Date range on metadata | `dcat.modified`, `dcat.issued` (nested) |
