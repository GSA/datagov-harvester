# Harvester API Changelog

This changelog documents consumer-facing changes to the Data.gov Harvester API.

Changes that may affect API consumers are documented here, including changes
to endpoints, request parameters, response fields, response formats,
pagination, ordering, authentication, and deprecations.

Issues that result in an entry in this changelog should be labeled
`changelog` in the GSA/data.gov issue tracker.

See [app/api/README.md](../app/api/README.md) for the API versioning policy.

## Unreleased

### Added

#### Optional structured response envelope for collection endpoints

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints

Collection endpoints now support an optional `?envelope=true` query parameter
that returns a structured object with `{data, meta, links}` format instead of
the default array response.

**Affected endpoints:**
- `GET /api/v1/organizations/`
- `GET /api/v1/harvest_sources/`
- `GET /api/v1/harvest_jobs/`
- `GET /api/v1/harvest_records/`
- `GET /api/v1/harvest_job_errors/`
- `GET /api/v1/harvest_record_errors/`

**Default response format (backward compatible):**
```json
[
  {"id": 1, "name": "Example"},
  {"id": 2, "name": "Another"}
]
```

**With ?envelope=true:**
```json
{
  "data": [
    {"id": 1, "name": "Example"},
    {"id": 2, "name": "Another"}
  ],
  "meta": {
    "page": 0,
    "per_page": 10,
    "total": 2,
    "page_count": 1,
    "order_by": "-date_created",
    "api_version": "v1",
    "application_version": "a1b2c3d"
  },
  "links": {
    "self": "/api/v1/organizations/?page=0&per_page=10&envelope=true",
    "next": "/api/v1/organizations/?page=1&per_page=10&envelope=true",
    "previous": null,
    "documentation": "/openapi/docs",
    "changelog": "/api/changelog"
  }
}
```

**Compatibility:** This is a non-breaking addition. Existing API consumers
continue to receive array responses. New consumers can opt into the structured
format by adding `?envelope=true` to their requests.

**Benefits of envelope format:**
- Pagination metadata (`page`, `per_page`, `total`, `page_count`) enables proper pagination UI
- `order_by` field reports the actual applied sort order
- Version tracking (`api_version`, `application_version`) aids debugging
- HATEOAS navigation links (`next`, `previous`, `self`) enable API discoverability
- Consistent response structure across all collection endpoints

#### Pagination metadata in collection responses

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints (with `?envelope=true`)

When using `?envelope=true`, collection endpoints include complete pagination
metadata in the `meta` object:

- `meta.page` - Current page number (0-indexed)
- `meta.per_page` - Items per page
- `meta.total` - Total number of items across all pages
- `meta.page_count` - Total number of pages

#### Ordering metadata in collection responses

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints (with `?envelope=true`)

When using `?envelope=true`, the `meta.order_by` field reports the actual
applied ordering (e.g., `-date_created`), making it clear how results are sorted.
This is especially useful when the endpoint applies a default sort order.

#### Version metadata in collection responses

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints (with `?envelope=true`)

When using `?envelope=true`, collection responses include version information:

- `meta.api_version` - API version from request path (e.g., `v1`)
- `meta.application_version` - Application commit SHA from deployment

The application version is sourced from the `APP_VERSION` environment variable
(set during deployment) or falls back to `git rev-parse` in development.

#### HATEOAS navigation links in collection responses

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints (with `?envelope=true`)

When using `?envelope=true`, the `links` object provides hypermedia navigation:

- `links.self` - Current request URL with pagination parameters
- `links.next` - Next page URL (`null` if on last page)
- `links.previous` - Previous page URL (`null` if on first page)
- `links.documentation` - Link to OpenAPI/Swagger UI at `/openapi/docs`
- `links.changelog` - Link to API changelog at `/api/changelog`

### Fixed

#### Pagination edge cases now handled correctly

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints

Pagination logic now correctly handles edge cases including page 0, per_page
limits (min 1, max 100), and queries that return no results.

#### Count queries optimized

**Issue:** GSA/data.gov#6392  
**Endpoints:** All collection endpoints with `?count=true`

Count queries (using `?count=true` parameter) are now optimized to avoid
fetching unnecessary data, improving performance for large result sets.
