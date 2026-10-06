# Harvest API Changelog

This changelog documents consumer-facing changes to the Data.gov Harvest API.

Changes that may affect API consumers are documented here, including changes
to endpoints, request parameters, response fields, response formats,
pagination, ordering, authentication, and deprecations.

Issues that result in an entry in this changelog should be labeled
`changelog` in the GSA/data.gov issue tracker.

## Unreleased

### Changed

#### Harvest Jobs date format

**Issue:** GSA/data.gov#5850  
**Endpoint:** `GET /api/v1/harvest_jobs/`

The `date_created`, `date_started`, and `date_finished` response fields are
being updated from RFC/HTTP date strings to ISO 8601 UTC timestamps.

Previous format:

```text
Tue, 19 May 2026 03:02:05 GMT
```

New format:

```text
2026-05-19T03:02:05.678Z
```

**Compatibility:** This is a potentially breaking change for API consumers
that explicitly parse the previous date representation.

The change is currently available in the development environment and is being
evaluated for API versioning and consumer communication before production
release.
