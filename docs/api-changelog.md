# Harvest API Changelog

This changelog documents consumer-facing changes to the Data.gov Harvest API.

Changes that may affect API consumers are documented here, including changes
to endpoints, request parameters, response fields, response formats,
pagination, ordering, authentication, and deprecations.

Issues that result in an entry in this changelog should be labeled
`changelog` in the GSA/data.gov issue tracker.

## Unreleased

### Added

#### Validator warnings and duplicate identifiers

**Issue:** GSA/data.gov#6380  
**Endpoint:** `POST /api/v1/validate`

The validator now also reports what a harvest of the catalog would report
beyond schema errors, for both DCAT-US 1.1 and 3.0:

- New `validation_warnings` field (always present): a list of
  `{identifier, record_type, warning_type, message}` objects for
  content-quality warnings, including spatial values that can't be resolved.
  `warning_type` is the same value the harvester records for that warning.
  Warnings never make a catalog invalid.
- New `warnings_incomplete` field (present only when true): warning checks
  stopped at the 1000-warning limit, or the catalog was too malformed to split
  into records.
- Duplicate identifiers are now included in `validation_errors` as
  `[identifier, "Duplicate identifier '<identifier>' found"]`, because the
  harvester skips the duplicate record. They count toward the existing
  1000-error limit.

Existing `validation_errors` entries are unchanged.

Known gap: the validator does no place-name lookup, so a free-text spatial
value (e.g. `"United States"`) always produces a warning, even for the names
the harvester can resolve from its locations table. See "Known gap: place
names" in `docs/validator-security.md`.

#### Outstanding record issues CSV download

**Issue:** GSA/data.gov#6476  
**Endpoint:** `GET /api/v1/harvest_source/<source_id>/warnings/download`

New endpoint that streams a CSV export of a harvest source's current
outstanding record issues (errors and warnings), independent of any single
job. Columns: `record_error_id`, `identifier`, `title`, `harvest_record_id`,
`record_error_type`, `severity`, `message`, `date_created`.

This mirrors the existing per-job error CSV export
(`GET /api/v1/harvest_job/<job_id>/errors/<error_type>`) but is scoped to a
source's current state rather than a single job's run.
