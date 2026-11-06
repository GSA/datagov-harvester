# Locations Dataset Update

Related issue: https://github.com/GSA/data.gov/issues/5111

## Objective

Research and prepare an updated locations dataset for Data.gov
spatial metadata translation.

## Existing CSV format

name,type,display_name,the_geom,type_order,id

The existing geometry is represented as hexadecimal WKB/EWKB.

## Proposed approach

1. Evaluate newer geographic datasets, prioritizing polygon boundaries.
2. Preserve compatibility with the existing location lookup.
3. Convert source geometries into the required format.
4. Validate location names, geometry, and duplicate records.
5. Load validated records using a separate database import process.

## Status

Dataset selection and database schema verification are pending.
No database changes have been made.

## GSA issue #5111 [G Updated location boundaries

This directory contains preparation and validation tools for refreshing
the existing Data.gov locations dataset while preserving record IDs,
names, display names, types, and ordering.

### Geographic sources

- States: 2026 Census TIGER/Line State boundaries.
- Counties: 2026 Census TIGER/Line County boundaries.
- Countries: Natural Earth Admin 0 Countries, downloaded shapefile.
- Postal codes: 2025 Census ZIP Code Tabulation Areas (ZCTA5).

### Initial conversion results

| Type | Existing | Replacement candidates |
|------|----------|------------------------|
| States | 51 | 51 |
| Counties | 3141 | 3074 |
| Countries | 250 | 229 |
| Postal-code location records | 60288 | 59566 |
| Total | 63730 | 62920 |

Unmatched records retain their original geometry during merging.

### Important limitations

Census ZCTAs are geographic approximations of USPS ZIP-code
delivery areas. They are not equivalent to USPS ZIP codes.

County and country matches were initially based on names and require
review for administrative changes and geographic equivalence.

The source versions differ by geography; this is not a single
uniformly dated geographic dataset.

### Safety

- Do not commit original or generated location CSVs.
- Do not commit downloaded shapefile archives.
- Do not modify production data as part of dataset preparation.
- Validate the complete merged CSV before importing.
- Test imports against a disposable PostGIS database first.
- Confirm destination table schema, geometry type, and ID mapping.
- Back up the destination before production deployment.
- Obtain review and approval before applying changes.

The importer defaults to a rolled-back dry run. Actual imports
require --apply and a configured LOCATIONS_DATABASE_URL.
