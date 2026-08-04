# Catalog contract test

Harvester owns the Postgres schema (`migrations/`), the OpenSearch index mapping
(`search/mappings.py`), and the indexed document shape (`search/documents.py`).
[datagov-catalog](https://github.com/GSA/datagov-catalog) reads all three and owns none of
them — it has no migrations of its own. So a harvester change can break catalog, and until
this test existed nothing caught it before deploy.

This matters more since [#6209](https://github.com/GSA/data.gov/issues/6209) vendored the
`datagov_data_access` code into `database/` and `search/` and dropped the dependency.
Catalog still pins `datagov-data-access@1.1.0`, so the two repos no longer share a package —
the code is currently identical apart from import paths, but nothing enforces that. This
test is what keeps the two copies honest until catalog is migrated off the library too.

The `Catalog Contract` GitHub Action runs on every harvester pull request. It provisions
Postgres and OpenSearch using *harvester's* migrations and mapping, then runs *catalog's*
test suite against them. Implements [GSA/data.gov#6210](https://github.com/GSA/data.gov/issues/6210).

## What it actually proves

Three things, in one run:

1. **Schema contract** — catalog's queries work against the schema `flask db upgrade`
   produces. Catalog's test fixtures insert rows through the shared SQLAlchemy models, so a
   dropped/renamed column, a changed enum, or a missing constraint fails loudly.
2. **Mapping contract** — catalog's searches, aggregations, and filters work against the
   index `flask search reset-mapping` creates.
3. **Document-shape contract** — catalog's tests index their fixtures through the
   `OpenSearchWriter`, so `DatasetDocument` output is exercised end to end:
   writer → index → reader → template.

Point 3 only holds because the job **overlays harvester's vendored `search/` package (and
`shared/constants.py`) onto catalog's `datagov_data_access` install** before running pytest.
Without that the test is a closed loop — catalog's own pinned writer would produce the
documents catalog then reads, so a harvester-side change to `MAPPINGS`, `DatasetDocument`,
the writer, or the filter registry would go completely undetected.

The overlay copies the files in and rewrites import paths (`search.*` →
`datagov_data_access.search.*`, `database.models` → `datagov_data_access.db.models`,
`shared.constants` → `datagov_data_access.shared.constants`), which is sufficient because
the vendored code is otherwise identical to the release. It then greps for any unrewritten
path and fails hard if one remains, so a silent fallback to catalog's own copy is not
possible.

Note the overlay deliberately stops at `search/` and `shared/constants.py`. `database/models.py`
is **not** overlaid: catalog's fixtures must exercise the schema that harvester's migrations
actually built, and substituting harvester's model definitions would make that check
tautological.

## Running it locally

```bash
make test-catalog-contract
```

That chains three targets, and the order matters:

| Target | What it does |
| --- | --- |
| `catalog-contract-up` | Starts Postgres (PostGIS) and OpenSearch, waits for both healthy |
| `catalog-contract-provision` | `flask db upgrade` then `flask search reset-mapping` |
| `catalog-contract-test` | Runs catalog's `tests/unit` against those services |

Provision must finish before the tests start. Catalog's `create_app()` connects to
OpenSearch at import and will create the index with *its own* mapping if the index is
missing — which would quietly defeat the whole test. Migrations also terminate other
database backends, so the two steps must not overlap.

Results land in `reports/catalog-contract.xml` (JUnit; uploaded as a CI artifact).
Tear down with `make catalog-contract-down`; `make clean` includes it.

The stack uses its own compose project name and publishes **no host ports**, so it won't
collide with `make up` (which already occupies 5432, 5433 and 9200).

### Testing against an unmerged catalog branch

By default the job pulls `ghcr.io/gsa/datagov-catalog:main`, published by catalog's
`publish-image` workflow. Override it to test a specific catalog commit:

```bash
CATALOG_IMAGE=ghcr.io/gsa/datagov-catalog:sha-<full-sha> make test-catalog-contract
```

Or build catalog locally and point at that:

```bash
cd ../datagov-catalog && docker build --build-arg DEV=True -t catalog-dev-test .
cd ../datagov-harvester && CATALOG_IMAGE=catalog-dev-test make test-catalog-contract
```

## How catalog cooperates

Catalog's `tests/unit/conftest.py` normally does `db.drop_all()` / `db.create_all()` before
every test, which would destroy the very schema under test. When
`CATALOG_TEST_EXTERNAL_SCHEMA` is set (this job sets it), that fixture instead `TRUNCATE`s
the model tables and leaves the DDL — and `alembic_version` — alone.

If you ever see this job pass when it shouldn't, check that variable first: with it unset,
catalog rebuilds the schema from its own models and the job goes green even against a
broken migration.

## When this job fails

Read it as "this harvester change breaks catalog." Two failure modes are worth calling out
because neither is a flaky test:

- **A migration removed or changed something catalog reads.** Either keep it, or land the
  catalog change first.
- **`database/models.py` changed without a matching migration.** The ORM then expects
  DB-level behavior the schema doesn't have — for example, adding `ondelete="CASCADE"` plus
  `passive_deletes=True` tells SQLAlchemy to stop emitting child deletes and rely on the
  database to cascade, which produces integrity errors if the constraint was never migrated.
  `flask db check` in the `Pytests` job catches most of this class; anything that slips
  through surfaces here as a catalog failure. Either way it's a real bug, not a false
  positive — write the migration.
- **`search/` changed in a way catalog can't read.** Since the vendored copy and catalog's
  pinned `datagov-data-access@1.1.0` are meant to stay equivalent, a mapping, document, or
  filter-registry change that breaks catalog needs a coordinated catalog release. Once
  catalog is migrated off the library ([#6211](https://github.com/GSA/data.gov/issues/6211)
  and follow-ons), the overlay in `docker-compose.catalog-contract.yml` can be dropped and
  catalog can import the vendored code directly.
