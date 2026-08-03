# Catalog contract test

Harvester owns the Postgres schema (`migrations/`) and the OpenSearch index mapping
(`flask search reset-mapping`). [datagov-catalog](https://github.com/GSA/datagov-catalog)
reads both and owns neither — it has no migrations of its own. So a harvester change to
either can break catalog, and until this test existed nothing caught it before deploy.

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
3. **Document-shape contract** — catalog's tests index their fixtures through
   `datagov_data_access`'s own `OpenSearchWriter`, so `DatasetDocument` output is exercised
   end to end: writer → index → reader → template.

Point 3 only holds because the job **forces catalog's image onto harvester's pinned
`datagov-data-access` ref** before running pytest. Without that override the test is a
closed loop — catalog's own pinned writer would produce the documents catalog then reads,
and a harvester-side change to the models, `MAPPINGS`, `DatasetDocument`, or the filter
registry would go undetected. The ref is read from harvester's `pyproject.toml`
(`make print-data-access-ref`) and installed as a release tarball, since the Alpine-based
catalog image has no `git` binary.

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
- **The `datagov-data-access` pin moved without a matching harvester migration.** The ORM
  then expects DB-level behavior the schema doesn't have. Notably, 1.2.x adds
  `ondelete="CASCADE"` to six foreign keys plus `passive_deletes=True`, which tells
  SQLAlchemy to stop emitting child deletes and rely on the database to cascade. Bumping
  the pin without the corresponding migration produces integrity errors, and this job
  surfaces them as catalog test failures. That is a real bug, not a false positive —
  write the migration.
