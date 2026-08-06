# Catalog contract test

Harvester owns the Postgres schema (`migrations/`) and the OpenSearch index mapping
(`search/mappings.py`). [datagov-catalog](https://github.com/GSA/datagov-catalog) reads both
and owns neither — it has no migrations of its own. So a harvester change to the schema or
mapping can break catalog, and until this test existed nothing caught it before deploy.

This is deliberately **one-directional**: it checks that catalog's code, as it exists today
in its published `:main` image, still works against the Postgres schema and OpenSearch
mapping a harvester PR is proposing. It does not require harvester's and catalog's vendored
`search/` trees (each repo's copy of the former `datagov_data_access` code —
[#6209](https://github.com/GSA/data.gov/issues/6209) harvester-side,
[#6211](https://github.com/GSA/data.gov/issues/6211) catalog-side) to match file-for-file.
The two applications are allowed to diverge there over time; see "Why not tree parity?"
below.

The `Catalog Contract` GitHub Action runs on every harvester pull request. It provisions
Postgres and OpenSearch using *harvester's* migrations and mapping, then runs *catalog's own,
unmodified* test suite against them. Implements
[GSA/data.gov#6210](https://github.com/GSA/data.gov/issues/6210).

## What it actually proves

Two things, in one run:

1. **Schema contract** — catalog's queries work against the schema `flask db upgrade`
   produces. Catalog's test fixtures insert rows through its own SQLAlchemy models, so a
   dropped/renamed column, a changed enum, or a missing constraint fails loudly.
2. **Mapping contract** — catalog's searches, aggregations, and filters work against the
   index `flask search reset-mapping` creates.

Catalog's own `OpenSearchWriter`, `DatasetDocument`, and filter registry index the test
fixtures and read them back — none of that is touched or replaced by harvester's copies.
That is intentional: this job answers "does catalog's current code still work," not "are the
two vendored trees identical." A harvester-side change to `MAPPINGS`, `DatasetDocument`, the
writer, or the filter registry that catalog's copy doesn't know about yet will not, by
itself, fail this job — see below for why.

## Why not tree parity?

An earlier version of this job overlaid harvester's vendored `search/` modules onto
catalog's tree before running catalog's tests, and hard-failed if harvester had a module
catalog lacked. That check answered a different question — "are the two trees identical" —
which has two problems:

- It penalizes pure additions (e.g. a new filter module) exactly as hard as a breaking
  change, because it can't tell the two apart.
- It forces the two repos' releases into lockstep: harvester couldn't ship a `search/`
  addition until catalog had already merged and released a matching file to `main`, even
  when nothing about catalog's current behavior was at risk. Timing that across two
  independently deployed apps is itself an outage risk if the timing slips.

What actually matters operationally is whether harvester's *schema and mapping* changes
break catalog's *current, deployed* code — that's what a bad migration or mapping change
does in production, independent of whether the two `search/` trees match. This job checks
that directly by running catalog's real test suite against harvester's proposed services,
and leaves `search/` drift between the repos as an accepted, unenforced condition.

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

Read it as "this harvester change breaks catalog's current, deployed code." Two failure
modes are worth calling out because neither is a flaky test:

- **A migration removed or changed something catalog reads.** Either keep it, or land the
  catalog change first.
- **`database/models.py` changed without a matching migration.** The ORM then expects
  DB-level behavior the schema doesn't have — for example, adding `ondelete="CASCADE"` plus
  `passive_deletes=True` tells SQLAlchemy to stop emitting child deletes and rely on the
  database to cascade, which produces integrity errors if the constraint was never migrated.
  `flask db check` in the `Pytests` job catches most of this class; anything that slips
  through surfaces here as a catalog failure. Either way it's a real bug, not a false
  positive — write the migration.

A harvester-side `search/` change (mapping, document shape, filter registry) that catalog's
own copy doesn't know about yet will **not** fail this job by itself, since catalog's copy
isn't touched — see "Why not tree parity?" above. If such a change also alters the
OpenSearch mapping in a way catalog's current queries can't handle, that still surfaces here
as a mapping-contract failure; only the module-for-module identity check was removed.
Catalog's `commit.yml` has a non-blocking text-diff of harvester's `search/mappings.py` that
makes `search/` drift between the repos visible without blocking either side.
