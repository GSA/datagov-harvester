# Catalog contract test

Harvester owns the Postgres schema (`migrations/`), the OpenSearch index mapping
(`search/mappings.py`), and the indexed document shape (`search/documents.py`).
[datagov-catalog](https://github.com/GSA/datagov-catalog) reads all three and owns none of
them — it has no migrations of its own. So a harvester change can break catalog, and until
this test existed nothing caught it before deploy.

The `Catalog Contract` GitHub Action runs on every harvester pull request. It provisions
Postgres and OpenSearch using *harvester's* migrations and mapping, then runs *catalog's*
test suite — catalog's own code, exactly as it ships — against them.
Implements [GSA/data.gov#6210](https://github.com/GSA/data.gov/issues/6210).

## The one question it answers

**Will this harvester change break catalog?** That is the whole scope. If the job is green,
the harvester change can ship without waiting on catalog. If it's red, catalog needs a
change first — ideally one that supports both the old and new shape, so the two deploys
don't have to be timed together.

Two things it proves, in one run:

1. **Schema contract** — catalog's queries work against the schema `flask db upgrade`
   produces. Catalog's test fixtures insert rows through its own SQLAlchemy models, so a
   dropped/renamed column, a changed enum, or a missing constraint fails loudly.
2. **Mapping contract** — catalog's searches, aggregations, and filters work against the
   index `flask search reset-mapping` creates.

Catalog's `tests/unit` runs against real Postgres and real OpenSearch — only the HTTP layer
is faked, via Flask's in-process `test_client()`. So these are integration tests in
everything but the directory name.

## What it deliberately does *not* check

**Drift between harvester's `search/` and catalog's `app/search/`.** Both repos vendored the
same `datagov_data_access` 1.1.0 code and dropped the dependency — harvester into `database/`
and `search/` ([#6209](https://github.com/GSA/data.gov/issues/6209)), catalog into
`app/models.py` and `app/search/` ([#6211](https://github.com/GSA/data.gov/issues/6211)).
The two trees are **allowed to diverge**, and nothing here enforces otherwise.

An earlier version of this job copied harvester's `search/` modules over catalog's before
running pytest, and hard-failed when harvester had a module catalog lacked. That turned every
purely additive harvester change into a blocking cross-repo dependency — exactly the lockstep
coupling this test is meant to avoid. Harvester and catalog should not have to ship together.

The cost of dropping the overlay, stated plainly: catalog's tests index their fixtures
through *catalog's* `OpenSearchWriter`, so the document-shape path (`DatasetDocument`, the
writer) is a closed loop and a harvester-side change to it won't be caught here. The schema
and mapping contracts are unaffected. Catalog's committed
`tests/data/harvester_snapshot/` — a real dump from a migrated, fixture-loaded harvester —
covers part of that gap from catalog's side.

Also not checked: `database/models.py` is not substituted into catalog. Catalog's fixtures
must exercise the schema harvester's migrations actually built; swapping in harvester's model
definitions would make that check tautological. Catalog's own slim models in `app/models.py`
are the client under test.

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

Read it as "this harvester change breaks catalog" — never as "harvester and catalog have
drifted." A failure means catalog, running its current production code, could not work
against the schema or mapping this PR produces. Three failure modes, none of them flaky:

- **A migration removed or changed something catalog reads.** A dropped or renamed column, a
  changed enum, a tightened constraint.
- **`database/models.py` changed without a matching migration.** The ORM then expects
  DB-level behavior the schema doesn't have — for example, adding `ondelete="CASCADE"` plus
  `passive_deletes=True` tells SQLAlchemy to stop emitting child deletes and rely on the
  database to cascade, which produces integrity errors if the constraint was never migrated.
  `flask db check` in the `Pytests` job catches most of this class; anything that slips
  through surfaces here. Either way it's a real bug — write the migration.
- **A mapping change catalog can't read.** Changing an existing field's type or analyzer, or
  removing a field catalog queries. Note that *adding* a field is additive and will not fail
  this job — catalog ignores mapping fields it doesn't know about.

### Getting unblocked without timing two deploys

The point of a red check is to catch the breakage before deploy, not to force a synchronized
release. Preferred order:

1. **Make the catalog change dual-support** — able to read both the current and the proposed
   shape (tolerate the old column and the new one, both mapping types, etc.).
2. **Ship that to catalog first.** This job goes green once the change is on catalog's `main`
   and the image republishes.
3. **Ship the harvester change.**
4. **Simplify catalog** to drop the old branch once harvester is fully deployed.

That sequence has no window where either app is broken, regardless of deploy timing. To verify
step 1 before it merges, point the job at your catalog branch's image with `CATALOG_IMAGE`
(see above) — no need to guess.
