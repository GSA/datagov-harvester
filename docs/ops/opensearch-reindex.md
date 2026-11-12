# Reindexing the OpenSearch dataset index

## The process: label the PR

To run a reindex, open a pull request with the change that requires it (a
mapping change, a document-shape change, etc.), attach the
**`force re-index recommended`** label, and merge it. The deploy pipeline
handles the rest automatically: it provisions a replacement OpenSearch
cluster, backfills every dataset into it from PostgreSQL, validates it, and
cuts over with no downtime. Nothing needs to be run by hand.

See [Zero-downtime OpenSearch deployment](opensearch-zero-downtime-deployment.md)
for the full release sequence and failure recovery.

## Title sorting rollout (GSA/data.gov#6512)

The `title.normalized` keyword subfield requires a full reindex of existing
datasets. Adding the mapping alone does not populate the subfield for documents
already in the index. Label the harvester mapping PR
`force re-index recommended` and complete the replacement-cluster release in
each environment before deploying catalog's `sort=title` support there.

Before deploying catalog, verify the replacement index has the subfield with
the `lowercase_normalizer` normalizer, and compare the counts of documents
with an indexed `title` and `title.normalized`. Check a search ordered by
`title.normalized` ascending, with missing values last and `_id` descending
as a tie-breaker, including a title containing uppercase letters.

After deploying catalog, smoke-test `/search?sort=title`, query/filter
preservation in the UI, and cursor pagination. If the catalog release needs
to be rolled back, the added subfield is compatible with the previous catalog
version; it does not require reverting the index mapping.

## Manual / advanced

The following exist for ops troubleshooting — repairing drift outside of a
deploy, or recovering from a failed release — not as the normal way to
reindex. There is no Airflow DAG or harvest-job type for this; it's `flask
search` CLI commands (`app/commands/search.py`) exposed through GitHub
Actions workflows.

### CLI commands

- **`flask search compare`** — reports how the DB and the index differ
  (missing, extra, and stale documents by `last_harvested_date`).
  - `--update` indexes missing/updated datasets and deletes extra documents.
  - `--force-update` re-indexes every dataset regardless of
    `last_harvested_date`. Implies `--update`. This is what the automated
    release flow runs after provisioning a replacement cluster.
  - `--fail-on-discrepancy` exits non-zero if there are any extra or stale
    documents, or more missing documents than `--max-failed-records` (default
    `50`) allows. Used to validate a replacement cluster before cutover.
- **`flask search reset-mapping`** — deletes the whole index and recreates it
  empty with the current mapping/settings. Destructive; only use when you
  intend to fully repopulate the index afterward.

Locally:

```bash
docker compose exec app flask search compare
docker compose exec app flask search compare --update
```

### GitHub Actions workflows

- **Sync OpenSearch** (`.github/workflows/synchronize_opensearch_index.yml`)
  — runs `flask search compare --update` against staging and prod daily at
  6am EDT to repair routine drift, and can be run manually against any single
  environment with a choice of `search reset-mapping` / `search compare` /
  `search compare --update` / `search compare --force-update`. Opens a GitHub
  issue if the task fails or its logs show an index-batch failure.

It runs against `datagov-harvest` via `cf run-task` and shares the
`opensearch-maintenance-<environment>` concurrency group with the release and
restart workflows, so they queue rather than run concurrently against the same
environment.
