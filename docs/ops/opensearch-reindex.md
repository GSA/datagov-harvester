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
- **`flask search rebuild-index`** — zero-downtime rebuild: creates a new
  physical index, backfills every dataset from PostgreSQL into it, validates
  the document count against the DB, then atomically switches the `datasets`
  alias to it. The old index keeps serving reads until the switch. This is
  the manual equivalent of what the label-driven release does, without a PR
  or a replacement Cloud Foundry service.
- **`flask search delete-index --index-name <name>`** — deletes an unused
  physical index left behind by a rebuild. Refuses to delete an index still
  attached to the `datasets` alias.

Locally:

```bash
docker compose exec app flask search compare
docker compose exec app flask search compare --update
docker compose exec app flask search rebuild-index --no-switch-alias
```

### GitHub Actions workflows

- **Sync OpenSearch** (`.github/workflows/synchronize_opensearch_index.yml`)
  — runs `flask search compare --update` against staging and prod daily at
  6am EDT to repair routine drift, and can be run manually against any single
  environment with a choice of `search reset-mapping` / `search compare` /
  `search compare --update` / `search compare --force-update`. Opens a GitHub
  issue if the task fails or its logs show an index-batch failure.
- **Rebuild OpenSearch Index**
  (`.github/workflows/rebuild_opensearch_index.yml`) — manual only. Disables
  the harvester, waits for in-flight harvest tasks to drain, runs
  `flask search rebuild-index`, then re-enables the harvester unconditionally,
  even if the rebuild failed.
- **Delete OpenSearch Physical Index**
  (`.github/workflows/delete_opensearch_index.yml`) — manual only. Deletes one
  named unused physical index, e.g. one left behind by a rebuild run with
  `delete_old_index: false`.

All three run against `datagov-harvest` via `cf run-task` and share the
`opensearch-maintenance-<environment>` concurrency group, so they queue
rather than run concurrently against the same environment.
