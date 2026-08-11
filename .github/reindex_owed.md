---
title: OpenSearch reindex still owed ({{ env.RELEASE_PATH }})
labels: ["bug", "o&m"]
---

A merged PR carried `force re-index recommended`, so the `datasets` index needed
rebuilding — but this run did not finish one in every environment on this path.

**Until the index is rebuilt, search results reflect the OLD mapping/document shape.**
Nothing else detects this: `MAPPINGS` carries no version, and `_ensure_index()` only
creates an index when one is absent, so the deployed schema change is a silent no-op
against the existing index.

| | |
| --- | --- |
| Release path | {{ env.RELEASE_PATH }} |
| Labelled PR(s) | {{ env.PR_NUMBERS }} |
| Per-space results | {{ env.SPACE_RESULTS }} |
| Commit deployed | {{ env.LAST_COMMIT }} |

Last observed: {{ date | date('YYYY-MM-DD HH:mm:ss Z') }}
GitHub Action Run: https://github.com/{{ env.REPO }}/actions/runs/{{ env.RUN_ID }}

## What to do

1. Read the run above to find which stage failed and why.
2. Check for a leftover replacement cluster in the affected space — provisioning
   deliberately refuses to run over one:
   ```
   cf target -s <space> && cf services | grep opensearch
   ```
3. Resume rather than restarting from scratch. If `datagov-catalog-opensearch-next`
   exists and is bound, dispatch **Migrate OpenSearch Cluster** with
   `start_at: rebuild`; provisioning an `es-large` again would cost hours. If it does
   not exist, `start_at: provision`.
4. Or delete the leftover and let the next labelled merge redo it:
   ```
   bin/delete_opensearch_cluster.sh datagov-catalog-opensearch-next datagov-harvest datagov-catalog
   ```

The next successful run of **{{ env.WORKFLOW_NAME }}** re-examines every commit since
the last successful one, so this obligation is still detectable automatically — this
issue exists so a human sees it too. Close it once the index has been rebuilt.

The title carries the release path deliberately: `update_existing: true` matches on
title, so without it a `develop` failure and a `main` failure would share one issue and
overwrite each other.

See [docs/ops/migrate-opensearch-cluster.md](https://github.com/{{ env.REPO }}/blob/main/docs/ops/migrate-opensearch-cluster.md).
