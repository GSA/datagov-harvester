# Operator guide: Rebuild OpenSearch Index

How to run the zero-downtime OpenSearch dataset index rebuild from GitHub
Actions.

Design details, validation rules, and failure modes live in
[docs/opensearch-index-rebuild.md](../opensearch-index-rebuild.md).

## What the workflow does

In short:

1. Saves the current `HARVEST_RUNNER_MAX_TASKS` value.
2. Sets `HARVEST_RUNNER_MAX_TASKS=0` and rolling-restarts the harvester.
3. Waits for active `harvest-job-*` Cloud Foundry tasks to drain.
4. Builds and validates a versioned physical index from PostgreSQL.
5. Optionally switches the logical `datasets` alias to that physical index.
6. Restores the saved max-tasks value (for example `2`, `3`, or `4`) and
   rolling-restarts the harvester again.

Search traffic keeps using the current `datasets` target while the candidate is
built. Harvest jobs already queued as `new` stay in the database and can start
again after capacity is restored.

## Prerequisites

- Deploy the harvester version that includes the rebuild workflow and
  `HARVEST_RUNNER_MAX_TASKS=0` scheduling gates.
- Prefer an OpenSearch snapshot before the **first** alias conversion in an
  environment, because that cutover removes the legacy concrete `datasets`
  index.
- Do not run overlapping OpenSearch maintenance or harvester restart workflows
  for the same environment; concurrency already queues them.

## Compatible rebuild (normal path)

Use this when the new mapping is backward-compatible with the running Catalog
app (no renamed/removed fields that Catalog still queries).

1. Open **Actions → Rebuild OpenSearch Index**.
2. Choose `development`, `staging`, or `prod`.
3. Leave **Switch the datasets alias to the rebuilt index** enabled.
4. Optionally enable **Cancel harvest jobs still running after 15 minutes** if
   you cannot wait for a long drain.
5. Run the workflow and watch:
   - capacity set to `0` + rolling restart
   - harvest-task drain
   - candidate create / backfill / validate
   - alias switch
   - capacity restore + rolling restart
6. Verify:
   - workflow succeeded
   - `cf env datagov-harvest` shows the original `HARVEST_RUNNER_MAX_TASKS`
   - `datasets` points at the new physical index
   - catalog search and harvest processing look healthy

Physical index names look like:

```text
datasets-<github-run-id>-<github-run-attempt>
```

## Breaking schema change (no immediate alias switch)

Use this when the index mapping is incompatible with the currently deployed
Catalog — for example renaming a field, removing a field Catalog still reads,
or otherwise changing document shape in a breaking way.

Switching the alias during the rebuild would cut Catalog over to the new schema
before Catalog understands it. Instead:

1. Open **Actions → Rebuild OpenSearch Index**.
2. Choose the target environment.
3. **Disable** **Switch the datasets alias to the rebuilt index**.
4. Run the workflow. It still:
   - sets max tasks to `0` and restarts
   - drains active harvest tasks
   - builds and validates the physical index
   - restores max tasks and restarts
5. Copy the physical index name from the workflow logs (for example
   `datasets-29762881914-1`). The live `datasets` alias is unchanged.
6. Deploy Catalog hardcoded to that physical index name (and deploy Harvester
   with matching writer/schema changes if needed).
7. Confirm Catalog is healthy against the new physical index.
8. Point the `datasets` alias at that physical index so remaining consumers can
   use the logical name again.
9. After that alias cutover is verified, redeploy Catalog/Harvester back to the
   `datasets` alias if you temporarily hardcoded the physical name.

Keep writers and readers coordinated. If harvesting resumes while Catalog still
reads the old alias target and Harvester writes the old schema, the new
candidate can drift or Catalog can see mixed results.

### Alias cutover after a build-only rebuild

Alias switching during rebuild is already implemented:

- Workflow input → `--switch-alias` / `--no-switch-alias`
- Flask command: `flask search rebuild-index ... --switch-alias`
- Library method: `OpenSearchInterface.switch_alias(target_index)`

There is **no** separate operator workflow or Flask command today that only
points `datasets` at an already-built physical index. Do not re-run the full
rebuild just to flip the alias; that creates another candidate and backfills
again. If you need a post-deploy cutover, add a thin CLI around
`switch_alias()` or run the equivalent OpenSearch `update_aliases` request
manually.

## Aftercare: delete the previous physical index

Rebuilds keep the previous physical index for rollback. After you are confident
the new index is healthy, delete the old one with
**Actions → Delete OpenSearch Physical Index**.

That workflow refuses to delete:

- the index currently targeted by the `datasets` alias
- anything that is not a physical `datasets-*` name

Do not leave obsolete indexes around longer than needed; they consume cluster
storage.

## Manual recovery if capacity restore fails

If the workflow is canceled mid-run or restore fails, capacity may remain at
`0` (safe mode: search still works, harvests do not start).

```shell
cf env datagov-harvest
cf set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS <environment-value>
cf restart datagov-harvest --strategy rolling
```

Use the value from `vars.<environment>.yml` for that space. Only restore
capacity after confirming no rebuild task is still running.

## Related workflows and scripts

| Piece | Role |
| --- | --- |
| **Actions → Rebuild OpenSearch Index** | Main operator entrypoint |
| **Actions → Delete OpenSearch Physical Index** | Remove retained old indexes |
| **Actions → Synchronize OpenSearch Index** | Separate sync maintenance; shares concurrency |
| `bin/manage_harvest_runner_capacity.sh` | Save/set/restore max tasks + verified restart |
| `bin/wait_for_harvest_tasks.sh` | Drain active harvest CF tasks |
| `flask search rebuild-index` | Create, backfill, validate, optional alias switch |
| `scripts/opensearch_status.sh` | Inspect whether `datasets` is alias or concrete |
