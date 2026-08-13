# Zero-downtime OpenSearch deployment

Pull requests carrying the `force re-index recommended` label use a replacement
OpenSearch cluster when they are merged. This runs in development for merges to
`develop`, then in staging and production for merges to `main`.

The label detector scans from the last successful release rather than inspecting
only the current push. This preserves the migration request when GitHub replaces a
pending release with a newer merge.

## Release sequence

For each Cloud Foundry space, the release:

1. Sets `HARVEST_RUNNER_MAX_TASKS` to `0` and rolling-restarts
   `datagov-harvest`.
2. Creates `datagov-catalog-opensearch-next` with the normal plan for the space.
3. Pushes `datagov-harvest-next` with `cf push --task`. The temporary app uses the
   canonical harvester database, secrets, and SMTP services, but it binds the
   replacement OpenSearch service.
4. Binds the replacement service to catalog and verifies that both bindings
   contain a real OpenSearch host.
5. Runs:

   ```sh
   flask db upgrade && flask search compare --force-update
   ```

6. Deletes the temporary app whether the task succeeds or fails.
7. Renames the live service to `datagov-catalog-opensearch-old`, then renames the
   replacement service to `datagov-catalog-opensearch`.
8. Performs a blocking rolling deployment of the canonical harvester and restarts
   catalog. If catalog already has a deployment in progress, that deployment is
   allowed to complete instead.
9. Deletes the old cluster only when no harvest tasks or catalog deployment may
   still be using it. Otherwise the release retains it and creates an O&M issue.
10. Applies network policies and restores scheduled harvesting to three tasks.

Existing harvest tasks are not drained or canceled. They may finish against the
old cluster; the nightly OpenSearch sync repairs any records they changed after the
replacement was populated.

Database migrations run before the canonical application deployment. They must
remain backward-compatible with the old application instances, which is also a
requirement of the existing rolling deployment process.

## Failure behavior

Any migration failure stops later steps and creates or updates an environment-
specific issue. Scheduled harvesting remains disabled until an operator reviews
the state and runs the **Toggle Harvester** workflow to enable it.

The temporary app is always deleted. Replacement or retired clusters are retained
so the operator can inspect or recover them. A later labeled release refuses to
provision while either `datagov-catalog-opensearch-next` or
`datagov-catalog-opensearch-old` remains.

Inspect the space before recovery:

```sh
cf services
cf apps
cf tasks datagov-harvest
```

When an old cluster was intentionally retained for running tasks, retry its guarded
cleanup after those tasks and any catalog deployment finish:

```sh
bin/cleanup_opensearch_cluster.sh datagov-catalog-opensearch-old
```

If the service-name swap failed after moving the live service to `-old`, restore
the canonical name before restarting applications:

```sh
cf rename-service datagov-catalog-opensearch-old datagov-catalog-opensearch
```
