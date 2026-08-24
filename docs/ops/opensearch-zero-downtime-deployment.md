# Zero-downtime OpenSearch deployment

A release rebuilds the index on a replacement OpenSearch cluster when the schema
the live index was built with no longer matches the schema the release declares.
This runs in development for merges to `develop`, then in staging and production
for merges to `main`.

## Deciding whether to rebuild

`search/fingerprint.py` hashes the mapping and settings that
`OpenSearchClient._ensure_index` sends to OpenSearch. Each space records the
fingerprint its index was last built with on the harvester app as
`OPENSEARCH_MAPPING_FINGERPRINT`; a release compares that recorded value against
the fingerprint of the revision being deployed and rebuilds when they differ.

Because the decision reads the state of the target space rather than replaying the
commits that reached it, it is idempotent and cannot repeat a rebuild that already
succeeded. Keys are sorted before hashing, so reordering a mapping does not force a
rebuild — only a change OpenSearch would apply to the index does.

Inspect what a space currently has recorded with:

```sh
cf env datagov-harvest | grep OPENSEARCH_MAPPING_FINGERPRINT
python -m search.fingerprint
```

The fingerprint is recorded only after a rebuild is validated and promoted, so a
failed migration leaves the previous value in place and the next release retries.

A space with nothing recorded — a new environment, or the first release after this
mechanism shipped — adopts its live index and records the fingerprint without
rebuilding.

## Rebuilding when the mapping has not changed

Changes to `search/documents.py` can alter document contents while leaving the
mapping identical, which the fingerprint cannot see. Rebuild those deliberately
with the **Rebuild OpenSearch Index** workflow, which takes a target environment
and performs the same replacement-cluster sequence.

The `force re-index recommended` label no longer triggers anything. It is a review
signal that a merge needs the rebuild workflow run against it; applying it does not
start a rebuild on its own.

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

6. Validates the replacement cluster before promotion:

   ```sh
   flask search compare --fail-on-discrepancy --max-failed-records 50
   ```

   Up to 50 missing documents are tolerated and reported in full. Any extra or
   stale document, or more than 50 missing documents, fails the release.
7. Deletes the temporary app whether the tasks succeed or fail.
8. Renames the live service to `datagov-catalog-opensearch-old`, then renames the
   replacement service to `datagov-catalog-opensearch`.
9. Records the promoted schema fingerprint on `datagov-harvest`, so later releases
   know this rebuild is done.
10. Performs a blocking rolling deployment of the canonical harvester and restarts
   catalog. If catalog already has a deployment in progress, that deployment is
   allowed to complete instead.
11. Deletes the old cluster only when no harvest tasks or catalog deployment may
   still be using it. Otherwise the release retains it and creates an O&M issue.
12. Applies network policies and restores scheduled harvesting to three tasks.

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

Any failed main release disables `deploy.yml`, and any failed development release
disables `deploy-development.yml`. After investigating and recovering the failed
release, re-enable the appropriate workflow:

```sh
gh workflow enable deploy.yml --repo GSA/datagov-harvester
gh workflow enable deploy-development.yml --repo GSA/datagov-harvester
```

The temporary app is always deleted. Replacement or retired clusters are retained
so the operator can inspect or recover them. A later rebuild refuses to provision
while either `datagov-catalog-opensearch-next` or
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
