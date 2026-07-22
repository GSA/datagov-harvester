# Operations notes

This is miscellaneous notes on operating the Harvester system.

## Enabling or disabling harvesting

Run the **Toggle Harvester** GitHub Actions workflow and select the target
environment and desired state. The workflow sets `HARVEST_RUNNER_ENABLED` to
`false` when disabling or `true` when enabling. It preserves
`HARVEST_RUNNER_MAX_TASKS` and every other credential in the environment's
`datagov-harvest-secrets` service, then rolling-restarts `datagov-harvest`.

Set `HARVEST_RUNNER_MAX_TASKS` in each environment's secrets service to its
desired positive capacity. It defaults to `3` when absent.
`HARVEST_RUNNER_ENABLED` defaults to `true`. The effective capacity is the
configured maximum when enabled and `0` when disabled.

The workflow completes only after all previous application instance GUIDs have
been replaced, every desired instance is running, and every new instance has
logged the expected setting. The same startup logs can be checked manually:

```bash
cf logs datagov-harvest --recent | grep 'Harvester startup: HARVEST_RUNNER_MAX_TASKS='
```

Both values are read from the bound secrets service when the application
starts. Deployments do not set them, so a merge to `main` does not change the
configured capacity or re-enable a disabled environment.

Disabling prevents scheduled and manually requested harvest tasks from
starting. Tasks that were already running are not stopped.

## Scheduling many harvest jobs

Using the API's `/harvest_source/harvest/<id>/<type>` endpoint circumvents the
job manager's limits on how many running jobs are allowed and calling it many\
times can result in overloading the memory quota for our cloud.gov
organization and the jobs will not actually be started even though the API
call succeeds. In order to kick off harvest jobs that will run under the
cap for the number of running tasks, we need to use the `/harvest_job/add`
method to add a job with the `new` status and a `date_created` time in the
future.

On a Mac, this command will schedule a harvest source to be harvested at a
specified amount of time in the future (see the `-v` option to the Mac OS X
`date` command):

```bash
$ export API_TOKEN=...
$ export id=.....  # harvest source id
$ curl -s -H "X-API-Key: ${API_TOKEN}" \
  https://datagov-harvest-admin-dev.app.cloud.gov/harvest_job/add \
  --json "{\"harvest_source_id\": \"$id\", \
           \"status\": \"new\", \
           \"date_created\": \"$(date -Iseconds -j -v +45M)\"}"
```

Looping over a file with harvest source IDs, this method can be used to
start harvests for a large number of sources, but subject to the running
tasks limit necessitated by our limited memory quota.
