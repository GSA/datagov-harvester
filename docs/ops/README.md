# Operations notes

This is miscellaneous notes on operating the Harvester system.

## Enabling or disabling harvesting

Run the **Toggle Harvester** GitHub Actions workflow and select the target
environment and desired state. The workflow sets `HARVEST_RUNNER_MAX_TASKS` to
`0` when disabling or to the optional maximum-tasks input when enabling with
`cf set-env`, then rolling-restarts `datagov-harvest`. The input defaults to
production's previous value of `3` and is ignored when disabling. The workflow
does not read or update the application's bound secrets service.

The workflow uses the blocking form of `cf restart --strategy rolling` (without
`--no-wait`), so it completes only after Cloud Foundry reports that the rolling
deployment has finished and its replacement instances are healthy.

`HARVEST_RUNNER_MAX_TASKS` is intentionally omitted from the manifest and vars
files, so Cloud Foundry's additive manifest behavior preserves its current value
across deployments. If the variable or application does not exist, it defaults
to `3`, enabling harvesting.

Disabling prevents scheduled harvest tasks from starting. Tasks that were
already running are not stopped. Manual triggers do not use the scheduler's
task-limit setting.

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
