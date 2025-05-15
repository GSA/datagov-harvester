# Operations notes

This is miscellaneous notes on operating the Harvester system.

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
$ curl -s -H "Authorization: ${API_TOKEN}" \
  https://datagov-harvest-admin-dev.app.cloud.gov/harvest_job/add \
  --json "{\"harvest_source_id\": \"$id\", \
           \"status\": \"new\", \
           \"date_created\": \"$(date -Iseconds -j -v +45M)\"}"
```

Looping over a file with harvest source IDs, this method can be used to
start harvests for a large number of sources, but subject to the running
tasks limit necessitated by our limited memory quota.
