# Migration scripts

Migrating from our production CKAN into our new system takes lots of automated
data transformation. The scripts in this directory help with that.

## Scheduling Harvest Sources for Harvesting

Part of our QA process is to schedule a harvest job for every one of our
harvest sources. Here's some commands to pull the list of harvest sources. (We
should make a JSON form of this list page.)

```bash
$ curl https://harvest-dev.data.gov/harvest_source_list/ > harvest_source_list.html
$ grep href=\"/harvest_source/ harvest_source_list.html | sed -e s/\<a// -e s/href=\"// -e s/\"// > harvest_source_paths.txt
$ sed -e "s:^.*/harvest_source/::" < harvest_source_paths.txt > harvest_source_ids.txt
$ cf env datagov-harvest | grep HARVEST_API_TOKEN
$ export API_TOKEN=...
$ cat harvest_source_ids.txt | while read id; do \
    curl -s -H "X-API-Key: ${API_TOKEN}" \
    https://harvest-dev.data.gov/api/harvest_source/edit/$id \
    --json "{\"date_next_run\": \"$(TZ=UTC date -Iseconds -j -v +45M)\"}" ;
  done
```

This downloads the IDs for all of the harvest sources and then sets each
source's next run 45 minutes in the future. When that time passes, the
scheduler creates a job for the source and works through the queue under
the running-task cap.

## Force-Reharvesting Sources

`flask harvest_source force_reharvest_sources` (in `app/commands/source.py`)
force-reharvests every harvest source whose `schema_type` starts with a given
prefix (e.g. `dcatus`, `iso19115`). It's a Flask CLI command, not a standalone
script, so it runs inside the app itself and uses whatever database it's
already configured with, no separate DB connection setup needed.

Locally:

```bash
docker compose exec app flask harvest_source force_reharvest_sources --schema-type-prefix dcatus
```

Against a deployed environment (e.g. prod), run it as a one-off task instead
of a local script, the same way other one-off admin commands run there (see
`docs/developer.md`):

```bash
cf run-task datagov-harvest --command "flask harvest_source force_reharvest_sources --schema-type-prefix dcatus"
```

Dry-run mode is on by default. Add `--no-dry-run` to actually queue the jobs.
This only inserts `HarvestJob` rows with `status="new"` and
`job_type="force_harvest"`, one per matching source that doesn't already have
an active job; it does not start any CF tasks itself. The app's own scheduler
(`LoadManager._start_new_jobs`, which already runs on a periodic schedule)
picks up "new" jobs and starts them under the existing
`HARVEST_RUNNER_MAX_TASKS` cap, the same way it drains regularly-scheduled
harvests.
