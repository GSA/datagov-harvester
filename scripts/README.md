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
