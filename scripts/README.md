# Migration scripts

Migrating from our production CKAN into our new system takes lots of automated
data transformation. The scripts in this directory help with that.

## Scheduling Harvest Sources for Harvesting

Part of our QA process is to schedule a harvest job for every one of our
harvest sources. Here's some commands to pull the list of harvest sources. (We
should make a JSON form of this list page.)

```bash
$ curl https://datagov-harvest-dev.app.cloud.gov/harvest_source_list/ > harvest_source_list.html
$ grep href=\"/harvest_source/ harvest_source_list.html | sed -e s/\<a// -e s/href=\"// -e s/\"// > harvest_source_paths.txt
$ sed -e "s:^.*/harvest_source/::" < harvest_source_paths.txt > harvest_source_ids.txt
$ cf env datagov-harvest | grep FLASK_APP_SECRET_KEY
$ export API_TOKEN=...
$ cat harvest_source_ids.txt | while read id; do \
    curl -s -H "Authorization: ${API_TOKEN}" \
    https://datagov-harvest-dev.app.cloud.gov/harvest_job/add \
    --json "{\"harvest_source_id\": \"$id\", \"status\": \"new\", \"date_created\": \"$(TZ=UTC date -Iseconds -j -v +45M)\"}" ;
  done
```

This downloads the IDs for all of the harvest sources and then creates a new
job for each source by ID at 45 minutes in the future. When that time in the
near future passes, the harvest jobs will begin being worked through.

