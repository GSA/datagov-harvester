# CKAN to Harvester migration

These scripts are used in the migration of data from our CKAN-based system
into the new Harvester/CKAN-catalog system.

## migrate_harvest_sources.py

This script queries the existing CKAN production API to get information on all
of the existing harvest sources and then moves them over to the Harvester and
the new CKAN catalog together using their APIs. 

The Harvester and the new CKAN catalog need API keys to write information into
those systems. The API key for Harvester is available in `cf env ...` and for
the new CKAN catalog, an API key needs to be created in the web UI for a user
with admin permissions.  The API keys are provided as command line arguments
to the script with `--harvester-api-token` and `--new-catalog-api-token`.

The URLs for the harvester and new catalog instances to be migrated into can
be set with command line arguments so that the same script could be used to move
data into dev, and then staging, and then finally into production Harvesters.

The script uses threads to parallelize the many API calls, but if that isn't
desired, the number of thread workers can be set with `--workers=1` which will
give serial rather than parallel behavior.

Example usage could be:

```bash
$ export API_TOKEN=...
$ export CKAN_API_TOKEN=...
$ poetry run scripts/ckan_migration/migrate_harvest_sources.py \
    --harvester-api-token=$API_TOKEN \
    --new-catalog-api-token=$CKAN_API_TOKEN \
    --harvester-url=https://datagov-harvest-admin-dev.app.cloud.gov/ \
    --new-catalog-url=https://catalog-next-dev-datagov.app.cloud.gov/
```
## Post migration
On the development and staging environments, we want to prevent harvest reports from
being emailed to the recipients specified in the harvester's notification_emails.
The following SQL script modifies all email addresses by appending `.local`, making
them invalid and ensuring that no emails are sent.
```sql
UPDATE harvest_source
SET notification_emails = (
    SELECT array_agg(
        CASE
            WHEN email LIKE '%.local' THEN email
            WHEN email LIKE '%@gsa.gov%' THEN email
            ELSE email || '.local'
        END
    )
    FROM unnest(notification_emails) AS email
);

```
