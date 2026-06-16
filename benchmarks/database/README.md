sources by harvest records (large, medium, small)
```terminal
          harvest_source_id           | successful_historical_rows | successful_identifiers 
--------------------------------------+----------------------------+------------------------
 35d79097-243c-414d-b67e-d8484397abc6 |                     848493 |                  71850
 b1bd9ece-a304-434d-80e3-1800ab1742db |                      94915 |                  13582
 a73d251c-f44e-4f76-8bf3-d8d45876adb2 |                       2723 |                   1355
```

jobs by record errors (large, medium, small)
```terminal
                  id                  | record_error_count 
--------------------------------------+--------------------
 02c5d53a-dc97-4597-b854-cc07203506da |              32309
 12f6b6c7-321a-4d53-8d7f-ebf5d4f67ef9 |               9285
 3d9abdfa-2ba1-4948-8ee9-a454797c28cd |               1003
```

datasets by harvest source (large, medium, small)
```terminal
          harvest_source_id           | dataset_count 
--------------------------------------+---------------
 9996b8ab-2032-4fd4-acfd-2702991ae5ef |        106885
 bebdce30-696c-424b-ad16-eca2913bde29 |         19591
 30facec2-c2a5-4037-90be-f9242db6f6f3 |          1550
```

./run_pgbench.sh \
  after \
  latest-small \
  "Latest successful records by source" \
  "Small source" \
  queries/latest_records_by_source.sql.template \
  "$SMALL_SOURCE_ID"


./run_pgbench.sh \
  after \
  record-error-job-medium \
  "Harvest record errors by job" \
  "Medium record errors" \
  queries/record_errors_by_job.sql.template \
  "$MEDIUM_SOURCE_ID"

./run_pgbench.sh \
  after \
  record-error-job-small \
  "Harvest record errors by job" \
  "Small record errors" \
  queries/record_errors_by_job.sql.template \
  "$SMALL_SOURCE_ID"
