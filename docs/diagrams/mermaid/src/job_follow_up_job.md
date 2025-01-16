```mermaid
---
title: Harvest Job - Follow Up Job
---
sequenceDiagram
    autonumber
    participant FA as Flask App
    participant HDB as Harvest DB
    participant DHR as Datagov Harvest Runner
    participant MD as MDTranslator
    participant HS as Agency<br>Harvest Source
    participant CKAN
    participant SES
    note over FA: TRIGGER <br> via GH Action,<br>or manually via Flask app
    FA->>+HDB: create harvest_job<br>(type: follow_up)
    HDB-->>-FA: returns harvest_job obj
    FA->>+DHR: invoke harvest.py<br> with corresponding harvest_source config & <<job_id>>
    DHR-->>-FA: returns OK
    FA->>HDB: update job_status: in_progress
    note over DHR: EXTRACT
    DHR->>+HDB: Fetch harvest records<br>from most recent harvest job
    HDB->>-DHR: return harvest records
    DHR->>DHR: Hydrate new Harvest Source object<br>with harvest record objects
    note over DHR: COMPARE<br>(SKIPPED)
    note over DHR: TRANSFORM<br>(SKIPPED)
    note over DHR: VALIDATE<br>(SKIPPED)
    note over DHR: LOAD
    loop SYNC items to create/update/delete
        DHR->>CKAN: CKAN package_create (create), <br>package_update (update), <br>dataset_purge (delete)<br>*ignore records with ckan_id as they are already synced
        alt Sync fails
            DHR-->>HDB: Log failures as harvest_error with type: sync<br>UPDATE harvest_record to status: error_sync
        end
    end
    note over DHR: REPORT
    DHR->>HDB: POST harvest job metrics <br> UPDATE harvest_job to status: complete<br>NOTE: we subtract previous job's results unlike restart
    DHR->>SES: Email job metrics (jobMetrics, notification_emails)
```
