DATAGOV-HARVESTING-LOGIC
├── app/
│   ├── __init__.py
│   ├── models.py
│   ├── interface.py
│   ├── routes.py
│   ├── forms.py
│   └── templates/
│       ├── index.html
│       ├── source_form.html
│       ├── org_form.html
│       └── harvest_source.html
│   └── static/
│       └── styles.css (to-do)
│
├── migrations/
│   └── versions/
│   ├── alembic.ini
│   ├── env.py
│   └── script.py.mako
│
├── tests/
│
├── docker-compose.yml
├── Dockerfile
├── .profile
├── requirements.txt
└── run.py



curl -X POST http://localhost:8080/organization/add -H "Content-Type: application/json" -d '
{
    "name": "New Org",
    "logo": "test url"
}'

curl -X POST http://localhost:8080/harvest_source/add -H "Content-Type: application/json" -d '
{
    "organization_id": "c32e18ee-a854-47be-a05f-42516498a44d",
    "name": "Example Harvest Source",
    "notification_emails": "admin@example.com",
    "frequency": "daily",
    "url": "http://example2.com",
    "schema_type": "strict",
    "source_type": "json"
}
'

curl -X POST http://localhost:8080/harvest_job/add -H "Content-Type: application/json" -d '
{
    "harvest_source_id": "760129d6-2bf0-4c94-94b9-09622a8a0b23",
    "status": "in_progress",
    "date_created": "Wed, 27 Mar 2024 20:37:52 GMT"
}'


curl -X POST http://localhost:8080/harvest_error/add -H "Content-Type: application/json" -d '
{
    "harvest_job_id": "aac30640-bd76-46c2-8a64-cf8ee389b190",
    "harvest_record_id": "record123", 
    "date_created": "Wed, 27 Mar 2024 20:37:52 GMT",
    "type": "Validation Error",
    "severity": "ERROR",
    "message": "Invalid data format."
}
'


curl -X DELETE http://localhost:8080/organization/da183992-e598-467a-b245-a3fe8ee2fb91

curl -X DELETE http://localhost:8080/harvest_source/c7abedad-4420-4d71-b519-3284f9a9a132


curl -X PUT http://localhost:8080/harvest_job/c82e0481-5884-4029-931e-234c53767e50 -H "Content-Type: application/json" -d '
{
    "status": "complete",
    "date_finished": "Wed, 27 Mar 2024 22:37:52 GMT",
    "records_added": 200,
    "records_updated": 50,
    "records_deleted": 6,
    "records_errored": 4,
    "records_ignored": 2
}'

-------------

curl -X POST https://harvester-dev-datagov.app.cloud.gov/organization/add -H "Content-Type: application/json" -d '
{
    "name": "New Org 1",
    "logo": "test url for new org1"
}'


curl -X POST https://lharvester-dev-datagov.app.cloud.gov/harvest_job/add -H "Content-Type: application/json" -d '
{
    "harvest_source_id": "760129d6-2bf0-4c94-94b9-09622a8a0b23",
    "status": "in_progress",
    "date_created": "Wed, 27 Mar 2024 20:37:52 GMT"
}'

curl -X PUT https://harvester-dev-datagov.app.cloud.gov/harvest_job/add -H "Content-Type: application/json" -d '
{
    "status": "complete",
    "date_finished": "Wed, 27 Mar 2024 22:37:52 GMT",
    "records_added": 200,
    "records_updated": 50,
    "records_deleted": 6,
    "records_errored": 4,
    "records_ignored": 2
}'


curl -X PUT https://harvester-dev-datagov.app.cloud.gov/organization/4c456ed3-4717-4933-82c9-d87464063f19 -H "Content-Type: application/json" -d '
{
    "logo": "url for test 1"
}'