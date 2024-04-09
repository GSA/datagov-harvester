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
    "organization_id": "4ed9d20a-7de8-4c2d-884f-86b50ec8065d",
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
    "harvest_source_id": "59e93b86-83f1-4b70-afa7-c7ca027aeacb",
    "status": "in_progress"
}'

curl -X POST http://localhost:8080/harvest_record/add -H "Content-Type: application/json" -d '
{
    "id": "identifier-1",
    "harvest_job_id": "a8c03b83-907c-41c9-95aa-d71c3be626b1",
    "harvest_source_id": "59e93b86-83f1-4b70-afa7-c7ca027aeacb"
}'

curl -X POST http://localhost:8080/harvest_error/add -H "Content-Type: application/json" -d '
{
    "harvest_job_id": "a8c03b83-907c-41c9-95aa-d71c3be626b1",
    "harvest_record_id": "identifier-1",
    "type": "Validation Error",
    "severity": "ERROR",
    "message": "Invalid data format."
}
'

curl -X GET http://localhost:8080/harvest_job/a8c03b83-907c-41c9-95aa-d71c3be626b1




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

curl -X PUT https://harvester-dev-datagov.app.cloud.gov/harvest_job/<job_id> -H "Content-Type: application/json" -d '
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


curl -X DELETE https://harvester-dev-datagov.app.cloud.gov/organization/e1301d69-d747-4040-9e31-bba7c9508fb9


curl -X DELETE https://harvester-dev-datagov.app.cloud.gov/organization/4c456ed3-4717-4933-82c9-d87464063f19