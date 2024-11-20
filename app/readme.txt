datagov-harvester
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


examples:

curl -X POST http://{site}/organization/add -H "Content-Type: application/json" -d '
{
    "name": "New Org",
    "logo": "test url"
}'

curl -X POST http://{site}/harvest_source/add -H "Content-Type: application/json" -d '
{
    "organization_id": "4ed9d20a-7de8-4c2d-884f-86b50ec8065d",
    "name": "Example Harvest Source",
    "notification_emails": "admin@example.com",
    "frequency": "daily",
    "url": "http://example2.com",
    "schema_type": "dcatus1.1",
    "source_type": "json"
}
'

curl -X POST http://{site}/harvest_job/add -H "Content-Type: application/json" -d '
{
    "harvest_source_id": "59e93b86-83f1-4b70-afa7-c7ca027aeacb",
    "status": "in_progress"
}'

curl -X POST http://{site}/harvest_record/add -H "Content-Type: application/json" -d '
{
    "id": "identifier-1",
    "harvest_job_id": "a8c03b83-907c-41c9-95aa-d71c3be626b1",
    "harvest_source_id": "59e93b86-83f1-4b70-afa7-c7ca027aeacb"
}'

curl -X POST http://{site}/harvest_error/add -H "Content-Type: application/json" -d '
{
    "harvest_job_id": "a8c03b83-907c-41c9-95aa-d71c3be626b1",
    "harvest_record_id": "identifier-1",
    "type": "Validation Error",
    "severity": "ERROR",
    "message": "Invalid data format."
}
'

curl -X GET http://{site}/harvest_job/a8c03b83-907c-41c9-95aa-d71c3be626b1

curl -X DELETE http://{site}/organization/da183992-e598-467a-b245-a3fe8ee2fb91

curl -X DELETE http://{site}/harvest_source/ca299fd6-5553-401e-ac36-05b841e31cd1


curl -X PUT http://{site}/harvest_job/c82e0481-5884-4029-931e-234c53767e50 -H "Content-Type: application/json" -d '
{
    "status": "complete",
    "date_finished": "Wed, 27 Mar 2024 22:37:52 GMT",
    "records_added": 200,
    "records_updated": 50,
    "records_deleted": 6,
    "records_errored": 4,
    "records_ignored": 2
}'
