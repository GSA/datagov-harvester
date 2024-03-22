from datetime import datetime

new_org = {"name": "GSA", "logo": "url for the logo"}

new_source = {
    "name": "Example Harvest Source",
    "notification_emails": ["admin@example.com"],
    "frequency": "daily",
    "url": "http://example.com",
    "schema_type": "strict",
    "source_type": "json",
}

new_job = {
    "status": "in_progress",
    "date_created": datetime.utcnow(),
    "date_finished": datetime.utcnow(),
    "records_added": 100,
    "records_updated": 20,
    "records_deleted": 5,
    "records_errored": 3,
    "records_ignored": 1,
}

new_error = {
    "harvest_record_id": "record123",
    "date_created": datetime.utcnow(),
    "type": "Validation Error",
    "severity": "ERROR",
    "message": "Invalid data format.",
}
