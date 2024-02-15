from datetime import datetime

new_source = {
    'name': 'Example Harvest Source',
    'notification_emails': ['admin@example.com'],
    'organization_name': 'Example Organization',
    'frequency': 'daily',
    'config': '{"url": "http://example.com", "schema_validation_type": "strict"}'
}

new_job = {
    "date_created": datetime.utcnow(),
    "date_finished": datetime.utcnow(),
    "records_added": 100,
    "records_updated": 20,
    "records_deleted": 5,
    "records_errored": 3,
    "records_ignored": 1
}

new_error = {
    "record_id": "record123", 
    "record_reported_id": "recordXYZ",
    "date_created": datetime.utcnow(),
    "type": "Validation Error",
    "severity": "ERROR",
    "message": "Invalid data format."
}