from datetime import datetime

new_source = {
    'name': 'Example Harvest Source',
    'notification_emails': ['admin@example.com'],
    'organization_name': 'Example Organization',
    'frequency': 'daily',
    'url': "http://example.com",
    'schema_type': 'strict',
    'source_type': 'json',
    'harvest_source_id': '8d15bfa3-2b48-4166-bce0-631a1e336ae4',
    'harvest_source_name': 'source name from ckan'
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