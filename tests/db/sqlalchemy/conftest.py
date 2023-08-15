import pytest 

@pytest.fixture 
def create_test_harvest_source():
    return {
        "id": "7b295ba9-137e-4d0d-8295-3abac2849a0e",
        "name": "test_harvest_source",
        "notification_emails": [ "something@example.com", "another@test.com" ],
        "organization_name": "example",
        "frequency": "weekly",
        "config": { "test": 1, "example": True, "something": [ "a", "b" ] },
        "urls": [ "https://path_to_resource/example.json", "https://path_to_resource/another_example.json"],
        "schema_validation_type": "dcatus"
    }

@pytest.fixture 
def create_test_harvest_job():
    return {
        "id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "source_id": "7b295ba9-137e-4d0d-8295-3abac2849a0e",
        "status": "INACTIVE",
    }

@pytest.fixture 
def create_test_harvest_record():
    return {
        "job_id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "source_id": "7b295ba9-137e-4d0d-8295-3abac2849a0e",
        "status": "ACTIVE",
        "s3_path": "https://bucket-name.s3.region-code.amazonaws.com/key-name"
    }

@pytest.fixture
def create_test_harvest_error():
    return {
        "job_id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "record_id": "b5195975-03db-4cf8-b373-52035f0f428f",
        "record_reported_id": "example",
        "severity": "CRITICAL"
    }