import pytest
import os
import dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
from harvester.db.models import Base

dotenv.load_dotenv()


@pytest.fixture(scope="session")
def engine():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")

    engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="session")
def session(engine, tables):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def test_harvest_source():
    return {
        "id": "efecc0d6-9dac-4916-8356-150a683e9864",
        "name": "test_harvest_source",
        "notification_emails": ["something@example.com", "another@test.com"],
        "organization_name": "example",
        "frequency": "weekly",
        "config": {"test": 1, "example": True, "something": ["a", "b"]},
        "urls": [
            "https://path_to_resource/example.json",
            "https://path_to_resource/another_example.json",
        ],
        "schema_validation_type": "dcatus",
    }


@pytest.fixture
def test_harvest_job():
    return {
        "id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "source_id": "efecc0d6-9dac-4916-8356-150a683e9864",
        "status": "INACTIVE",
    }


@pytest.fixture
def test_harvest_record():
    return {
        "id": "a2a9f93d-b91e-4df6-bb71-00fca7b519a1",
        "job_id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "source_id": "efecc0d6-9dac-4916-8356-150a683e9864",
        "status": "ACTIVE",
        "s3_path": "https://bucket-name.s3.region-code.amazonaws.com/key-name",
    }


@pytest.fixture
def test_harvest_error():
    return {
        "id": "860f5605-66a0-4d8c-beb4-989b2e18d295",
        "job_id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "record_id": "b5195975-03db-4cf8-b373-52035f0f428f",
        "record_reported_id": "example",
        "severity": "CRITICAL",
    }


@pytest.fixture
def test_update_source():
    return {
        "id": "efecc0d6-9dac-4916-8356-150a683e9864",
        "urls": ["http://another_path/data.json"],
    }


@pytest.fixture
def test_update_job():
    return {
        "id": "c8837a2f-bd0b-4333-82f5-0b64f126da34",
        "status": "ACTIVE",
        "extract_started": datetime.utcnow(),
    }


@pytest.fixture
def test_update_record():
    return {"id": "a2a9f93d-b91e-4df6-bb71-00fca7b519a1", "status": "INVALID"}
