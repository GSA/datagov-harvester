import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from harvester.database.models import Base 
from harvester.database.interface import HarvesterDBInterface
from dotenv import load_dotenv
import os

load_dotenv()

@pytest.fixture(scope='session')
def db_session():
    DATABASE_SERVER = os.getenv("DATABASE_SERVER")
    DATABASE_URI = os.getenv("DATABASE_URI")
    TEST_SCHEMA = "test_schema"
    modified_uri = DATABASE_URI.replace('@' + DATABASE_SERVER, '@localhost')
    engine = create_engine(modified_uri)

    with engine.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA};"))
        connection.execute(text(f"SET search_path TO {TEST_SCHEMA};"))

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    session = scoped_session(SessionLocal)
    yield session

    session.remove()
    engine.dispose()

    with engine.begin() as connection:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE;"))

@pytest.fixture(scope='session')
def db_interface(db_session):
    return HarvesterDBInterface(db_session)

def test_add_harvest_source(db_interface):
    source_data = {'name': 'Test Source',
                   'organization_id': 'Test Org',
                   'frequency': 'daily',
                   'url': "http://example.com",
                   'schema_type': 'strict',
                   'source_type': 'json'}
    new_source = db_interface.add_harvest_source(source_data)
    assert new_source.name == 'Test Source'

def test_add_and_get_harvest_source(db_interface):
    new_source = db_interface.add_harvest_source({
            'name': 'Test Source',
            'notification_emails': ['test@example.com'],
            'organization_id': 'Test Org',
            'frequency': 'daily',
            'url': "http://example.com",
            'schema_type': 'strict',
            'source_type': 'json'
    })
    assert new_source.name == 'Test Source' 
   
    sources = db_interface.get_all_harvest_sources()
    assert any(source['name'] == 'Test Source' for source in sources)


def test_add_harvest_job(db_interface):
    new_source = db_interface.add_harvest_source({
            'name': 'Test Source',
            'notification_emails': ['test@example.com'],
            'organization_id': 'Test Org',
            'frequency': 'daily',
            'url': "http://example.com",
            'schema_type': 'strict',
            'source_type': 'json'
        })
        
    job_data = {
        'date_created': '2022-01-01',
        'date_finished': '2022-01-02',
        'records_added': 10,
        'records_updated': 5,
        'records_deleted': 2,
        'records_errored': 1,
        'records_ignored': 0
    }
    new_job = db_interface.add_harvest_job(job_data, str(new_source.id))
    assert new_job.harvest_source_id == new_source.id

def test_add_harvest_error(db_interface):
    new_source = db_interface.add_harvest_source({
        'name': 'Error Test Source',
        'notification_emails': ['error@example.com'],
        'organization_id': 'Error Org',
        'frequency': 'weekly',
        'url': "http://example.com",
        'schema_type': 'strict',
        'source_type': 'json'
    })

    new_job = db_interface.add_harvest_job({
        'date_created': '2022-01-03',
        'date_finished': '2022-01-04',
        'records_added': 5,
        'records_updated': 3,
        'records_deleted': 1,
        'records_errored': 0,
        'records_ignored': 2
    }, str(new_source.id))

    error_data = {
        'harvest_job_id': str(new_job.id),
        'record_reported_id': 'test_record',
        'date_created': '2022-01-04',
        'type': 'Test Error',
        'severity': 'high',
        'message': 'This is a test error'
    }
    new_error = db_interface.add_harvest_error(error_data, str(new_job.id))

    assert new_error.harvest_job_id == new_job.id
    assert new_error.type == 'Test Error'
    assert new_error.message == 'This is a test error'
