import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from app.models import Base 
from app.interface import HarvesterDBInterface
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

    with engine.begin() as connection:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE;"))

    engine.dispose()

@pytest.fixture(scope='session')
def db_interface(db_session):
    return HarvesterDBInterface(db_session)

def test_add_harvest_source(db_interface):
    org_data = db_interface.add_organization({'name': 'GSA',
                'logo': 'url for the logo'})
    source_data = {'name': 'Test Source',
                   'frequency': 'daily',
                   'url': "http://example-1.com",
                   'schema_type': 'strict',
                   'source_type': 'json'}
    new_source = db_interface.add_harvest_source(source_data, str(org_data.id))
    assert new_source.name == 'Test Source'

def test_add_and_get_harvest_source(db_interface):
    org_data = db_interface.add_organization({'name': 'GSA',
            'logo': 'url for the logo'})
    new_source = db_interface.add_harvest_source({
            'name': 'Test Source',
            'notification_emails': ['test@example.com'],
            'frequency': 'daily',
            'url': "http://example-2.com",
            'schema_type': 'strict',
            'source_type': 'json'
    }, str(org_data.id))
    assert new_source.name == 'Test Source' 
   
    sources = db_interface.get_all_harvest_sources()
    assert any(source['name'] == 'Test Source' for source in sources)


def test_add_harvest_job(db_interface):
    org_data = db_interface.add_organization({'name': 'GSA',
                'logo': 'url for the logo'})
    new_source = db_interface.add_harvest_source({
            'name': 'Test Source',
            'notification_emails': ['test@example.com'],
            'frequency': 'daily',
            'url': "http://example-3.com",
            'schema_type': 'strict',
            'source_type': 'json'
    }, str(org_data.id))
        
    job_data = {
        'status': 'in_progress',
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
