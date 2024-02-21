import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base 
from harvester.database.interface import HarvesterDBInterface

@pytest.fixture(scope='function')
def session():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(engine)
    
@pytest.fixture(scope='function')
def db_interface(session):
    return HarvesterDBInterface(session)

def test_add_harvest_source(db_interface):
    source_data = {'name': 'Test Source', 'notification_emails': ['test@example.com']}
    new_source = db_interface.add_harvest_source(source_data)
    assert new_source.name == 'Test Source'

def test_add_and_get_harvest_source(db_interface):
    new_source = db_interface.add_harvest_source({
        'name': 'Test Source',
        'notification_emails': ['test@example.com'],
        'organization_name': 'Test Org',
        'frequency': 'daily',
        'config': '{}'
    })
    assert new_source.name == 'Test Source' 
   
    sources = db_interface.get_all_harvest_sources()
    assert any(source['name'] == 'Test Source' for source in sources)


def test_add_harvest_job(db_interface, session):
    new_source = db_interface.add_harvest_source({
            'name': 'Test Source',
            'notification_emails': ['test@example.com'],
            'organization_name': 'Test Org',
            'frequency': 'daily',
            'config': '{}'
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

