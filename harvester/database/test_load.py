import pytest
from models import HarvestJob, HarvestError, HarvestSource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URI=os.getenv("DATABASE_URI")
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(bind=engine)

@pytest.fixture(scope="module")
def db_session():
    connection = engine.connect()
    transaction = connection.begin()
    session = SessionLocal(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

def create_fake_harvest_job(source_id):
    return HarvestJob(
        harvest_source_id=source_id,
        date_created='2024-02-12',
        date_finished='2024-02-12',
        records_added=1,
        records_updated=0,
        records_deleted=0,
        records_errored=0,
        records_ignored=0
    )

def test_insert_massive_amount_of_jobs(db_session):
    source = HarvestSource(name='Test Source', notification_emails=[], organization_name='Test Org', frequency='daily', config='{}')
    db_session.add(source)
    db_session.commit()
    
    for id in range(1000): 
        print(id)
        job = create_fake_harvest_job(source.id)
        db_session.add(job)
    
    db_session.commit()
    assert db_session.query(HarvestJob).count() == 1000
