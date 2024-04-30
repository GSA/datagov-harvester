import pytest
import uuid
from sqlalchemy.orm import scoped_session, sessionmaker

from app import create_app
from app.interface import HarvesterDBInterface
from app.models import db


@pytest.fixture(scope="session")
def app():
    _app = create_app(testing=True)
    _app.config["TESTING"] = True
    _app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"

    with _app.app_context():
        db.create_all()
        yield _app
        db.drop_all()


@pytest.fixture(scope="function")
def session(app):
    with app.app_context():
        connection = db.engine.connect()
        transaction = connection.begin()

        SessionLocal = sessionmaker(bind=connection, autocommit=False, autoflush=False)
        session = scoped_session(SessionLocal)
        yield session

        session.remove()
        transaction.rollback()
        connection.close()


@pytest.fixture(scope="function")
def interface(session):
    return HarvesterDBInterface(session=session)


@pytest.fixture
def org_data():
    return {"name": "Test Org", "logo": "https://example.com/logo.png"}


@pytest.fixture
def source_data(organization):
    return {
        "name": "Test Source",
        "notification_emails": "email@example.com",
        "organization_id": organization.id,
        "frequency": "daily",
        "url": "http://example.com",
        "schema_type": "type1",
        "source_type": "typeA",
        "status": "active",
    }


@pytest.fixture
def organization(interface, org_data):
    org = interface.add_organization(org_data)
    return org


@pytest.fixture
def job_data():
    return {"status": "new"}


@pytest.fixture
def record_data():
    return {"identifier": "1234abcd", "source_hash": "1234abcd"}


def test_add_organization(interface, org_data):
    org = interface.add_organization(org_data)
    assert org is not None
    assert org.name == "Test Org"


def test_get_all_organizations(interface, org_data):
    interface.add_organization(org_data)

    orgs = interface.get_all_organizations()
    assert len(orgs) > 0
    assert orgs[0]["name"] == "Test Org"


def test_update_organization(interface, organization):
    updates = {"name": "Updated Org"}
    updated_org = interface.update_organization(organization.id, updates)
    assert updated_org["name"] == "Updated Org"


def test_delete_organization(interface, organization):
    result = interface.delete_organization(organization.id)
    assert result == "Organization deleted successfully"


def test_add_harvest_source(interface, source_data):
    source = interface.add_harvest_source(source_data)
    assert source is not None
    assert source.name == source_data["name"]


def test_get_all_harvest_sources(interface, source_data):
    interface.add_harvest_source(source_data)
    sources = interface.get_all_harvest_sources()
    assert len(sources) > 0
    assert sources[0]["name"] == source_data["name"]


def test_get_harvest_source(interface, source_data):
    source = interface.add_harvest_source(source_data)
    fetched_source = interface.get_harvest_source(source.id)
    assert fetched_source is not None
    assert fetched_source["name"] == source_data["name"]


def test_update_harvest_source(interface, source_data):
    source = interface.add_harvest_source(source_data)
    updates = {"name": "Updated Test Source"}
    updated_source = interface.update_harvest_source(source.id, updates)
    assert updated_source is not None
    assert updated_source["name"] == updates["name"]


def test_delete_harvest_source(interface, source_data):
    source = interface.add_harvest_source(source_data)
    assert source is not None

    response = interface.delete_harvest_source(source.id)
    assert response == "Harvest source deleted successfully"

    deleted_source = interface.get_harvest_source(source.id)
    assert deleted_source is None


def test_harvest_source_by_jobid(interface, source_data, job_data):
    source = interface.add_harvest_source(source_data)
    job_data["harvest_source_id"] = source.id

    harvest_job = interface.add_harvest_job(job_data)
    harvest_source = interface.get_source_by_jobid(harvest_job.id)

    assert source.id == harvest_source["id"]


def test_add_harvest_record(interface, source_data, job_data, record_data):
    source = interface.add_harvest_source(source_data)
    job_data["harvest_source_id"] = source.id
    harvest_job = interface.add_harvest_job(job_data)
    record_data["harvest_source_id"] = source.id
    record_data["harvest_job_id"] = harvest_job.id

    record = interface.add_harvest_record(record_data)

    assert record.harvest_source_id == source.id
    assert record.harvest_job_id == harvest_job.id


def test_add_harvest_records(interface, source_data, job_data, record_data):
    source = interface.add_harvest_source(source_data)
    job_data["harvest_source_id"] = source.id
    harvest_job = interface.add_harvest_job(job_data)
    record_data["harvest_source_id"] = source.id
    record_data["harvest_job_id"] = harvest_job.id

    records_data = [record_data.copy() for i in range(10)]
    for record in records_data:
        record["identifier"] = str(uuid.uuid4())
    success = interface.add_harvest_records(records_data)
    assert success is True
    assert len(interface.get_all_harvest_records()) == 10
