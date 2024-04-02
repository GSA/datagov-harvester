from pathlib import Path
import os

import pytest
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from app.models import Base
from app.interface import HarvesterDBInterface

from harvester.utils import open_json
from harvester.utils import CFHandler

load_dotenv()

HARVEST_SOURCES = Path(__file__).parents[0] / "harvest-sources"


@pytest.fixture(scope="session")
def db_session():
    DATABASE_SERVER = os.getenv("DATABASE_SERVER")
    DATABASE_URI = os.getenv("DATABASE_URI")
    TEST_SCHEMA = "test_schema"
    modified_uri = DATABASE_URI.replace("@" + DATABASE_SERVER, "@localhost")
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


@pytest.fixture(scope="session")
def db_interface(db_session):
    return HarvesterDBInterface(db_session)


@pytest.fixture
def bad_url_dcatus_config() -> dict:
    return {
        "_title": "test_harvest_source_title",
        "_url": "http://localhost/dcatus/bad_url.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
    }


@pytest.fixture
def dcatus_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
    }


@pytest.fixture
def invalid_dcatus_config() -> dict:
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/missing_title.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
    }


@pytest.fixture
def waf_config() -> dict:
    """example waf job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost",
        "_extract_type": "waf-collection",
        "_owner_org": "example_organization",
        "_waf_config": {"filters": ["../", "dcatus/"]},
        "_job_id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
    }


@pytest.fixture
def dcatus_compare_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus_compare.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": "1db556ff-fb02-438b-b7d2-ad914e1f2531",
    }


@pytest.fixture
def ckan_compare() -> dict:
    return open_json(HARVEST_SOURCES / "dcatus" / "ckan_datasets_resp.json")["results"]


@pytest.fixture
def cf_handler() -> CFHandler:
    return CFHandler()


@pytest.fixture
def dhl_cf_task_data() -> dict:
    return {
        "app_uuid": "f4ab7f86-bee0-44fd-8806-1dca7f8e215a",
        "task_id": "cf_task_integration",
        "command": "/usr/bin/sleep 60",
    }
