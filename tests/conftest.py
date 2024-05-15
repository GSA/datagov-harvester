import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from sqlalchemy.orm import scoped_session, sessionmaker
from app import create_app
from flask import Flask
from database.interface import HarvesterDBInterface
from database.models import db
from harvester.utils import CFHandler

load_dotenv()

EXAMPLE_DATA = Path(__file__).parents[1] / "example_data"


@pytest.fixture(scope="session")
def app() -> Flask:
    app = create_app()

    with app.app_context():
        db.create_all()
        yield app
        db.drop_all()


@pytest.fixture(scope="function")
def session(app) -> scoped_session:
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
def interface(session) -> HarvesterDBInterface:
    return HarvesterDBInterface(session=session)


@pytest.fixture
def organization_data() -> dict:
    return {
        "name": "Test Org",
        "logo": "https://example.com/logo.png",
        "id": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
    }


@pytest.fixture
def source_data_dcatus(organization_data: dict) -> dict:
    return {
        "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Test Source",
        "notification_emails": "email@example.com",
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost:80/dcatus/dcatus.json",
        "schema_type": "type1",
        "source_type": "dcatus",
        "status": "active",
    }


@pytest.fixture
def source_data_waf(organization_data: dict) -> dict:
    return {
        "id": "55dca495-3b92-4fe4-b9c5-d433cbc3c82d",
        "name": "Test Source (WAF)",
        "notification_emails": "wafl@example.com",
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost:80/waf",
        "schema_type": "type1",
        "source_type": "waf",
        "status": "active",
    }


@pytest.fixture
def source_data_dcatus_invalid(organization_data: dict) -> dict:
    return {
        "id": "2bfcb047-70dc-435a-a46c-4dec5df7532d",
        "name": "Test Source ( Invalid Records )",
        "notification_emails": "invalid@example.com",
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost:80/dcatus/missing_title.json",
        "schema_type": "type1",
        "source_type": "dcatus",
        "status": "active",
    }


@pytest.fixture
def job_data_dcatus(source_data_dcatus: dict) -> dict:
    return {
        "id": "6bce761c-7a39-41c1-ac73-94234c139c76",
        "status": "pending",
        "harvest_source_id": source_data_dcatus["id"],
    }


@pytest.fixture
def job_data_waf(source_data_waf: dict) -> dict:
    return {
        "id": "963cdc51-94d5-425d-a688-e0a57e0c5dd2",
        "status": "pending",
        "harvest_source_id": source_data_waf["id"],
    }


@pytest.fixture
def job_data_dcatus_invalid(source_data_dcatus_invalid: dict) -> dict:
    return {
        "id": "59df7ba5-102d-4ae3-abd6-01b7eb26a338",
        "status": "pending",
        "harvest_source_id": source_data_dcatus_invalid["id"],
    }


@pytest.fixture
def record_data_dcatus(job_data_dcatus: dict) -> dict:
    return {
        "identifier": "test_identifier",
        "harvest_job_id": job_data_dcatus["id"],
        "harvest_source_id": job_data_dcatus["harvest_source_id"],
    }


@pytest.fixture
def source_data_dcatus_bad_url(organization_data: dict) -> dict:
    return {
        "id": "b059e587-a4a1-422e-825a-830b4913dbfb",
        "name": "Bad URL Source",
        "notification_emails": "bad@example.com",
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost:80/dcatus/bad_url.json",
        "schema_type": "type1",
        "source_type": "dcatus",
        "status": "active",
    }


@pytest.fixture
def job_data_dcatus_bad_url(source_data_dcatus_bad_url: dict) -> dict:
    return {
        "id": "707aee7b-bf72-4e07-a5fc-68980765b214",
        "status": "pending",
        "harvest_source_id": source_data_dcatus_bad_url["id"],
    }


@pytest.fixture
def source_data_dcatus_invalid_records(organization_data) -> dict:
    return {
        "id": "8e7f539b-0a83-43ad-950e-3976bb11a425",
        "name": "Invalid Record Source",
        "notification_emails": "invalid_record@example.com",
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost/dcatus/missing_title.json",
        "schema_type": "type1",
        "source_type": "dcatus",
        "status": "active",
    }


@pytest.fixture
def source_data_dcatus_invalid_records_job(
    source_data_dcatus_invalid_records: dict,
) -> dict:
    return {
        "id": "2b57046b-cfda-4a37-bf84-a4766a54a743",
        "status": "new",
        "harvest_source_id": source_data_dcatus_invalid_records["id"],
    }


@pytest.fixture
def interface_with_multiple_jobs(
    interface, organization_data, source_data_dcatus, source_data_waf
):
    statuses = ["pending", "pending_manual", "in_progress", "complete"]
    source_ids = [source_data_dcatus["id"], source_data_waf["id"]]
    jobs = [
        {"status": status, "harvest_source_id": source}
        for status in statuses
        for source in source_ids
    ]
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_source(source_data_waf)

    for job in jobs:
        interface.add_harvest_job(job)

    return interface


@pytest.fixture
def internal_compare_data(job_data_dcatus: dict) -> dict:
    # ruff: noqa: E501
    return {
        "job_id": job_data_dcatus["id"],
        "harvest_source_id": job_data_dcatus["harvest_source_id"],
        "records": [
            {
                "contactPoint": {
                    "fn": "Harold W. Hild",
                    "hasEmail": "mailto:hhild@CFTC.GOV",
                },
                "describedBy": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ExplanatoryNotes/index.htm",
                "description": "COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC",
                "distribution": [
                    {
                        "accessURL": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
                    }
                ],
                "modified": "R/P1W",
                "programCode": ["000:000"],
                "publisher": {
                    "name": "U.S. Commodity Futures Trading Commission",
                    "subOrganizationOf": {"name": "U.S. Government"},
                },
                "title": "Commitment of Traders",
                "accessLevel": "public",
                "bureauCode": ["339:00"],
                "identifier": "cftc-dc10",
                "keyword": ["commitment of traders", "cot", "open interest"],
            },
            {
                "accessLevel": "public",
                "bureauCode": ["339:00"],
                "contactPoint": {
                    "fn": "Harold W. Hild",
                    "hasEmail": "mailto:hhild@CFTC.GOV",
                },
                "describedBy": "https://www.cftc.gov/MarketReports/BankParticipationReports/ExplanatoryNotes/index.htm",
                "description": "The Bank Participation Report (BPR), developed by the Division of Market Oversight to provide the U.S. banking authorities and the Bank for International Settlements (BIS, located in Basel, Switzerland) aggregate large-trader positions of banks participating in various financial and non-financial commodity futures.",
                "distribution": [
                    {
                        "accessURL": "https://www.cftc.gov/MarketReports/BankParticipationReports/index.htm"
                    }
                ],
                "identifier": "cftc-dc2",
                "keyword": ["bank participation report", "bpr", "banking", "new value"],
                "modified": "R/P1M",
                "programCode": ["000:000"],
                "publisher": {
                    "name": "U.S. Commodity Futures Trading Commission",
                    "subOrganizationOf": {"name": "U.S. Government"},
                },
                "title": "Bank Participation Reports",
            },
        ],
    }


@pytest.fixture
def cf_handler() -> CFHandler:
    url = os.getenv("CF_API_URL")
    user = os.getenv("CF_SERVICE_USER")
    password = os.getenv("CF_SERVICE_AUTH")

    return CFHandler(url, user, password)


@pytest.fixture
def dhl_cf_task_data() -> dict:
    return {
        "app_guuid": "f4ab7f86-bee0-44fd-8806-1dca7f8e215a",
        "task_id": "cf_task_integration",
        "command": "/usr/bin/sleep 60",
    }
