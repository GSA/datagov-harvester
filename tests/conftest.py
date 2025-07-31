import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator, List
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner
from dotenv import load_dotenv
from flask import Flask
from sqlalchemy.orm import scoped_session, sessionmaker

from app import create_app
from database.interface import HarvesterDBInterface
from database.models import HarvestJob, HarvestSource, Locations, Organization, db
from harvester.lib.load_manager import create_future_date
from harvester.utils.general_utils import dataset_to_hash, sort_dataset

load_dotenv()

logger = logging.getLogger("pytest.conftest")

EXAMPLE_DATA = Path(__file__).parents[1] / "example_data"

HARVEST_SOURCE_URL = os.getenv("HARVEST_SOURCE_URL")

# ignore tests in the functional dir
collect_ignore_glob = ["functional/*"]


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture():
    with (
        patch("harvester.lib.cf_handler.CloudFoundryClient"),
        patch("harvester.utils.general_utils.smtplib"),
        patch("app.load_manager.start", lambda: True),
    ):
        yield


@pytest.fixture(scope="session", autouse=True)
def app() -> Generator[Any, Flask, Any]:
    app = create_app()
    app.config.update({"TESTING": True})
    return app


@pytest.fixture(autouse=True)
def dbapp(app):
    with app.app_context():
        db.drop_all()  # drop unconditionally in case tests errored and the db isn't clean...
        db.create_all()
        # Add US location, used in multiple tests
        us = Locations(
            **{
                "id": "34315",
                "type": "country",
                "name": "United States",
                "display_name": "United States",
                "the_geom": "0103000020E6100000010000000500000069ACFD9DED2E5FC0F302ECA3538B384069ACFD9DED2E5FC0D4EE5701BEB148401CB7989F1BBD50C0D4EE5701BEB148401CB7989F1BBD50C0F302ECA3538B384069ACFD9DED2E5FC0F302ECA3538B3840",  # noqa E501
                "type_order": "1",
            }
        )
        db.session.add(us)
        db.session.commit()
        yield app


@pytest.fixture()
def client(app: dbapp):
    return app.test_client()


@pytest.fixture()
def session(app: dbapp) -> Generator[Any, scoped_session, Any]:
    with app.app_context():
        connection = db.engine.connect()
        transaction = connection.begin()

        SessionLocal = sessionmaker(bind=connection, autoflush=True)
        session = scoped_session(SessionLocal)
        yield session

        session.remove()
        transaction.rollback()
        connection.close()


@pytest.fixture()
def interface(session) -> HarvesterDBInterface:
    return HarvesterDBInterface(session=session)


@pytest.fixture(autouse=True)
def default_function_fixture(interface):
    logger.info("Patching core.feature.service")
    with (
        patch("harvester.harvest.db_interface", interface),
        patch("harvester.exceptions.db_interface", interface),
        patch("harvester.lib.load_manager.interface", interface),
        patch("app.routes.db", interface),
        patch("harvester.utils.ckan_utils.db", interface),
    ):
        yield
    logger.info("Patching complete. Unpatching")


@pytest.fixture
def fixtures_json():
    from tests.generate_fixtures import generate_dynamic_fixtures

    return generate_dynamic_fixtures()


@pytest.fixture
def dol_distribution_json():
    file = Path(__file__).parents[0] / "distribution-examples/dol-example.json"
    with open(file, "r") as file:
        return json.load(file)


## ORGS
@pytest.fixture
def organization_data(fixtures_json) -> dict:
    return fixtures_json["organization"][0]


@pytest.fixture
def organization_data_orm(organization_data: dict) -> Organization:
    return Organization(**organization_data)


## HARVEST SOURCES
@pytest.fixture
def source_data_dcatus(fixtures_json) -> dict:
    return fixtures_json["source"][0]


@pytest.fixture
def source_data_dcatus_orm(source_data_dcatus: dict) -> HarvestSource:
    return HarvestSource(**source_data_dcatus)


@pytest.fixture
def source_data_dcatus_2(organization_data: dict) -> dict:
    return {
        "id": "3f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Test Source 2",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_2.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_same_title(organization_data: dict) -> dict:
    return {
        "id": "50301cdb-5766-46ed-8f46-ca63844315a2",
        "name": "Test Source Same Title",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_same_title.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_waf_csdgm(organization_data: dict) -> dict:
    return {
        "id": "55dca495-3b92-4fe4-b9c5-d433cbc3c82d",
        "name": "Test Source (WAF CSDGM)",
        "notification_emails": ["wafl@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/waf/",
        "schema_type": "csdgm",
        "source_type": "waf",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_waf_iso19115_2(organization_data: dict) -> dict:
    return {
        "id": "8c3cd8c5-6174-42ef-9512-10503533c3a9",
        "name": "Test Source (WAF ISO19115_2)",
        "notification_emails": ["wafl@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/iso_2_waf/",
        "schema_type": "iso19115_2",
        "source_type": "waf",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_iso19115_2_orm(source_data_waf_iso19115_2: dict) -> HarvestSource:
    return HarvestSource(**source_data_waf_iso19115_2)


@pytest.fixture
def source_data_dcatus_invalid(organization_data: dict) -> dict:
    return {
        "id": "2bfcb047-70dc-435a-a46c-4dec5df7532d",
        "name": "Test Source ( Invalid Records )",
        "notification_emails": ["invalid@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/missing_title.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_single_record(organization_data: dict) -> dict:
    return {
        "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Single Record Test Source",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_single_record.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "on_error",
    }


@pytest.fixture
def source_data_dcatus_single_record_non_federal(organization_data: dict) -> dict:
    return {
        "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Single Record Test Source",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_single_record_non-federal.json",
        "schema_type": "dcatus1.1: non-federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_bad_license_uri(organization_data: dict) -> dict:
    return {
        "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Single Record Test Source",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_bad_license_uri.json",
        "schema_type": "dcatus1.1: non-federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_multiple_invalid(organization_data: dict) -> dict:
    return {
        "id": "2f2652de-91df-4c63-8b53-bfced20b276b",
        "name": "Single Record Test Source",
        "notification_emails": ["email@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/dcatus_multiple_invalid.json",
        "schema_type": "dcatus1.1: non-federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_bad_url(organization_data: dict) -> dict:
    return {
        "id": "b059e587-a4a1-422e-825a-830b4913dbfb",
        "name": "Bad URL Source",
        "notification_emails": ["bad@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": f"{HARVEST_SOURCE_URL}/dcatus/bad_url.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def source_data_dcatus_invalid_records(organization_data) -> dict:
    return {
        "id": "8e7f539b-0a83-43ad-950e-3976bb11a425",
        "name": "Invalid Record Source",
        "notification_emails": ["invalid_record@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "daily",
        "url": "http://localhost/dcatus/missing_title.json",
        "schema_type": "dcatus1.1: federal",
        "source_type": "document",
        "notification_frequency": "always",
    }


## HARVEST JOBS
@pytest.fixture
def job_data_dcatus(fixtures_json) -> dict:
    return fixtures_json["job"][0]


@pytest.fixture
def job_data_new(fixtures_json) -> dict:
    return [job for job in fixtures_json["job"] if job["status"] == "new"][0]


@pytest.fixture
def job_data_dcatus_orm(job_data_dcatus: dict) -> HarvestJob:
    return HarvestJob(**job_data_dcatus)


@pytest.fixture
def job_data_dcatus_2(source_data_dcatus_2: dict) -> dict:
    return {
        "id": "392ac4b3-79a6-414b-a2b3-d6c607d3b8d4",
        "status": "new",
        "harvest_source_id": source_data_dcatus_2["id"],
    }


@pytest.fixture
def job_data_waf_csdgm(source_data_waf_csdgm: dict) -> dict:
    return {
        "id": "963cdc51-94d5-425d-a688-e0a57e0c5dd2",
        "status": "new",
        "harvest_source_id": source_data_waf_csdgm["id"],
    }


@pytest.fixture
def job_data_waf_iso19115_2(source_data_waf_iso19115_2: dict) -> dict:
    return {
        "id": "83c431e6-0f53-4471-8003-b6afc0541101",
        "status": "new",
        "harvest_source_id": source_data_waf_iso19115_2["id"],
    }


@pytest.fixture
def job_data_dcatus_invalid(source_data_dcatus_invalid: dict) -> dict:
    return {
        "id": "59df7ba5-102d-4ae3-abd6-01b7eb26a338",
        "status": "new",
        "harvest_source_id": source_data_dcatus_invalid["id"],
    }


@pytest.fixture
def job_data_dcatus_bad_url(source_data_dcatus_bad_url: dict) -> dict:
    return {
        "id": "707aee7b-bf72-4e07-a5fc-68980765b214",
        "status": "new",
        "harvest_source_id": source_data_dcatus_bad_url["id"],
    }


@pytest.fixture
def job_data_dcatus_non_federal(
    source_data_dcatus_single_record_non_federal: dict,
) -> dict:
    return {
        "id": "34dce1c4-dfab-4426-af11-21accfd0ef34",
        "status": "new",
        "harvest_source_id": source_data_dcatus_single_record_non_federal["id"],
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


## HARVEST JOB ERRORS
@pytest.fixture
def job_error_data(fixtures_json) -> dict:
    return fixtures_json["job_error"][0]


## HARVEST RECORDS
@pytest.fixture
def record_data_dcatus(fixtures_json) -> List[dict]:
    return fixtures_json["record"]


@pytest.fixture
def record_data_dcatus_2(job_data_dcatus_2):
    return [
        {
            "id": "72bae4b2-336e-49df-bc4c-410dc73dc316",
            "identifier": "test_identifier-2",
            "harvest_job_id": job_data_dcatus_2["id"],
            "harvest_source_id": job_data_dcatus_2["harvest_source_id"],
            "action": "create",
            "status": "error",
            "source_raw": "example data 2",
        }
    ]


## HARVEST RECORD ERRORS
@pytest.fixture
def record_error_data(fixtures_json) -> List[dict]:
    return fixtures_json["record_error"]


@pytest.fixture
def record_error_data_2(record_data_dcatus_2) -> dict:
    return [
        {
            "harvest_record_id": record_data_dcatus_2[0]["id"],
            "harvest_job_id": record_data_dcatus_2[0]["harvest_job_id"],
            "message": "record is invalid",
            "type": "ValidationException",
        }
    ]


@pytest.fixture
def duplicated_identifier_records():
    return [
        {
            "identifier": "GSA-2015-02-26-1",
            "title": "Networx Business Volume FY2013, 3rd Qtr",
            "description": "Original dataset",
            "accessLevel": "public",
            "modified": "2019-06-12",
        },
        {
            "identifier": "GSA-2015-02-26-1",
            "title": "Duplicate Dataset",
            "description": "This is first duplicate of the dataset above.",
            "accessLevel": "public",
            "modified": "2019-07-01",
        },
        {
            "identifier": "GSA-2015-02-26-1",
            "title": "Duplicate Dataset",
            "description": "This is second duplicate of the dataset above.",
            "accessLevel": "public",
            "modified": "2019-08-01",
        },
    ]


@pytest.fixture
def interface_no_jobs(interface, organization_data, source_data_dcatus):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)

    return interface


@pytest.fixture
def interface_with_fixture_json(
    interface_no_jobs,
    job_data_dcatus,
    job_error_data,
    record_data_dcatus,
    record_error_data,
):
    interface_no_jobs.add_harvest_job(job_data_dcatus)
    interface_no_jobs.add_harvest_job_error(job_error_data)
    for record in record_data_dcatus:
        interface_no_jobs.add_harvest_record(record)
    for error in record_error_data:
        interface_no_jobs.add_harvest_record_error(error)

    return interface_no_jobs


@pytest.fixture
def interface_with_multiple_sources(
    interface_with_fixture_json,
    source_data_dcatus_2,
    job_data_dcatus_2,
    record_data_dcatus_2,
    record_error_data_2,
):
    interface_with_fixture_json.add_harvest_source(source_data_dcatus_2)
    interface_with_fixture_json.add_harvest_job(job_data_dcatus_2)
    for record in record_data_dcatus_2:
        interface_with_fixture_json.add_harvest_record(record)
    for error in record_error_data_2:
        interface_with_fixture_json.add_harvest_record_error(error)

    return interface_with_fixture_json


@pytest.fixture
def interface_with_multiple_jobs(interface_no_jobs, source_data_dcatus):
    statuses = ["new", "in_progress", "complete", "error"]
    frequencies = ["daily", "monthly"]
    jobs = [
        {
            "status": status,
            "harvest_source_id": source_data_dcatus["id"],
            "date_created": create_future_date(frequency),
        }
        for status in statuses
        for frequency in frequencies
    ]
    jobs_2 = [
        {
            "status": status,
            "harvest_source_id": source_data_dcatus["id"],
        }
        for status in statuses
    ]

    for job in jobs + jobs_2:
        interface_no_jobs.add_harvest_job(job)

    return interface_no_jobs


@pytest.fixture
def latest_records(
    source_data_dcatus, source_data_dcatus_2, job_data_dcatus, job_data_dcatus_2
):
    return [
        {
            "identifier": "a",
            "date_created": "2024-01-01T00:00:00.001Z",
            "source_raw": "data",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "a",
            "date_created": "2024-03-01T00:00:00.001Z",
            "source_raw": "data_1",
            "status": "success",
            "action": "update",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "b",
            "date_created": "2024-03-01T00:00:00.001Z",
            "source_raw": "data_10",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "b",
            "date_created": "2022-05-01T00:00:00.001Z",
            "source_raw": "data_30",
            "status": "error",
            "action": "update",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "c",
            "date_created": "2024-05-01T00:00:00.001Z",
            "source_raw": "data_12",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "d",
            "date_created": "2024-05-01T00:00:00.001Z",
            "source_raw": "data_2",
            "status": "success",
            "action": "delete",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "d",
            "date_created": "2024-04-01T00:00:00.001Z",
            "source_raw": "data_5",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "e",
            "date_created": "2024-04-01T00:00:00.001Z",
            "source_raw": "data_123",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "e",
            "date_created": "2024-04-02T00:00:00.001Z",
            "source_raw": "data_123",
            "status": "success",
            "action": "delete",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "e",
            "date_created": "2024-04-03T00:00:00.001Z",
            "source_raw": "data_123",
            "status": "success",
            "action": "create",
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_job_id": job_data_dcatus["id"],
        },
        {
            "identifier": "f",
            "date_created": "2024-04-03T00:00:00.001Z",
            "source_raw": "data_123",
            "status": "success",
            "action": "create",
            # different harvest source and job
            "harvest_source_id": source_data_dcatus_2["id"],
            "harvest_job_id": job_data_dcatus_2["id"],
        },
        {
            "identifier": "f",
            "date_created": "2024-04-04T00:00:00.001Z",
            "source_raw": "data_123456",
            "status": "success",
            "action": "update",
            "harvest_source_id": source_data_dcatus_2["id"],
            "harvest_job_id": job_data_dcatus_2["id"],
        },
    ]


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
def single_internal_record(internal_compare_data):
    record = internal_compare_data["records"][0]
    return {
        "identifier": record["identifier"],
        "harvest_job_id": internal_compare_data["job_id"],
        "harvest_source_id": internal_compare_data["harvest_source_id"],
        "source_hash": dataset_to_hash(sort_dataset(record)),
        "status": "success",
        "action": "create",
    }


@pytest.fixture
def dhl_cf_task_data() -> dict:
    return {
        "task_id": "cf_task_integration",
        "command": "/usr/bin/sleep 60",
    }


@pytest.fixture
def iso19115_2_transform() -> dict:
    return {
        "@type": "dcat:Dataset",
        "title": "Bringing Wetlands to Market: Expanding Blue Carbon Implementation - NERRS/NSC(NERRS Science Collaborative)",
        "description": "Blue carbon storage – carbon sequestration in coastal wetlands – can help coastal managers and policymakers achieve broader wetlands management, restoration, and conservation goals, in part by securing payment for carbon credits. Despite considerable interest in bringing wetland restoration projects to market, the transaction costs related to quantifying greenhouse gas fluxes and carbon storage in restored marsh has been a significant limiting factor to realizing these projects. The Waquoit Bay National Estuarine Research Reserve has been at the forefront of blue carbon research and end user engagement. Building on the efforts of a previous project, Bringing Wetlands to Market in Massachusetts, this project developed a verified and generalized model that can be used across New England and the mid-Atlantic East Coast to assess and predict greenhouse gas fluxes and potential wetland carbon across a wide\nenvironmental gradient using a small set of readily available data. Using this model, the project conducted a first-of-its-kind market feasibility assessment for the Herring River Restoration Project, one of the largest potential wetland restoration projects in New England. The project team developed targeted tools and education programs for coastal managers, decision makers, and teachers. These efforts have built an understanding of blue carbon and the capacity to integrate blue carbon considerations into restoration and management decisions.",
        "keyword": [
            "EARTH SCIENCE > BIOSPHERE > ECOSYSTEMS > MARINE ECOSYSTEMS > COASTAL",
            "EARTH SCIENCE > HUMAN DIMENSIONS > ENVIRONMENTAL GOVERNANCE/MANAGEMENT",
            "EARTH SCIENCE > LAND SURFACE > LANDSCAPE > RECLAMATION/REVEGETATION/RESTORATION",
            "EARTH SCIENCE > BIOSPHERE > AQUATIC ECOSYSTEMS > WETLANDS > ESTUARINE WETLANDS",
            "blue carbon",
            "carbon sequestration",
            "coastal wetlands",
            "Waquoit Bay NERR, MA",
            "DOC/NOAA/NOS/OCM > Office of Coastal Management, National Ocean Service, NOAA, U.S. Department of Commerce",
            "NERRS",
        ],
        "modified": "2024-02-29T00:00:00.000+00:00",
        "publisher": {
            "@type": "org:Organization",
            "name": "Office for Coastal Management",
        },
        "contactPoint": {
            "@type": "vcard:Contact",
            "fn": "Office for Coastal Management",
            "hasEmail": "mailto:test@gmail.com",
        },
        "identifier": "gov.noaa.nmfs.inport:47598",
        "accessLevel": "non-public",
        "distribution": [
            {
                "@type": "dcat:Distribution",
                "description": "NOAA Data Management Plan for this record on InPort.",
                "downloadURL": "https://www.fisheries.noaa.gov/inportserve/waf/noaa/nos/ocm/dmp/pdf/47598.pdf",
                "title": "NOAA Data Management Plan (DMP)",
                "mediaType": "placeholder/value",
            },
            {
                "@type": "dcat:Distribution",
                "description": "Global Change Master Directory (GCMD). 2024. GCMD Keywords, Version 19. Greenbelt, MD: Earth Science Data and Information System, Earth Science Projects Division, Goddard Space Flight Center (GSFC), National Aeronautics and Space Administration (NASA). URL (GCMD Keyword Forum Page): https://forum.earthdata.nasa.gov/app.php/tag/GCMD+Keywords",
                "downloadURL": "https://forum.earthdata.nasa.gov/app.php/tag/GCMD%2BKeywords",
                "title": "GCMD Keyword Forum Page",
                "mediaType": "placeholder/value",
            },
            {
                "@type": "dcat:Distribution",
                "description": "View the complete metadata record on InPort for more information about this dataset.",
                "downloadURL": "https://www.fisheries.noaa.gov/inport/item/47598",
                "title": "Full Metadata Record",
                "mediaType": "placeholder/value",
            },
        ],
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "rights": "otherRestrictions, unclassified",
        "spatial": "-70.482,41.544,-70.555,41.64",
        "temporal": "2015-09-01T00:00:00+00:00/2019-09-01T00:00:00+00:00",
        "language": [],
        "references": [
            "https://www.fisheries.noaa.gov/inportserve/waf/noaa/nos/ocm/dmp/pdf/47598.pdf"
        ],
        "landingPage": "https://www.fisheries.noaa.gov/inport/item/47598",
    }


@pytest.fixture
def iso19115_1_transform() -> dict:
    return {
        "@type": "dcat:Dataset",
        "title": "Demographics for US Census Tracts - 2012 (American Community Survey 2008-2012 Derived Summary Tables)",
        "description": "This map service displays data derived from the 2008-2012 American Community Survey (ACS). Values derived from the ACS and used for this map service include: Total Population, Population Density (per square mile), Percent Minority, Percent Below Poverty Level, Percent Age (less than 5, less than 18, and greater than 64), Percent Housing Units Built Before 1950, Percent (population) 25 years and over (with less than a High School Degree and with a High School Degree), Percent Linguistically Isolated Households, Population of American Indians and Alaskan Natives, Population of American Indians and Alaskan Natives Below Poverty Level, and Percent Low Income Population (Less Than 2X Poverty Level). This dataset was produced by the US EPA to support research and online mapping activities related to EnviroAtlas. EnviroAtlas (https://www.epa.gov/enviroatlas) allows the user to interact with a web-based, easy-to-use, mapping application to view and analyze multiple ecosystem services for the contiguous United States.",
        "keyword": [
            "Human",
            "Human",
            "Alabama",
            "Alaska",
            "American Samoa",
            "Arizona",
            "Arkansas",
            "California",
            "Canada",
            "Colorado",
            "Connecticut",
            "Delaware",
            "Florida",
            "Georgia",
            "Hawaii",
            "Idaho",
            "Illinois",
            "Indiana",
            "Iowa",
            "Kansas",
            "Kentucky",
            "Louisiana",
            "Maine",
            "Maryland",
            "Massachusetts",
            "Mexico",
            "Michigan",
            "Minnesota",
            "Mississippi",
            "Missouri",
            "Montana",
            "Nebraska",
            "Nevada",
            "New Hampshire",
            "New Jersey",
            "New Mexico",
            "New York",
            "North Carolina",
            "North Dakota",
            "Ohio",
            "Oklahoma",
            "Oregon",
            "Pennsylvania",
            "Rhode Island",
            "South Carolina",
            "South Dakota",
            "Tennessee",
            "Texas",
            "United States",
            "Utah",
            "Vermont",
            "Virginia",
            "Washington",
            "Washington DC",
            "West Virginia",
            "Wisconsin",
            "Wyoming",
        ],
        "modified": "2017-05-23T00:00:00.000+00:00",
        "publisher": {
            "@type": "org:Organization",
            "name": "U.S. EPA Office of Environmental Information (OEI) - Office of Information Analysis and Access (OIAA)",
        },
        "contactPoint": {
            "@type": "vcard:Contact",
            "fn": "U.S. Environmental Protection Agency, Office of Research and Development-Sustainable and Healthy Communities Research Program, EnviroAtlas",
            "hasEmail": "mailto:enviroatlas@epa.gov",
        },
        "identifier": "{4c6928d8-6ac2-4909-8b3d-a29e2805ce2d}",
        "accessLevel": "non-public",
        "distribution": [],
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "rights": "otherRestrictions, PUBLIC",
        "spatial": "-12.68151645,6.65223303,-138.21454852,61.7110157",
        "temporal": "2008-01-01T00:00:00+00:00/2012-01-01T00:00:00+00:00",
        "issued": "2017-05-23T00:00:00.000+00:00",
        "language": [],
    }


@pytest.fixture
def named_location_us():
    return (
        '{"type":"MultiPolygon","coordinates":[[[[-124.733253,24.544245],[-124.733253,49.388611],'
        "[-66.954811,49.388611],[-66.954811,24.544245],[-124.733253,24.544245]]]]}"
    )


@pytest.fixture
def named_location_stoneham():
    return (
        '{"type":"MultiPolygon","coordinates":[[[[-71.1192,42.444],[-71.1192,42.5022],'
        "[-71.0749,42.5022],[-71.0749,42.444],[-71.1192,42.444]]]]}"
    )


@pytest.fixture
def invalid_envelope_geojson():
    return '{"type": "envelope", "coordinates": [[-81.0563, 34.9991], [-80.6033, 35.4024]]}'


@pytest.fixture
def mock_requests_get_ms_iis_waf(monkeypatch):
    """Fixture to mock requests.get with ms-iis-waf HTML content"""
    import requests

    def mock_get(url, *args, **kwargs):
        """Mock function to return a predefined HTML response"""
        mock_response = Mock()
        mock_response.status_code = 200

        # Read mock HTML content from file
        file_path = Path(__file__).parent / "waf-html-examples/ms-iis-waf.html"
        with open(file_path, "r", encoding="utf-8") as file:
            mock_response.text = file.read()

        # Set UTF-8 content
        mock_response.content = mock_response.text.encode("utf-8")

        return mock_response

    # Apply the patch using monkeypatch
    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def dcatus_keywords():
    return [
        "EARTH         SCIENCE > BIOSPHERE > ECOSYSTEMS > MARINE ECOSYSTEMS > COASTAL",
        "earth science",
        "Waquoit Bay NERR, MA",
        "DOC/NOAA/NOS/OCM > Office of Coastal Management, National Ocean Service, NOAA, U.S. Department of Commerce",
        "NERRS",
    ]


@pytest.fixture
def bad_jsonschema_uri():
    # this is a truncated value present in the wild via dcatus license
    return "<p align='center' style='margin-top: 0px; margin-bottom: 1.55rem"


@pytest.fixture
def runner():
    """
    Fixture to provide a CLI runner for testing command line interfaces w/ click.
    """
    return CliRunner()


@pytest.fixture
def mock_interface():
    """Mock the HarvesterDBInterface for testing."""
    with patch("scripts.orphan_job_clean_up.HarvesterDBInterface") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture
def mock_cf_handler():
    """Fixture to mock CFHandler."""
    with patch("scripts.orphan_job_clean_up.CFHandler") as mock_handler_class:
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler
        yield mock_handler


@pytest.fixture
def timestamps():
    """Fixture providing various timestamps for testing."""
    now = datetime.now(timezone.utc)
    return {
        "now": now,
        "fresh": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "stale": (now - timedelta(hours=25)).isoformat().replace("+00:00", "Z"),
    }


@pytest.fixture
def sample_running_tasks(timestamps):
    """Fixture providing sample running tasks from CF."""
    return [
        {
            "id": 244,  # sequence_id
            "guid": "cf-task-guid-1",
            "command": "python harvester/harvest.py"
            " 7cd474ba-5437-4d50-b7ec-6114a877510e harvest",
            "state": "RUNNING",
            "updated_at": timestamps["stale"],  # Stale task
            "name": "harvest-job-stale",
        },
        {
            "id": 245,
            "guid": "cf-task-guid-2",
            "command": "python harvester/harvest.py"
            " 12345678-1234-1234-1234-123456789abc harvest",
            "state": "RUNNING",
            "updated_at": timestamps["fresh"],  # Fresh task
            "name": "harvest-job-fresh",
        },
        {
            "id": 246,
            "guid": "cf-task-guid-3",
            "command": "python harvester/harvest.py"
            " abcdef12-3456-7890-abcd-ef1234567890 harvest",
            "state": "RUNNING",
            "updated_at": timestamps["stale"],  # Another stale task
            "name": "harvest-job-stale-2",
        },
    ]


@pytest.fixture
def sample_harvest_jobs():
    """Fixture providing sample harvest job database objects."""
    job1 = Mock()
    job1.id = "7cd474ba-5437-4d50-b7ec-6114a877510e"

    job2 = Mock()
    job2.id = "12345678-1234-1234-1234-123456789abc"

    job3 = Mock()
    job3.id = "abcdef12-3456-7890-abcd-ef1234567890"

    return {
        "7cd474ba-5437-4d50-b7ec-6114a877510e": job1,
        "12345678-1234-1234-1234-123456789abc": job2,
        "abcdef12-3456-7890-abcd-ef1234567890": job3,
    }


@pytest.fixture
def dcatus_non_federal_schema():
    schema = Path(__file__).parents[1] / "schemas" / "non-federal_dataset.json"
    with open(schema) as f:
        return json.load(f)
