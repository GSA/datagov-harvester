from pathlib import Path
from uuid import uuid4

import pytest

from harvester.utils import open_json

HARVEST_SOURCES = Path(__file__).parents[0] / "harvest-sources"


@pytest.fixture
def bad_url_dcatus_config() -> dict:
    return {
        "_title": "test_harvest_source_title",
        "_url": "http://localhost/dcatus/bad_url.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": str(uuid4()),
    }


@pytest.fixture
def dcatus_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": str(uuid4()),
    }


@pytest.fixture
def invalid_dcatus_config() -> dict:
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/missing_title.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": str(uuid4()),
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
        "_job_id": str(uuid4()),
    }


@pytest.fixture
def dcatus_compare_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus_compare.json",
        "_extract_type": "datajson",
        "_owner_org": "example_organization",
        "_job_id": str(uuid4()),
    }


@pytest.fixture
def ckan_compare() -> dict:
    return open_json(HARVEST_SOURCES / "dcatus" / "ckan_datasets_resp.json")["results"]
