from pathlib import Path

import pytest

from harvester.utils import open_json

HARVEST_SOURCES = Path(__file__).parents[0] / "harvest-sources"


@pytest.fixture
def dcatus_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus.json",
        "_extract_type": "datajson",
    }


@pytest.fixture
def waf_config() -> dict:
    """example waf job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost",
        "_extract_type": "waf-collection",
        "_waf_config": {"filters": ["../", "dcatus/"]},
    }


@pytest.fixture
def dcatus_compare_config() -> dict:
    """example dcatus job payload"""
    return {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus_compare.json",
        "_extract_type": "datajson",
    }


@pytest.fixture
def ckan_compare() -> dict:
    return open_json(HARVEST_SOURCES / "dcatus" / "ckan_datasets_resp.json")["results"]
