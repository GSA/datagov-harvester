from pathlib import Path
import pytest
import os
from harvester.load import dcatus_to_ckan, create_ckan_entrypoint
from harvester.utils.json import open_json

TEST_DIR = Path(__file__).parents[3]
HARVEST_SOURCES = TEST_DIR / "harvest-sources"


@pytest.fixture
def ckan_entrypoint():
    catalog_dev_api_key = os.getenv("CKAN_API_TOKEN_DEV")  # gha
    if catalog_dev_api_key is None:  # local
        import credentials

        catalog_dev_api_key = credentials.ckan_catalog_dev_api_key

    return create_ckan_entrypoint("https://catalog-dev.data.gov/", catalog_dev_api_key)


@pytest.fixture
def test_ckan_package_id():
    return "e875348b-a7c3-47eb-b0c3-168d978b0c0f"


@pytest.fixture
def test_dcatus_catalog():
    return open_json(HARVEST_SOURCES / "dcatus" / "dcatus_to_ckan.json")


@pytest.fixture
def test_ckan_package(test_ckan_package_id, test_dcatus_catalog):
    ckan_dataset = dcatus_to_ckan(test_dcatus_catalog, "test_harvest_source_name")
    ckan_dataset["id"] = test_ckan_package_id
    return ckan_dataset


@pytest.fixture
def test_ckan_update_package(test_ckan_package):
    return {**test_ckan_package, **{"author": "test author"}}


@pytest.fixture
def test_ckan_patch_package(test_ckan_package_id):
    return {"author_email": "test@gmail.com", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_purge_package(test_ckan_package_id):
    return {"id": test_ckan_package_id}
