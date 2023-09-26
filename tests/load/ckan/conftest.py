import pytest
from harvester.load.ckan import create_ckan_entrypoint, dcatus_to_ckan
import os
import requests


@pytest.fixture
def ckan_entrypoint():
    catalog_dev_api_key = os.getenv("CKAN_API_TOKEN_DEV")  # gha
    if catalog_dev_api_key is None:  # local
        import credentials

        catalog_dev_api_key = credentials.ckan_catalog_dev_api_key

    return create_ckan_entrypoint("https://catalog-dev.data.gov/", catalog_dev_api_key)


@pytest.fixture
def test_dcatus_catalog_url():
    return (
        "https://catalog.data.gov/harvest/object/cb22fea9-0c90-43e9-94bf-903eacd37c92"
    )


@pytest.fixture
def test_ckan_package_id():
    return "e875348b-a7c3-47eb-b0c3-168d978b0c0f"


@pytest.fixture
def test_dcatus_catalog(test_dcatus_catalog_url):
    return requests.get(test_dcatus_catalog_url).json()


@pytest.fixture
def test_ckan_package(test_ckan_package_id, test_dcatus_catalog):
    ckan_dataset = dcatus_to_ckan(test_dcatus_catalog)
    ckan_dataset["id"] = test_ckan_package_id
    return ckan_dataset


@pytest.fixture
def test_ckan_update_package(test_ckan_package_id):
    return {"author": "test author", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_patch_package(test_ckan_package_id):
    return {"author_email": "test@gmail.com", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_purge_package(test_ckan_package_id):
    return {"id": test_ckan_package_id}
