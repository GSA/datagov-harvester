import pytest
from harvester.load.ckan import create_ckan_entrypoint
import os


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
def test_ckan_package(test_ckan_package_id):
    return {
        "name": "test_package_name",
        "title": "title_test_name",
        "owner_org": "test",
        "tag_string": "test",
        "id": test_ckan_package_id,
    }


@pytest.fixture
def test_ckan_update_package(test_ckan_package_id):
    return {"author": "test author", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_patch_package(test_ckan_package_id):
    return {"author_email": "test@gmail.com", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_purge_package(test_ckan_package_id):
    return {"id": test_ckan_package_id}
