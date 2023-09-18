import pytest
from harvester.load.ckan import create_ckan_entrypoint
import credentials


@pytest.fixture
def ckan_entrypoint():
    return create_ckan_entrypoint(
        "http://localhost:5000", credentials.ckan_catalog_dev_api_key
    )


@pytest.fixture
def test_ckan_package_id():
    return "e875348b-a7c3-47eb-b0c3-168d978b0c0f"


@pytest.fixture
def test_ckan_package(test_ckan_package_id):
    return {
        "name": "package_test",
        "title": "title_test",
        "owner_org": "test_org",
        "tag_string": "test",
        "id": test_ckan_package_id,
    }


@pytest.fixture
def test_ckan_update_package(test_ckan_package_id):
    return {"author": "test author", "id": test_ckan_package_id}


@pytest.fixture
def test_ckan_patch_package(test_ckan_package_id):
    return {"author_email": "test@gmail.com", "id": test_ckan_package_id}
