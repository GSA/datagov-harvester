import pytest
from harvester.load.ckan import (
    create_ckan_package,
    delete_ckan_package,
    patch_ckan_package,
    update_ckan_package,
)


def test_create_package(ckan_entrypoint, test_ckan_package):
    try:
        create_ckan_package(ckan_entrypoint, test_ckan_package)
        assert True
    except:
        assert False


def test_update_package(ckan_entrypoint, test_ckan_update_package):
    try:
        update_ckan_package(ckan_entrypoint, test_ckan_update_package)
        assert True
    except:
        assert False


def test_patch_package(ckan_entrypoint, test_ckan_patch_package):
    try:
        patch_ckan_package(ckan_entrypoint, test_ckan_patch_package)
        assert True
    except:
        assert False


def test_delete_package(ckan_entrypoint, test_ckan_package_id):
    try:
        delete_ckan_package(ckan_entrypoint, {"id": test_ckan_package_id})
        assert True
    except:
        assert False
