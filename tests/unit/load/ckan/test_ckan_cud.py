from unittest.mock import patch

import harvester


@patch("harvester.create_ckan_package")
def test_create_package(mock_create_ckan_package, ckan_entrypoint, test_ckan_package):
    mock_create_ckan_package.return_value = test_ckan_package.copy()
    assert (
        harvester.create_ckan_package(ckan_entrypoint, test_ckan_package)["title"]
        == test_ckan_package["title"]
    )


@patch("harvester.update_ckan_package")
def test_update_package(
    mock_update_ckan_package, ckan_entrypoint, test_ckan_update_package
):
    mock_update_ckan_package.return_value = test_ckan_update_package.copy()
    assert (
        harvester.update_ckan_package(ckan_entrypoint, test_ckan_update_package)[
            "author"
        ]
        == test_ckan_update_package["author"]
    )


@patch("harvester.patch_ckan_package")
def test_patch_package(
    mock_patch_ckan_package, ckan_entrypoint, test_ckan_patch_package
):
    mock_patch_ckan_package.return_value = test_ckan_patch_package.copy()
    assert (
        harvester.patch_ckan_package(ckan_entrypoint, test_ckan_patch_package)[
            "author_email"
        ]
        == test_ckan_patch_package["author_email"]
    )


@patch("harvester.purge_ckan_package")
def test_delete_package(
    mock_purge_ckan_package, ckan_entrypoint, test_ckan_purge_package
):
    mock_purge_ckan_package.return_value = None
    # ckan doesn't return anything when you purge
    assert (
        harvester.purge_ckan_package(ckan_entrypoint, test_ckan_purge_package) is None
    )
