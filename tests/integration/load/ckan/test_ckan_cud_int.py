from harvester.load import (create_ckan_package, patch_ckan_package,
                            purge_ckan_package, update_ckan_package)


def test_create_package(ckan_entrypoint, test_ckan_package):
    try:
        # ckan complains when you try to purge something that doesn't exist
        purge_ckan_package(ckan_entrypoint, {"id": test_ckan_package["id"]})
    except:  # noqa E722
        pass
    assert (
        create_ckan_package(ckan_entrypoint, test_ckan_package)["title"]
        == test_ckan_package["title"]
    )


def test_update_package(ckan_entrypoint, test_ckan_update_package):
    assert (
        update_ckan_package(ckan_entrypoint, test_ckan_update_package)["author"]
        == test_ckan_update_package["author"]
    )


def test_patch_package(ckan_entrypoint, test_ckan_patch_package):
    assert (
        patch_ckan_package(ckan_entrypoint, test_ckan_patch_package)["author_email"]
        == test_ckan_patch_package["author_email"]
    )


def test_delete_package(ckan_entrypoint, test_ckan_purge_package):
    # ckan doesn't return anything when you purge
    assert purge_ckan_package(ckan_entrypoint, test_ckan_purge_package) is None
