from harvester.load.ckan import (
    create_ckan_package,
    purge_ckan_package,
    patch_ckan_package,
    update_ckan_package,
    dcatus_to_ckan,
)
from deepdiff import DeepDiff


def test_dcatus_to_ckan_transform(test_ckan_package_id, test_dcatus_catalog):
    expected_result = {
        "name": "fdic-failed-bank-list",
        "owner_org": "test",
        "maintainer": "FDIC Public Data Feedback",
        "maintainer_email": "FDICPublicDataFeedback@fdic.gov",
        "notes": "The FDIC is often appointed as receiver for failed banks. This list includes banks which have failed since October 1, 2000.",
        "title": "FDIC Failed Bank List",
        "resources": [
            {
                "url": "https://www.fdic.gov/bank/individual/failed/banklist.csv",
                "mimetype": "text/csv",
            },
            {
                "url": "https://www.fdic.gov/bank/individual/failed/index.html",
                "mimetype": "text/html",
            },
        ],
        "tags": [
            {"name": "financial-institution"},
            {"name": "banks"},
            {"name": "failures"},
            {"name": "assistance-transactions"},
        ],
        "extras": [
            {
                "key": "publisher_hierarchy",
                "value": "U.S. Government > Federal Deposit Insurance Corporation > Division of Insurance and Research",
            },
            {"key": "resource-type", "value": "Dataset"},
            {"key": "publisher", "value": "Division of Insurance and Research"},
            {"key": "accessLevel", "value": "public"},
            {"key": "bureauCode", "value": ["357:20"]},
            {
                "key": "identifier",
                "value": "https://www.fdic.gov/bank/individual/failed/",
            },
            {"key": "modified", "value": "R/P1W"},
            {"key": "programCode", "value": ["000:000"]},
            {"key": "publisher", "value": "Division of Insurance and Research"},
            {
                "key": "publisher_hierarchy",
                "value": "U.S. Government > Federal Deposit Insurance Corporation > Division of Insurance and Research",
            },
            {"key": "resource-type", "value": "Dataset"},
            {"key": "publisher", "value": "Division of Insurance and Research"},
        ],
        "author": None,
        "author_email": None,
    }

    assert DeepDiff(dcatus_to_ckan(test_dcatus_catalog), expected_result) == {}


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
