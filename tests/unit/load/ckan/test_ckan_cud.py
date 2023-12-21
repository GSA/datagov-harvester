from deepdiff import DeepDiff
import harvester
from unittest.mock import patch


def test_dcatus_to_ckan_transform(test_dcatus_catalog):
    expected_result = {
        "name": "fdic-failed-bank-list",
        "owner_org": "test",
        "maintainer": "FDIC Public Data Feedback",
        "maintainer_email": "FDICPublicDataFeedback@fdic.gov",
        "notes": "The FDIC is often appointed as receiver for failed banks. This list includes banks which have failed since October 1, 2000.",  # noqa E501
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
                "value": "U.S. Government > Federal Deposit Insurance Corporation > Division of Insurance and Research",  # noqa E501
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
                "value": "U.S. Government > Federal Deposit Insurance Corporation > Division of Insurance and Research",  # noqa E501
            },
            {"key": "resource-type", "value": "Dataset"},
            {"key": "publisher", "value": "Division of Insurance and Research"},
        ],
        "author": None,
        "author_email": None,
    }

    assert (
        DeepDiff(harvester.dcatus_to_ckan(test_dcatus_catalog), expected_result) == {}
    )


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
