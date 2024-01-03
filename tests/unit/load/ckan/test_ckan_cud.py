from unittest.mock import patch

from deepdiff import DeepDiff

import harvester


def test_dcatus_to_ckan_transform(test_dcatus_catalog):
    # ruff: noqa: E501
    expected_result = {
        "name": "fdic-failed-bank-list",
        "owner_org": "test",
        "identifier": "https://www.fdic.gov/bank/individual/failed/",
        "maintainer": "FDIC Public Data Feedback",
        "maintainer_email": "FDICPublicDataFeedback@fdic.gov",
        "notes": "The FDIC is often appointed as receiver for failed banks. This list includes banks which have failed since October 1, 2000.",
        "title": "FDIC Failed Bank List",
        "resources": [
            {"url": "https://www.fdic.gov/bank/individual/failed/banklist.csv"},
            {"url": "https://www.fdic.gov/bank/individual/failed/index.html"},
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
            {"key": "bureauCode", "value": "357:20"},
            {
                "key": "identifier",
                "value": "https://www.fdic.gov/bank/individual/failed/",
            },
            {"key": "modified", "value": "R/P1W"},
            {"key": "programCode", "value": "000:000"},
            {"key": "publisher", "value": "Division of Insurance and Research"},
            {
                "key": "publisher_hierarchy",
                "value": "U.S. Government > Federal Deposit Insurance Corporation > Division of Insurance and Research",
            },
            {"key": "resource-type", "value": "Dataset"},
            {"key": "publisher", "value": "Division of Insurance and Research"},
            {
                "key": "dcat_metadata",
                "value": "{'accessLevel': 'public', 'bureauCode': ['357:20'], 'contactPoint': {'fn': 'FDIC Public Data Feedback', 'hasEmail': 'mailto:FDICPublicDataFeedback@fdic.gov'}, 'description': 'The FDIC is often appointed as receiver for failed banks. This list includes banks which have failed since October 1, 2000.', 'distribution': [{'accessURL': 'https://www.fdic.gov/bank/individual/failed/index.html', 'mediaType': 'text/html'}, {'downloadURL': 'https://www.fdic.gov/bank/individual/failed/banklist.csv', 'mediaType': 'text/csv'}], 'identifier': 'https://www.fdic.gov/bank/individual/failed/', 'keyword': ['assistance transactions', 'banks', 'failures', 'financial institution'], 'modified': 'R/P1W', 'programCode': ['000:000'], 'publisher': {'name': 'Division of Insurance and Research', 'subOrganizationOf': {'name': 'Federal Deposit Insurance Corporation', 'subOrganizationOf': {'name': 'U.S. Government'}}}, 'title': 'FDIC Failed Bank List'}",
            },
            {"key": "harvest_source_name", "value": "example_harvest_source_name"},
        ],
        "author": None,
        "author_email": None,
    }

    assert (
        DeepDiff(
            harvester.dcatus_to_ckan(
                test_dcatus_catalog, "example_harvest_source_name"
            ),
            expected_result,
        )
        == {}
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
