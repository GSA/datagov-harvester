from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from app.commands.evaluate_sources import (
    CATALOG_SOURCE_URL,
    HEADERS,
    evaluate_sources,
    get_sources,
    handled_request,
)

CATALOG_SUCCESS_JSON = {
    "help": "https://catalog.data.gov/api/3/action/help_show?name=package_search",
    "success": True,
    "result": {
        "count": 910,
        "facets": {},
        "results": [
            {
                "config": '{"private_datasets": "False"}',
                "creator_user_id": "a4aca618-886b-4597-8af0-96dfbf874935",
                "frequency": "DAILY",
                "id": "f1a1d3f5-1041-4489-b24f-82e225c3a204",
                "metadata_created": "2020-11-10T18:34:39.703317",
                "metadata_modified": "2020-11-10T22:43:52.481062",
                "name": "sec-data-json",
                "notes": "Open data from SEC",
                "organization": {
                    "id": "13e5b106-8f82-4b16-a11e-2f48f1f24238",
                    "name": "sec-gov",
                    "title": "Securities and Exchange Commission",
                    "type": "organization",
                    "description": "",
                    "image_url": "https://www.sec.gov/files/sec-logo.png",
                    "created": "2020-11-10T18:34:38.629097",
                    "is_organization": True,
                    "approval_status": "approved",
                    "state": "active",
                },
                "owner_org": "13e5b106-8f82-4b16-a11e-2f48f1f24238",
                "private": False,
                "private_datasets": "False",
                "source_type": "datajson",
                "state": "active",
                "title": "SEC data.json",
                "tracking_summary": {"total": 1552, "recent": 33},
                "type": "harvest",
                "url": "https://www.sec.gov/data.json",
                "resources": [],
                "tags": [],
                "groups": [],
                "relationships_as_subject": [],
                "relationships_as_object": [],
            },
            {
                "config": '{"private_datasets": "False"}',
                "creator_user_id": "a4aca618-886b-4597-8af0-96dfbf874935",
                "frequency": "DAILY",
                "id": "04b59eaf-ae53-4066-93db-80f2ed0df446",
                "metadata_created": "2020-11-10T17:33:30.574802",
                "metadata_modified": "2020-11-10T21:53:23.615213",
                "name": "epa-sciencehub",
                "notes": "Office of Research and Development Data Catalog",
                "organization": {
                    "id": "82b85475-f85d-404a-b95b-89d1a42e9f6b",
                    "name": "epa-gov",
                    "title": "U.S. Environmental Protection Agency",
                    "type": "organization",
                    "description": (
                        "Our mission is to protect human health"
                        " and the environment. "
                    ),
                    "image_url": "https://edg.epa.gov/EPALogo.svg",
                    "created": "2020-11-10T15:10:42.298896",
                    "is_organization": True,
                    "approval_status": "approved",
                    "state": "active",
                },
                "owner_org": "82b85475-f85d-404a-b95b-89d1a42e9f6b",
                "private": False,
                "private_datasets": "False",
                "source_type": "datajson",
                "state": "active",
                "title": "EPA ScienceHub",
                "tracking_summary": {"total": 1889, "recent": 29},
                "type": "harvest",
                "url": "https://pasteur.epa.gov/metadata.json",
                "resources": [],
                "tags": [],
                "groups": [],
                "relationships_as_subject": [],
                "relationships_as_object": [],
            },
            {
                "creator_user_id": "a4aca618-886b-4597-8af0-96dfbf874935",
                "frequency": "WEEKLY",
                "id": "d2f87ec5-57e6-4475-ab96-ac80c64b3909",
                "metadata_created": "2020-11-10T18:42:43.936596",
                "metadata_modified": "2021-06-03T13:56:39.958399",
                "name": "omb",
                "notes": "OMB data.json",
                "organization": {
                    "id": "0349e8bf-a476-4e31-b97e-cbcab1dec4cb",
                    "name": "office-of-management-and-budget",
                    "title": "Office of Management and Budget",
                    "type": "organization",
                    "description": (
                        "The Office of Management and Budget (OMB)"
                        " serves the President of the United States "
                    ),
                    "image_url": "https://www.whitehouse.gov/wp-content/uploads/2017/08/eop-omb.png",
                    "created": "2020-11-10T18:42:42.653068",
                    "is_organization": True,
                    "approval_status": "approved",
                    "state": "active",
                },
                "owner_org": "0349e8bf-a476-4e31-b97e-cbcab1dec4cb",
                "private": False,
                "source_type": "datajson",
                "state": "active",
                "title": "OMB",
                "tracking_summary": {"total": 147, "recent": 26},
                "type": "harvest",
                "url": "https://max.gov/data.json",
                "resources": [],
                "tags": [],
                "groups": [],
                "relationships_as_subject": [],
                "relationships_as_object": [],
            },
        ],
        "sort": "views_recent desc",
        "search_facets": {},
    },
}


@pytest.fixture
def mock_csv_writer():
    # Patch open
    open_patch = patch("builtins.open", mock_open())

    # Patch csv.DictWriter
    dictwriter_patch = patch("csv.DictWriter")

    # Start patches
    mock_file = open_patch.start()
    mock_writer_class = dictwriter_patch.start()

    # Mock instance returned by csv.DictWriter()
    mock_writer_instance = MagicMock()
    mock_writer_class.return_value = mock_writer_instance

    yield {
        "mock_file": mock_file,
        "mock_writer_class": mock_writer_class,
        "mock_writer_instance": mock_writer_instance,
    }
    # Stop patches
    open_patch.stop()
    dictwriter_patch.stop()


@pytest.fixture
def mock_catalog_request():
    def _mock_catalog_request(mock_json, status_code=200, ok=True):
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_json
            mock_response.status_code = status_code
            mock_response.ok = ok
            mock_response.headers = {
                "Content-Type": "application/json",
                "Server": "nginx/1.18.0",
            }
            mock_get.return_value = mock_response
            yield {"mock_get": mock_get, "mock_response": mock_response}

    return _mock_catalog_request


@pytest.fixture
def mock_catalog_request_success():
    with patch("requests.get") as mock_get:
        # Create a mock response object
        mock_response = Mock()

        mock_response.json.return_value = CATALOG_SUCCESS_JSON

        # Make sure .status_code, .ok or other attributes are present if needed
        mock_response.status_code = 200
        mock_response.ok = True

        # The patched requests.get will return this mock_response
        mock_get.return_value = mock_response

        yield mock_get


@pytest.fixture
def mock_eval_requests_success():
    with patch("requests.get") as mock_get:
        # Mock the catalog response
        mock_catalog_response = Mock()
        mock_catalog_response.json.return_value = CATALOG_SUCCESS_JSON
        mock_catalog_response.status_code = 200
        mock_catalog_response.ok = True
        mock_catalog_response.headers = {
            "Content-Type": "application/json",
            "Server": "nginx/1.18.0",
        }

        # Mock the request to the source
        mock_source_response = Mock()
        mock_source_response.json.return_value = {
            "dataset": [{"modified": "2024-05-01T12:05:26Z"}]
        }
        mock_source_response.status_code = 200
        mock_source_response.ok = True
        mock_source_response.headers = {
            "Content-Type": "application/json",
            "Server": "nginx/1.18.0",
        }

        mock_get.side_effect = [
            mock_catalog_response,
            mock_source_response,
            mock_source_response,
            mock_source_response,
        ]
        yield mock_get


class TestHandledRequest:
    """
    Test to see that the handled_request returns a response
    when it's able to reach the server.
    """

    def test_handled_request_success(self, mock_catalog_request_success):
        """
        Test the success of handled_request
        """
        response = handled_request(CATALOG_SOURCE_URL, HEADERS)
        data = response.json()
        assert response.ok
        assert response.status_code == 200
        assert len(data["result"]["results"]) == 3


class TestGetSources:
    """
    Tests for get_sources, to confirm that it retuns the list of source
    urls needed for evaluation.
    """

    def test_get_sources_success(self, mock_catalog_request_success):
        # supported source_types
        valid_source_types = ["datajson", "waf", "waf-collection"]
        # list of sources once parsed
        results = [
            {
                "source_id": "f1a1d3f5-1041-4489-b24f-82e225c3a204",
                "source_title": "SEC data.json",
                "source_type": "datajson",
                "source_url": "https://www.sec.gov/data.json",
                "organization_id": "13e5b106-8f82-4b16-a11e-2f48f1f24238",
                "organization_title": "Securities and Exchange Commission",
                "organization_state": "active",
            },
            {
                "source_id": "04b59eaf-ae53-4066-93db-80f2ed0df446",
                "source_title": "EPA ScienceHub",
                "source_type": "datajson",
                "source_url": "https://pasteur.epa.gov/metadata.json",
                "organization_id": "82b85475-f85d-404a-b95b-89d1a42e9f6b",
                "organization_title": "U.S. Environmental Protection Agency",
                "organization_state": "active",
            },
            {
                "source_id": "d2f87ec5-57e6-4475-ab96-ac80c64b3909",
                "source_title": "OMB",
                "source_type": "datajson",
                "source_url": "https://max.gov/data.json",
                "organization_id": "0349e8bf-a476-4e31-b97e-cbcab1dec4cb",
                "organization_title": "Office of Management and Budget",
                "organization_state": "active",
            },
        ]
        sources = get_sources()
        #  test for correct length
        assert len(sources) == 3
        # confirm that the source types are supported
        for source in sources:
            assert source["source_type"] in valid_source_types
        # check that the lists are the same
        assert sources == results


class TestEvaluateSources:
    """
    Tests for the evaluate_sources command.
    """

    def test_evaluate_sources_success(
        self, mock_csv_writer, mock_eval_requests_success
    ):
        """
        Test the success of evaluate_sources
        """
        # Mock the CSV writer
        mock_writer_instance = mock_csv_writer["mock_writer_instance"]

        # Call the function to test
        evaluate_sources()

        # check to see that the `writerow` method was called with the correct
        # arguments
        assert mock_writer_instance.writerow.call_count == 3
        # confirm that the call was made with the correct parsing of the values
        assert mock_writer_instance.writerow.call_args.args[0] == {
            "source_id": "d2f87ec5-57e6-4475-ab96-ac80c64b3909",
            "source_title": "OMB",
            "source_type": "datajson",
            "source_url": "https://max.gov/data.json",
            "organization_id": "0349e8bf-a476-4e31-b97e-cbcab1dec4cb",
            "organization_title": "Office of Management and Budget",
            "organization_state": "active",
            "status_code": 200,
            "metadata_types": {"DCAT-US"},
            "last_modified": "2024-05-01T12:05:26Z",
            "server_type": "nginx",
        }
