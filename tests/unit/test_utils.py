from datetime import datetime
import http
import json
import logging
import time
from unittest.mock import Mock, call, patch

from bs4 import BeautifulSoup
import pytest
import requests
from jsonschema import Draft202012Validator, FormatChecker
from requests.exceptions import ConnectionError

from database.models import HarvestSource
from harvester.utils.general_utils import (
    USER_AGENT,
    RetrySession,
    munge_spatial,
    munge_title_to_name,
    translate_spatial,
    translate_spatial_to_geojson,
    assemble_validation_errors,
    create_retry_session,
    download_file,
    dynamic_map_list_items_to_dict,
    find_indexes_for_duplicates,
    is_valid_uuid4,
    parse_args,
    prepare_transform_msg,
    process_job_complete_percentage,
    query_filter_builder,
    traverse_waf,
    validate_geojson,
    get_waf_datetimes,
)


class TestCKANUtils:
    """Some of these tests are copied from
    # https://github.com/ckan/ckan/blob/master/ckan/tests/lib/test_munge.py
    """

    @pytest.mark.parametrize(
        "original,expected",
        [
            ("unchanged", "unchanged"),
            ("some spaces  here    &here", "some-spaces-here-here"),
            ("s", "s_"),  # too short
            ("random:other%character&", "random-othercharacter"),
            ("u with umlaut \xfc", "u-with-umlaut-u"),
            ("reallylong" * 12, "reallylong" * 9),
            ("reallylong" * 12 + " - 2012", "reallylong" * 8 + "reall" + "-2012"),
            (
                "10cm - 50cm Near InfraRed (NI) Digital Aerial Photography (AfA142)",
                "10cm-50cm-near-infrared-ni-digital-aerial-photography-afa142",
            ),
        ],
    )
    def test_munge_title_to_name(self, original, expected):
        """Munge a list of names gives expected results."""
        munge = munge_title_to_name(original)
        assert munge == expected

    def test_munge_spatial(self):
        assert munge_spatial("1.0,2.0,3.5,5.5") == (
            '{"type": "Polygon", "coordinates": '
            "[[[1.0, 2.0], [1.0, 5.5], [3.5, 5.5], "
            "[3.5, 2.0], [1.0, 2.0]]]}"
        )

    def test_munge_spatial_duplicates(self):
        assert (
            munge_spatial("-92.109, 15.132, -92.109, 15.132")
            == '{"type": "Point", "coordinates": [-92.109, 15.132]}'
        )

    def test_translate_spatial_simple_bbox(self):
        assert translate_spatial("1.0,2.0,3.5,5.5") == (
            '{"type": "Polygon", "coordinates": '
            "[[[1.0, 2.0], [1.0, 5.5], [3.5, 5.5], "
            "[3.5, 2.0], [1.0, 2.0]]]}"
        )

    def test_translate_spatial_geojson_string(self):
        assert translate_spatial(
            '{"type": "Polygon", "coordinates": '
            "[[[1.0, 2.0], [3.5, 2.0], [3.5, 5.5], "
            "[1.0, 5.5], [1.0, 2.0]]]}"
        ) == (
            '{"type": "Polygon", "coordinates": [[[1.0, 2.0], '
            "[3.5, 2.0], [3.5, 5.5], [1.0, 5.5], [1.0, 2.0]]]}"
        )

    def test_translate_spatial_over_meridian_negative(self):
        assert translate_spatial(
            '{"type": "Polygon", "coordinates": '
            "[[[-190, 40], [-190, 50], [-170, 50], "
            "[-170, 40], [-190, 40]]]}"
        ) == (
            json.dumps(
                {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                [170.0, 40.0],
                                [180.0, 40.0],
                                [180.0, 50.0],
                                [170.0, 50.0],
                                [170.0, 40.0],
                            ],
                            [
                                [-180.0, 40.0],
                                [-180.0, 50.0],
                                [-170.0, 50.0],
                                [-170.0, 40.0],
                                [-180.0, 40.0],
                            ],
                        ]
                    ],
                }
            )
        )

    def test_translate_spatial_over_meridian_positive(self):
        # Expected value tested with https://geojsonlint.com/
        assert translate_spatial(
            '{"type": "Polygon", "coordinates": '
            "[[[190.0, 40.0], [190.0, 50.0], [170.0, 50.0], "
            "[170.0, 40.0], [190.0, 40.0]]]}"
        ) == (
            json.dumps(
                {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                [-170.0, 40.0],
                                [-170.0, 50.0],
                                [-180.0, 50.0],
                                [-180.0, 40.0],
                                [-170.0, 40.0],
                            ],
                            [
                                [180.0, 50.0],
                                [180.0, 40.0],
                                [170.0, 40.0],
                                [170.0, 50.0],
                                [180.0, 50.0],
                            ],
                        ]
                    ],
                }
            )
        )

    def test_translate_spatial_geojson_fix(self):
        assert translate_spatial(
            {
                "type": "Polygon",
                "coordinates": [
                    [[1.0, 2.0], [1.0, 5.5], [3.5, 5.5], [3.5, 2.0], [1.0, 2.0]]
                ],
            }
        ) == (
            '{"type": "Polygon", "coordinates": [[[1.0, 2.0], '
            "[3.5, 2.0], [3.5, 5.5], [1.0, 5.5], [1.0, 2.0]]]}"
        )

    def test_translate_spatial_point_geojson(self):
        assert translate_spatial('{"type": "Point", "coordinates": [-55.1, 37.2]}') == (
            '{"type": "Point", "coordinates": [-55.1, 37.2]}'
        )

    def test_translate_spatial_point_numbers(self):
        assert translate_spatial("-88.9718,36.52033") == (
            '{"type": "Point", "coordinates": [-88.9718, 36.52033]}'
        )

    def test_translate_spatial_input_unchanged(self):
        metadata = {
            "spatial": "1.0,2.0,3.5,5.5",
        }
        translate_spatial(metadata["spatial"])
        assert metadata["spatial"] == "1.0,2.0,3.5,5.5"

    def test_translate_spatial_to_geojson(self):
        geojson = translate_spatial_to_geojson("-88.9718,36.52033")
        assert geojson == {
            "type": "Point",
            "coordinates": [-88.9718, 36.52033],
        }


# Point example
# "{\"type\": \"Point\", \"coordinates\": [-87.08258, 24.9579]}"
class TestGeneralUtils:
    def test_get_waf_datetimes(self):
        """
        so far web servers either use 'td' elements or text within a 'pre' element
        to list the documents.
        """
        # census-5-digit-zip-code-tabulation-area-zcta5-national
        page_pre = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n<html>\n <head>\n  <title>Index of /data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/zcta510</title>\n </head>\n <body>\n<h1>Index of /data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/zcta510</h1>\n<pre>      <a href="?C=N;O=A">Name</a>                                                   <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr>      <a href="/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/">Parent Directory</a>                                                            -   \n      <a href="2013_zcta510.ea.iso.xml">2013_zcta510.ea.iso.xml</a>                                2015-09-22 08:40   16K  \n      <a href="tl_2013_us_zcta510.shp.iso.xml">tl_2013_us_zcta510.shp.iso.xml</a>                         2015-09-22 08:31   34K  \n<hr></pre>\n</body></html>\n'
        soup = BeautifulSoup(page_pre)
        datetimes = get_waf_datetimes(soup, 2)

        assert datetimes == [datetime(2015, 9, 22, 8, 40), datetime(2015, 9, 22, 8, 31)]

        # shortened noaa-esrl-psd
        page_td = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n<html>\n <head>\n  <title>Index of /waf/NOAA/oar/esrl/psd/iso/xml</title>\n </head>\n <body>\n<h1>Index of /waf/NOAA/oar/esrl/psd/iso/xml</h1>\n  <table>\n   <tr><th valign="top"><img src="/icons/blank.gif" alt="[ICO]"></th><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>\n   <tr><th colspan="5"><hr></th></tr>\n<tr><td valign="top"><img src="/icons/text.gif" alt="[TXT]"></td><td><a href="COBE-SST2_Sea_Surface_Temperature_and_Ice.xml">COBE-SST2_Sea_Surface_Temperature_and_Ice.xml</a></td><td align="right">2025-01-01 09:15  </td><td align="right"> 13K</td><td>&nbsp;</td></tr>\n<tr><td valign="top"><img src="/icons/text.gif" alt="[TXT]"></td><td><a href="COBE_Sea_Surface_Temperature.xml">COBE_Sea_Surface_Temperature.xml</a></td><td align="right">2025-01-01 09:15  </td><td align="right"> 12K</td><td>&nbsp;</td></tr>\n<tr><td valign="top"><img src="/icons/text.gif" alt="[TXT]"></td><td><a href="CPC_GLOBAL_PRCP_V1.0.xml">CPC_GLOBAL_PRCP_V1.0.xml</a></td><td align="right">2025-01-01 09:15  </td><td align="right"> 11K</td><td>&nbsp;</td></tr></body></html>'
        soup = BeautifulSoup(page_td)
        datetimes = get_waf_datetimes(soup, 2)

        assert datetimes == [
            datetime(2025, 1, 1, 9, 15),
            datetime(2025, 1, 1, 9, 15),
        ]

    def test_assemble_validation_messages(
        self, dol_distribution_json, dcatus_non_federal_schema
    ):
        # the amount of test cases for something like this is massive
        # because you can check every rule for every piece of data
        # so trying to reasonably cover our bases

        del dol_distribution_json["identifier"]  # missing required field at root
        dol_distribution_json["keyword"] = []  # empty array
        dol_distribution_json["distribution"][0]["title"] = ""  # empty string
        dol_distribution_json["distribution"][1] = bool  # wrong type
        dol_distribution_json["contactPoint"]["hasEmail"] = (
            "bad email"  # bad value based on regex
        )
        dol_distribution_json["accrualPeriodicity"] = (
            "No longer updated (dataset archived)"  # bad const value
        )
        dol_distribution_json["rights"] = "a" * 256  # max string length exceeded
        dol_distribution_json["distribution"][0]["@type"] = (
            "Distribution"  # not dcat:Distribution
        )

        validator = Draft202012Validator(
            dcatus_non_federal_schema, format_checker=FormatChecker()
        )

        # validation messages are stored in the db via repr which will include the
        # object type (e.g. '<ValidationError: "$, \'identifier\' is a required property">')
        # omitting that here for brevity.
        # ruff: noqa E501
        expected = [
            "$, 'identifier' is a required property",
            "$.rights, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' does not match any of the acceptable formats: max string length requirement, 'null'",
            "$.accrualPeriodicity, 'No longer updated (dataset archived)' does not match any of the acceptable formats: constant value 'irregular' was expected, '^R\\\\/P(?:\\\\d+(?:\\\\.\\\\d+)?Y)?(?:\\\\d+(?:\\\\.\\\\d+)?M)?(?:\\\\d+(?:\\\\.\\\\d+)?W)?(?:\\\\d+(?:\\\\.\\\\d+)?D)?(?:T(?:\\\\d+(?:\\\\.\\\\d+)?H)?(?:\\\\d+(?:\\\\.\\\\d+)?M)?(?:\\\\d+(?:\\\\.\\\\d+)?S)?)?$', 'null', '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",
            "$.contactPoint.hasEmail, 'bad email' does not match any of the acceptable formats: \"^mailto:[\\\\w\\\\_\\\\~\\\\!\\\\$\\\\&\\\\'\\\\(\\\\)\\\\*\\\\+\\\\,\\\\;\\\\=\\\\:.-]+@[\\\\w.-]+\\\\.[\\\\w.-]+?$\", '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",
            "$.distribution[0]['@type'], @type value does not match any of the acceptable formats: constant value 'dcat:Distribution' was expected",
            "$.distribution[0].title, '' does not match any of the acceptable formats: non-empty, 'null', '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",
            "$.distribution[1], 'bool' does not match any of the acceptable formats: 'object', 'string'",
            "$.keyword, [] does not match any of the acceptable formats: non-empty, 'string'",
        ]

        errors = assemble_validation_errors(
            validator.iter_errors(dol_distribution_json)
        )

        for i in range(len(errors)):
            assert errors[i].message == expected[i]

    def test_find_indexes_for_duplicates(self):
        data = [
            {"identifier": "a"},
            {"identifier": "a"},
            {"identifier": "a"},
            {"identifier": "b"},
            {"identifier": "b"},
            {"identifier": "c"},
        ]
        assert find_indexes_for_duplicates(data) == [4, 2, 1]

    def test_args_parsing(self):
        args = parse_args(["test-id", "test-type"])
        assert args.jobId == "test-id"
        assert args.jobType == "test-type"

    def test_facet_builder_empty(self):
        assert query_filter_builder(HarvestSource, "") == []

    def test_facet_builder_single(self):
        assert len(query_filter_builder(HarvestSource, "id eq 1")) == 1

    def test_facet_builder_notequal(self):
        assert len(query_filter_builder(HarvestSource, "url startswith_op http:")) == 1

    def test_facet_builder_multiple(self):
        assert (
            len(query_filter_builder(HarvestSource, "id eq 1,organization_id eq 2"))
            == 2
        )

    def test_facet_builder_exception(self):
        with pytest.raises(AttributeError):
            query_filter_builder(HarvestSource, "nonexistent eq 1")

    @pytest.mark.parametrize(
        "original,expected",
        [
            (
                {
                    "readerStructureMessages": ["WARNING", "INFO"],
                    "readerValidationMessages": ["ERROR", "INFO"],
                },
                "structure messages: WARNING \nvalidation messages: ERROR",
            ),
            (
                {
                    "readerStructureMessages": ["WARNING", "INFO"],
                    "readerValidationMessages": ["INFO"],
                },
                "structure messages: WARNING \nvalidation messages: ",
            ),
            (
                {
                    "readerStructureMessages": [],
                    "readerValidationMessages": ["ERROR"],
                },
                "structure messages:  \nvalidation messages: ERROR",
            ),
            (
                {
                    "readerStructureMessages": ["INFO"],
                    "readerValidationMessages": [],
                },
                "structure messages:  \nvalidation messages: ",
            ),
        ],
    )
    def test_prepare_mdt_messages(self, original, expected):
        assert prepare_transform_msg(original) == expected

    def test_validate_geojson(self, invalid_envelope_geojson, named_location_stoneham):
        assert validate_geojson(invalid_envelope_geojson) is False
        assert validate_geojson(named_location_stoneham) is not False

    def test_make_jobs_chart_data(self):
        jobs_data = [
            {
                "records_added": 1,
                "records_updated": 1,
                "records_deleted": 1,
                "records_errored": 1,
                "records_ignored": 1,
            },
            {
                "records_added": 2,
                "records_updated": 2,
                "records_deleted": 2,
                "records_errored": 2,
                "records_ignored": 2,
            },
            {
                "records_added": 3,
                "records_updated": 3,
                "records_deleted": 3,
                "records_errored": 3,
                "records_ignored": 3,
            },
        ]
        chart_data = dynamic_map_list_items_to_dict(
            jobs_data, ["records_added", "records_errored", "records_ignored"]
        )
        chart_data_fixture = {
            "records_added": [1, 2, 3],
            "records_errored": [1, 2, 3],
            "records_ignored": [1, 2, 3],
        }
        assert chart_data == chart_data_fixture

    @pytest.mark.parametrize(
        "job_data,result",
        [
            (
                {
                    "records_total": 11,
                    "records_added": 1,
                    "records_updated": 1,
                    "records_deleted": 1,
                    "records_errored": 1,
                    "records_ignored": 1,
                },
                "45%",
            ),
            (
                {
                    "records_added": 1,
                    "records_updated": 1,
                    "records_deleted": 1,
                    "records_errored": 1,
                    "records_ignored": 1,
                },
                "0%",  # no job["records_total"]
            ),
            (
                {
                    "records_total": 0,
                    "records_added": 1,
                    "records_updated": 1,
                    "records_deleted": 1,
                    "records_errored": 1,
                    "records_ignored": 1,
                },
                "0%",  # records_total == 0
            ),
        ],
    )
    def test_process_job_complete_percentage(self, job_data, result):
        assert process_job_complete_percentage(job_data) == result

    @pytest.mark.parametrize(
        "job_id,result",
        [
            [0, False],
            ["test", False],
            [{}, False],
            ["cfbff0d1-9375-5685-968c-48ce8b15ae17", False],  # v5
            ["bdbc3cb3-d6e1-45bf-95d2-d92deedf3edf", True],  # v4
            ["9073926b-929f-31c2-abc9-fad77ae3e8eb", False],  # v3
            ["87d46f9c-7792-11f0-b35b-621e4597c515", False],  # v1
            ["TRUE; DROP TABLE users;", False],  # invalid inputs
        ],
    )
    def test_is_valid_uuid4(self, job_id, result):
        assert is_valid_uuid4(job_id) == result

    @patch("harvester.utils.general_utils.requests.get")
    def test_download_file_user_agent(self, mock_get):
        """Test that download_file includes correct User-Agent header."""
        expected_result = {"test": "data"}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = expected_result
        mock_get.return_value = mock_response

        result = download_file("http://example.com/test.json", ".json")

        mock_get.assert_called_once_with(
            "http://example.com/test.json", headers={"User-Agent": USER_AGENT}
        )
        assert result == expected_result

    @patch("harvester.utils.general_utils.requests.get")
    def test_download_file_xml_user_agent(self, mock_get):
        """Test that download_file includes correct User-Agent header for XML files."""
        expected_result = "<xml>test</xml>"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = expected_result.encode(
            "utf-8"
        )  # Encode to bytes for mock
        mock_get.return_value = mock_response

        result = download_file("http://example.com/test.xml", ".xml")

        mock_get.assert_called_once_with(
            "http://example.com/test.xml", headers={"User-Agent": USER_AGENT}
        )
        assert result == expected_result

    @patch("harvester.utils.general_utils.requests.get")
    def test_download_file_connection_error(self, mock_get):
        mock_get.side_effect = ConnectionError(
            "Connection aborted.",
            http.client.RemoteDisconnected(
                "Remote end closed connection without response"
            ),
        )

        with pytest.raises(requests.exceptions.ConnectionError) as e:
            download_file("http://example.com/test.xml", ".xml")


class TestRetrySession:
    """Tests for RetrySession class."""

    def test_initialization_with_defaults(self):
        """Test initialization with default parameters."""
        session = RetrySession()

        assert session.status_forcelist == {404, 499, 500, 502}
        assert session.max_retries == 3
        assert session.backoff_factor == 4.0

    def test_initialization_with_custom_parameters(self):
        """Test initialization with custom parameters."""
        custom_codes = {500, 502, 503}
        session = RetrySession(
            status_forcelist=custom_codes, max_retries=5, backoff_factor=0.5
        )

        assert session.status_forcelist == custom_codes
        assert session.max_retries == 5
        assert session.backoff_factor == 0.5

    def test_user_agent_header_set(self):
        """Test that User-Agent header is set correctly on initialization."""
        session = RetrySession()

        assert "User-Agent" in session.headers
        assert session.headers["User-Agent"] == USER_AGENT

    @patch("harvester.utils.general_utils.requests.Session.request")
    def test_user_agent_in_requests(self, mock_request):
        """Test that User-Agent header is included in actual requests."""
        mock_response = Mock(status_code=200)
        mock_request.return_value = mock_response

        session = RetrySession()
        session.request("GET", "http://example.com")

        # Verify that the parent request method was called with the User-Agent header
        mock_request.assert_called_once_with("GET", "http://example.com")
        # The headers should be set on the session level
        assert "User-Agent" in session.headers
        assert session.headers["User-Agent"] == USER_AGENT

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_successful_request_no_retry(self, mock_sleep, mock_request, caplog):
        """Test successful request that doesn't need retry."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        session = RetrySession()

        with caplog.at_level(logging.INFO):
            response = session.request("GET", "http://example.com")

        assert response.status_code == 200
        assert mock_request.call_count == 1
        assert mock_sleep.call_count == 0
        assert "Making initial GET request to http://example.com" in caplog.text

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_retry_on_target_status_codes(self, mock_sleep, mock_request, caplog):
        """Test retry behavior on target status codes."""
        # First two calls return 500, third call returns 200
        responses = [
            Mock(status_code=500),
            Mock(status_code=500),
            Mock(status_code=200),
        ]
        mock_request.side_effect = responses

        session = RetrySession(max_retries=3, backoff_factor=0.1)

        # with caplog.at_level(logging.WARNING):
        response = session.request("GET", "http://example.com")

        assert response.status_code == 200
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2  # Two retries

        # Check backoff delays
        expected_delays = [(0.1 * (2**0)) - 1, (0.1 * (2**1)) - 1]  # [0.1, 0.2]
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays
        assert "Making initial GET request to http://example.com" in caplog.text
        assert "Attempt 1: Received status code 500" in caplog.text

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_max_retries_exhausted(self, mock_sleep, mock_request, caplog):
        """Test behavior when max retries are exhausted."""
        # All calls return 500
        mock_response = Mock(status_code=500)
        mock_request.return_value = mock_response

        session = RetrySession(max_retries=2, backoff_factor=0.1)

        with caplog.at_level(logging.ERROR):
            response = session.request("GET", "http://example.com")

        assert response.status_code == 500
        assert mock_request.call_count == 3  # Initial + 2 retries
        assert mock_sleep.call_count == 2
        assert "Final attempt: Still received status code 500" in caplog.text

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_exception_retry_behavior(self, mock_sleep, mock_request, caplog):
        """Test retry behavior on request exceptions."""
        # First two calls raise exception, third succeeds
        mock_response = Mock(status_code=200)
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            requests.exceptions.Timeout("Request timeout"),
            mock_response,
        ]

        session = RetrySession(max_retries=3, backoff_factor=0.1)

        with caplog.at_level(logging.ERROR):
            response = session.request("GET", "http://example.com")

        assert response.status_code == 200
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        assert "Connection failed" in caplog.text
        assert "Request timeout" in caplog.text

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_exception_max_retries_exhausted(self, mock_sleep, mock_request):
        """Test exception raised when max retries exhausted on exceptions."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Persistent connection error"
        )

        session = RetrySession(max_retries=2)

        with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
            session.request("GET", "http://example.com")

        assert "Persistent connection error" in str(exc_info.value)
        assert mock_request.call_count == 3  # Initial + 2 retries

    @patch("harvester.utils.general_utils.requests.Session.request")
    def test_different_http_methods(self, mock_request):
        """Test that different HTTP methods work correctly."""
        mock_response = Mock(status_code=200)
        mock_request.return_value = mock_response

        session = RetrySession()
        methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]

        for method in methods:
            response = session.request(method, "http://example.com")
            assert response.status_code == 200

        assert mock_request.call_count == len(methods)

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_non_status_forcelist(self, mock_sleep, mock_request):
        """Test that non-retry status codes don't trigger retries."""
        mock_response = Mock(status_code=403)  # Not in default retry codes
        mock_request.return_value = mock_response

        session = RetrySession()
        response = session.request("GET", "http://example.com")

        assert response.status_code == 403
        assert mock_request.call_count == 1
        assert mock_sleep.call_count == 0

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_custom_status_forcelist(self, mock_sleep, mock_request, caplog):
        """Test custom retry status codes."""
        # First call returns 418 (custom retry code), second succeeds
        responses = [Mock(status_code=418), Mock(status_code=200)]
        mock_request.side_effect = responses

        session = RetrySession(
            status_forcelist={418, 503}, max_retries=2, backoff_factor=0.1
        )

        response = session.request("GET", "http://example.com")

        assert response.status_code == 200
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        assert (
            "Making initial GET request to http://example.com"
            == caplog.records[0].message
        )
        assert (
            "Received status code 418 for GET http://example.com. Retrying..."
            == caplog.records[1].message
        )

    def test_backoff_factor_calculation(self):
        """
        Test that backoff factor is calculated correctly.
        """
        session = RetrySession(backoff_factor=2.0)

        with patch("harvester.utils.general_utils.time.sleep") as mock_sleep:
            with patch(
                "harvester.utils.general_utils.requests.Session.request"
            ) as mock_request:
                # All calls return 500 to trigger retries
                mock_request.return_value = Mock(status_code=500)

                session.request("GET", "http://example.com")

                # Check that sleep was called with correct backoff delays
                expected_delays = [
                    (2.0 * (2**0)) - 1,
                    (2.0 * (2**1)) - 1,
                    (2.0 * (2**2)) - 1,
                ]
                actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
                assert actual_delays == expected_delays

    @patch("harvester.utils.general_utils.requests.Session.request")
    @patch("harvester.utils.general_utils.time.sleep")
    def test_retries_disabled(self, mock_sleep, mock_request, caplog, monkeypatch):
        """Test that when HARVEST_RETRY_ON_ERROR is set to `false`, no retries occur."""
        monkeypatch.setenv("HARVEST_RETRY_ON_ERROR", "false")
        mock_response = Mock(status_code=500)
        mock_request.return_value = mock_response

        session = create_retry_session()

        with caplog.at_level(logging.ERROR):
            response = session.request("GET", "http://example.com")

        assert response.status_code == 500
        assert mock_request.call_count == 1
        assert "Final attempt: Still received status code 500" in caplog.text


class TestCreateRetrySession:
    """Test the create_retry_session factory function."""

    def test_create_retry_session_defaults(self):
        """Test create_retry_session returns correctly configured session."""
        session = create_retry_session()

        assert isinstance(session, RetrySession)
        assert session.max_retries == 3
        assert session.backoff_factor == 4.0
        assert session.status_forcelist == {404, 499, 500, 502}
        # Verify User-Agent header is set
        assert "User-Agent" in session.headers
        assert session.headers["User-Agent"] == USER_AGENT

    def test_create_retry_session_with_retries_disabled(self, monkeypatch):
        """Test create_retry_session with retries disabled still sets User-Agent."""
        monkeypatch.setenv("HARVEST_RETRY_ON_ERROR", "false")

        session = create_retry_session()

        assert isinstance(session, RetrySession)
        assert session.max_retries == 0
        # Verify User-Agent header is still set
        assert "User-Agent" in session.headers
        assert session.headers["User-Agent"] == USER_AGENT
