import http
import itertools
import json
import logging
import time
from datetime import datetime
from unittest.mock import Mock, call, patch

import pytest
import requests
from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator, FormatChecker
from requests.exceptions import ConnectionError

from database.interface import HarvesterDBInterface
from database.models import HarvestSource
from harvester.utils import general_utils
from harvester.utils.general_utils import (
    DT_PLACEHOLDER,
    USER_AGENT,
    RetrySession,
    assemble_validation_errors,
    backfill_catalog_record_identifiers,
    build_dcatus3_validator,
    create_retry_session,
    describe_identifier_error,
    download_file,
    dynamic_map_list_items_to_dict,
    extract_dcatus3_catalog_datasets,
    extract_dcatus3_catalog_records,
    extract_dcatus3_catalog_services,
    extract_dcatus3_nested_datasets,
    find_indexes_for_duplicates,
    get_waf_datetimes,
    is_valid_uuid4,
    merge_dcatus3_datasets,
    munge_spatial,
    munge_title_to_name,
    normalize_dataset_identifier,
    parse_args,
    prepare_distributions,
    prepare_transform_msg,
    process_job_complete_percentage,
    sort_dataset,
    strip_dcatus3_catalog_objects,
    translate_spatial,
    translate_spatial_to_geojson,
    validate_geojson,
)
from harvester.utils.schema_paths import (
    DCATUS3_COMPLETE_EXAMPLE,
    DCATUS3_DEFINITIONS_DIR,
)

# Real DCAT-US 3.0 validator, used to reproduce assembler errors on the
# complete example.
DCATUS3_DATASET_VALIDATOR = build_dcatus3_validator(
    DCATUS3_DEFINITIONS_DIR,
    root_ref="https://resources.data.gov/dcat-us/3.0.0/definitions/dataset",
)


@pytest.fixture
def dcatus3_complete_example():
    with open(DCATUS3_COMPLETE_EXAMPLE) as f:
        return json.load(f)


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

    def test_munge_spatial_linestring(self):
        assert (
            munge_spatial("-90.09,27.155,-90.09,27.275")
            == '{"type": "LineString", "coordinates": [[-90.09, 27.155], [-90.09, 27.275]]}'
        )

    def test_munge_all_zero(self):
        assert munge_spatial("0,0,0,0") == '{"type": "Point", "coordinates": [0, 0]}'

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

        page_pre = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n<html>\n <head>\n  <title>Index of /data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/zcta510</title>\n </head>\n <body>\n<h1>Index of /data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/zcta510</h1>\n<pre>      <a href="?C=N;O=A">Name</a>                                                   <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr>      <a href="/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2013/">Parent Directory</a>                                                            -   \n      <a href="2013_zcta510.ea.iso.xml">2013_zcta510.ea.iso.xml</a>                                6/17/2021 12:20 PM    16K  \n      <a href="tl_2013_us_zcta510.shp.iso.xml">tl_2013_us_zcta510.shp.iso.xml</a>                         8/1/2025 11:24 AM    34K  \n<hr></pre>\n</body></html>\n'

        soup = BeautifulSoup(page_pre)
        datetimes = get_waf_datetimes(soup, 2)

        assert datetimes == [
            datetime(2021, 6, 17, 12, 20),
            datetime(2025, 8, 1, 11, 24),
        ]

    def test_default_waf_datetime_is_now(self):
        """Test that the default waf datetime is now / the time of program execution"""

        page_html = """<html><body><pre>
          <a href="file1.xml">file1.xml</a>   12K
          <a href="file2.xml">file2.xml</a>   12K
          </pre></body></html>"""

        soup = BeautifulSoup(page_html)
        datetimes = get_waf_datetimes(soup, 2)

        # assert that datetimes are not the old default
        assert datetimes[0] != datetime(1900, 1, 1, 0, 0)
        assert datetimes[1] != datetime(1900, 1, 1, 0, 0)

        # assert the new default
        assert datetimes[0] == DT_PLACEHOLDER
        assert datetimes[1] == DT_PLACEHOLDER

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
        dol_distribution_json["contactPoint"][
            "hasEmail"
        ] = "bad email"  # bad value based on regex
        dol_distribution_json["accrualPeriodicity"] = (
            "No longer updated (dataset archived)"  # bad const value
        )
        dol_distribution_json["rights"] = "a" * 256  # max string length exceeded
        dol_distribution_json["distribution"][0][
            "@type"
        ] = "Distribution"  # not dcat:Distribution

        validator = Draft202012Validator(
            dcatus_non_federal_schema, format_checker=FormatChecker()
        )

        # validation messages are stored in the db via repr which will include the
        # object type (e.g. '<ValidationError: "$, \'identifier\' is a required property">')
        # omitting that here for brevity.
        # backslashes are doubled here but not in the final validation record error message.
        # ruff: noqa E501
        expected = [
            "$, 'identifier' is a required property",
            "$.rights, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' does not match any of the acceptable formats: max string length of 255 characters, 'null'",
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

    def test_assemble_validation_messages_keyword_too_many_items(
        self, dol_distribution_json, dcatus_non_federal_schema
    ):
        dol_distribution_json["keyword"] = ["a"] * 1001
        validator = Draft202012Validator(
            dcatus_non_federal_schema, format_checker=FormatChecker()
        )
        errors = assemble_validation_errors(
            validator.iter_errors(dol_distribution_json)
        )
        keyword_error = next(e for e in errors if "$.keyword" in e.message)
        assert "max 1000 items" in keyword_error.message

    def test_assemble_validation_messages_type_error_list_value_is_reported(
        self, dcatus3_complete_example
    ):
        """A non-empty list that fails `type: ["null", "string"]` must be
        reported, not dropped."""
        dcatus3_complete_example["spatialResolutionInMeters"] = ["bad"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.spatialResolutionInMeters, array value does not match any "
            "of the acceptable formats: 'string'"
        )

    def test_assemble_validation_messages_type_error_dict_value_is_reported(
        self, dcatus3_complete_example
    ):
        """Same as the list case; name the offender as "object value", not a
        dict key."""
        dcatus3_complete_example["temporalResolution"] = {"a": 1}

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.temporalResolution, object value does not match any of the "
            "acceptable formats: 'string'"
        )

    def test_assemble_validation_messages_type_error_single_type_leaf_is_reported(
        self, dcatus3_complete_example
    ):
        """A leaf `type: string` error with no parent (nested `@type`) must
        still be reported."""
        dcatus3_complete_example["distribution"][0]["@type"] = {"a": 1}

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.distribution[0]['@type'], object value does not match any of "
            "the acceptable formats: 'string'"
        )

        dcatus3_complete_example["distribution"][0]["@type"] = ["a"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.distribution[0]['@type'], array value does not match any of "
            "the acceptable formats: 'string'"
        )

    def test_assemble_validation_messages_anyof_context_still_finds_specific_cause(
        self, dcatus3_complete_example
    ):
        """An anyOf failure should surface the specific cause, not the
        null-branch type error."""
        del dcatus3_complete_example["contactPoint"][0]["hasEmail"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert (
            errors[0].message == "$.contactPoint[0], 'hasEmail' is a required property"
        )

    def test_assemble_validation_messages_nested_container_type_error_under_anyof(
        self, dcatus3_complete_example
    ):
        """A leaf type error reached through anyOf (deeper json_path than its
        parent) must be reported."""
        dcatus3_complete_example["contactPoint"][0]["@type"] = ["Kind"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.contactPoint[0]['@type'], array value does not match any of "
            "the acceptable formats: 'string'"
        )

    def test_assemble_validation_messages_plain_leaf_type_error_is_unaffected(
        self, dcatus3_complete_example
    ):
        """A plain `type: string` leaf with no context is unchanged by the
        forced fallback."""
        dcatus3_complete_example["title"] = {"a": 1}

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.title, object value does not match any of the acceptable "
            "formats: 'string'"
        )

    def test_assemble_validation_messages_anyof_of_all_scalar_types_is_rescued(
        self, dcatus3_complete_example
    ):
        """When every anyOf alternative is scalar, report a vague type error
        rather than silence."""
        dcatus3_complete_example["accessRights"] = ["x"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.accessRights, array value does not match any of the "
            "acceptable formats: 'null', 'string'"
        )

    def test_assemble_validation_messages_anyof_of_all_scalar_types_is_rescued_dict(
        self, dcatus3_complete_example
    ):
        """Same all-scalar anyOf rescue, for a dict against `language`."""
        dcatus3_complete_example["language"] = {"a": 1}

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.language, object value does not match any of the acceptable "
            "formats: 'null', 'string', 'array'"
        )

    def test_assemble_validation_messages_anyof_of_all_scalar_types_is_rescued_date(
        self, dcatus3_complete_example
    ):
        """Same all-scalar anyOf rescue, for `created` (nested anyOf of date forms)."""
        dcatus3_complete_example["created"] = ["2025"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.created, array value does not match any of the acceptable "
            "formats: 'string'"
        )

    def test_assemble_validation_messages_anyof_of_all_scalar_types_is_rescued_nested(
        self, dcatus3_complete_example
    ):
        """Same all-scalar anyOf rescue, nested under `spatial[0].bbox`."""
        dcatus3_complete_example["spatial"][0]["bbox"] = ["x"]

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.spatial[0].bbox, array value does not match any of the "
            "acceptable formats: 'null', 'string', 'object'"
        )

    def test_assemble_validation_messages_maxitems_numeric_array_does_not_crash(
        self, dcatus3_complete_example
    ):
        """A numeric maxItems array must not crash message assembly; name it
        as "array value"."""
        dcatus3_complete_example["spatial"][0]["centroid"] = {
            "type": "Point",
            "coordinates": [-77, 38, 1],
        }

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.spatial[0].centroid.coordinates, array value does not match "
            "any of the acceptable formats: max 2 items"
        )

    def test_assemble_validation_messages_minitems_names_specific_cause(
        self, dcatus3_complete_example
    ):
        """A minItems violation must be reported at its own path, not as a
        parent anyOf type error."""
        dcatus3_complete_example["spatial"][0]["centroid"] = {
            "type": "Point",
            "coordinates": [-77],
        }

        errors = assemble_validation_errors(
            DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
        )

        assert len(errors) == 1
        assert errors[0].message == (
            "$.spatial[0].centroid.coordinates, array value does not match "
            "any of the acceptable formats: min 2 items"
        )

    def test_assemble_validation_messages_formats_the_result_once(
        self, dcatus3_complete_example
    ):
        """
        Formatting on every recursive return repeated the same work and made the
        assembler quadratic. (GSA/data.gov#6067)
        """
        dcatus3_complete_example["spatialResolutionInMeters"] = ["bad"]
        del dcatus3_complete_example["title"]

        with patch(
            "harvester.utils.general_utils.finalize_validation_messages",
            wraps=general_utils.finalize_validation_messages,
        ) as finalize:
            errors = assemble_validation_errors(
                DCATUS3_DATASET_VALIDATOR.iter_errors(dcatus3_complete_example)
            )

        # the input recurses through anyOf context, so this is >1 without the fix
        assert finalize.call_count == 1
        assert len(errors) == 2

    def test_assemble_validation_messages_scales_linearly_with_dataset_count(self):
        """
        A 3.0 catalog is assembled in one call, so the message dict grew with the
        dataset count and the per-error work grew with it, on a catalog well under
        the upload limit. 28s before the fix, 0.25s after; 2s leaves a slow CI
        runner room without letting a 10x regression through.
        """
        count = 4000
        catalog = {
            "@type": "Catalog",
            "title": "Assembler scaling",
            "description": "Every dataset is missing its identifier.",
            "dataset": [
                {
                    "@type": "Dataset",
                    "title": "Example Dataset",
                    "description": "A dataset with no identifier.",
                    "contactPoint": {
                        "fn": "Support",
                        "hasEmail": "mailto:support@example.gov",
                    },
                    "publisher": {"name": "Example Org"},
                }
                for _ in range(count)
            ],
        }
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR)
        validation_errors = list(validator.iter_errors(catalog))

        start = time.perf_counter()
        errors = assemble_validation_errors(iter(validation_errors))
        elapsed = time.perf_counter() - start

        # one "'identifier' is a required property" per dataset, all still found
        assert len(errors) == count
        assert elapsed < 2, f"assembling {count} errors took {elapsed:.1f}s"

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

    def test_find_indexes_for_duplicates_object_identifier(self):
        data = [
            {"identifier": "https://example.gov/datasets/one"},
            {
                "identifier": {
                    "@type": "Identifier",
                    "@id": "https://example.gov/datasets/one",
                }
            },
            {"identifier": "b"},
        ]
        assert find_indexes_for_duplicates(data) == [1]

    @pytest.mark.parametrize(
        "identifier,expected",
        [
            ("https://example.gov/datasets/one", "https://example.gov/datasets/one"),
            (
                {"@type": "Identifier", "@id": "https://example.gov/datasets/three"},
                "https://example.gov/datasets/three",
            ),
            (
                {"@type": "Identifier", "notation": "DS-003"},
                None,
            ),
            ({"@type": "Identifier"}, None),
            (None, None),
            ("", None),
            ("   ", None),
        ],
    )
    def test_normalize_dataset_identifier(self, identifier, expected):
        assert normalize_dataset_identifier(identifier) == expected

    @pytest.mark.parametrize(
        "identifier,expected",
        [
            (None, "is missing 'identifier' field"),
            ("", "is missing 'identifier' field"),
            (
                {"@type": "Identifier"},
                "has an object 'identifier' with no usable '@id' field",
            ),
            (123, "has an invalid 'identifier' field"),
        ],
    )
    def test_describe_identifier_error(self, identifier, expected):
        assert describe_identifier_error(identifier) == expected

    def test_args_parsing(self):
        args = parse_args(["test-id", "test-type"])
        assert args.jobId == "test-id"
        assert args.jobType == "test-type"

    def test_facet_builder_empty(self):
        assert HarvesterDBInterface.query_filter_builder(HarvestSource, "") == []

    def test_facet_builder_single(self):
        assert (
            len(HarvesterDBInterface.query_filter_builder(HarvestSource, "id eq 1"))
            == 1
        )

    def test_facet_builder_notequal(self):
        assert (
            len(
                HarvesterDBInterface.query_filter_builder(
                    HarvestSource, "url startswith_op http:"
                )
            )
            == 1
        )

    def test_facet_builder_multiple(self):
        assert (
            len(
                HarvesterDBInterface.query_filter_builder(
                    HarvestSource, "id eq 1,organization_id eq 2"
                )
            )
            == 2
        )

    def test_facet_builder_exception(self):
        with pytest.raises(AttributeError):
            HarvesterDBInterface.query_filter_builder(HarvestSource, "nonexistent eq 1")

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

    def test_prepare_distributions(self):
        simplified_dcatus_doc = {
            "distribution": [
                {
                    "@type": "dcat:Distribution",
                    "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
                },
                {
                    "@type": "dcat:Distribution",
                    "describedBy": "https://data.wa.gov/api/views/f6w7-q2d2/columns.json",
                    "describedByType": "application/json",
                    "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD",
                },
                {
                    "@type": "dcat:Distribution",
                    "description": "The TIGER/Line Shapefiles are the fully supported, core geographic product from the U.S. Census Bureau. They are extracts of selected geographic and cartographic information from the U.S. Census Bureau's Master Address File/Topologically Integrated Geographic Encoding and Referencing (MAF/TIGER) System.",
                    "downloadURL": "https://www2.census.gov/geo/tiger/TIGER2025/PLACE/tl_2025_42_place.zip",
                    "mediaType": "placeholder/value",
                    "title": "tl_2025_42_place.zip",
                },
                {
                    "@type": "dcat:Distribution",
                    "describedBy": "https://data.wa.gov/api/views/f6w7-q2d2/columns.rdf",
                    "describedByType": "application/rdf+xml",
                    "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.rdf?accessType=DOWNLOAD",
                    "mediaType": "placeholder/value",
                },
                {
                    "@type": "dcat:Distribution",
                    "description": "Entity and attribute file",
                    "downloadURL": "https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/Current_19110/tl_2025_place.shp.ea.iso.xml",
                    "mediaType": "placeholder/value",
                    "title": "tl_2025_place.shp.ea.iso.xml",
                },
                {
                    "@type": "dcat:Distribution",
                    "description": "The Open Geospatial Consortium, Inc. (OGC) Web Map Service interface standard (WMS) provides a simple HTTP interface for requesting geo-registered map images from our geospatial database. The response to the request is one or more geo-registered map images that can be displayed in a browser or WMS client application. By gaining access to our data through our WMS, users can produce maps containing TIGERweb layers combined with layers from other servers.",
                    "accessURL": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer",
                    "mediaType": "placeholder/value",
                    "title": "TIGERweb/tigerWMS_Current (MapServer)",
                },
            ]
        }

        prepared_dcatus_doc = prepare_distributions(simplified_dcatus_doc)

        expected = [
            "text/csv",
            "application/json",
            "application/zip",
            "application/rdf+xml",
            "application/xml",
            "arcgis_rest",
        ]

        for i in range(len(prepared_dcatus_doc["distribution"])):
            assert prepared_dcatus_doc["distribution"][i]["mediaType"] == expected[i]

        # the mediatype isn't in RESOURCE_MAPPING so format shouldn't exist
        "format" not in prepared_dcatus_doc["distribution"][-1]


class TestSortDataset:
    def test_sort_is_deterministic_regardless_of_key_and_list_order(self):
        a = {
            "identifier": "a",
            "keyword": ["b", "a"],
            "distribution": [{"title": "two"}, {"title": "one"}],
        }
        b = {
            "distribution": [{"title": "one"}, {"title": "two"}],
            "keyword": ["a", "b"],
            "identifier": "a",
        }

        # dict equality ignores key order, but json.dumps (what harvest.py
        # actually hashes) does not -- compare the serialized form so this
        # test would fail if dict keys weren't also being sorted.
        assert json.dumps(sort_dataset(a)) == json.dumps(sort_dataset(b))

    def test_sort_handles_nested_dict_values_that_cannot_be_ordered(self):
        """
        regression test for https://github.com/GSA/data.gov/issues/5450

        harvested records can carry vendor-specific fields (e.g. ArcGIS's
        "metadata" field) whose list elements are dicts sharing a first key
        with dict-valued, unequal values. python can't order dicts with
        `<`/`>`, which crashed the third-party sansjson library this
        function used to delegate to.
        """
        record = {
            "identifier": "https://www.arcgis.com/home/item.html?id=bd1b6ee9",
            "metadata": {
                "mdContact": {"rpCntInfo": {"cntAddress": {"city": "Washington"}}},
                "spatRepInfo": {"VectSpatRep": {"geometObjs": {"geoObjCnt": 5}}},
            },
            "fields": [
                {"name": "A", "domain": {"codedValues": [{"code": "US"}]}},
                {"name": "B", "domain": {"codedValues": [{"code": "CA"}]}},
            ],
        }

        sorted_record = sort_dataset(record)  # should not raise

        assert (
            sorted_record["metadata"]["mdContact"]["rpCntInfo"]["cntAddress"]["city"]
            == "Washington"
        )
        assert {f["name"] for f in sorted_record["fields"]} == {"A", "B"}

    def test_sort_orders_dict_keys_alphabetically(self):
        assert list(sort_dataset({"b": 1, "a": 2}).keys()) == ["a", "b"]

    def test_sort_recurses_into_dict_elements_of_a_list(self):
        record = {"distribution": [{"z": 1, "a": 2}]}

        assert list(sort_dataset(record)["distribution"][0].keys()) == ["a", "z"]

    def test_sort_orders_numeric_lists_by_value_not_json_string(self):
        # "10" sorts before "2" as a json/string value, but should not here
        assert sort_dataset([2, 10, 1]) == [1, 2, 10]

    def test_sort_orders_string_lists_naturally(self):
        # a naive `key=lambda i: json.dumps(i)` sorts "food safety" before
        # "food", because a quote (0x22) sorts after a space (0x20) --
        # breaking hash stability for the common "keyword"/"keyword extra"
        # pattern in harvested keyword lists.
        assert sort_dataset(["food safety", "food", "foodborne"]) == [
            "food",
            "food safety",
            "foodborne",
        ]

    def test_sort_does_not_reorder_a_linestring(self):
        """
        an array of arrays is positional geometry, not an unordered
        collection -- reordering it moves vertices. this is the depth-2
        shape `spatial.coordinates` takes for a GeoJSON LineString, which
        federal_dataset.json permits as "array of array of number".
        """
        line = [[10.0, 1.0], [2.0, 3.0], [-5.0, 4.0]]

        assert sort_dataset({"coordinates": line})["coordinates"] == line

    def test_sort_keeps_a_polygon_ring_closed(self):
        ring = [
            [-77.119759, 38.791645],
            [-76.909393, 38.791645],
            [-76.909393, 38.99538],
            [-77.119759, 38.99538],
            [-77.119759, 38.791645],
        ]

        sorted_ring = sort_dataset({"coordinates": [ring]})["coordinates"][0]

        assert sorted_ring == ring
        assert sorted_ring[0] == sorted_ring[-1], "ring must stay closed"

    def test_sort_canonicalizes_dict_keys_inside_a_nested_list(self):
        """
        preserving a positional list's element order must not stop dict keys
        nested inside it from being canonicalized -- otherwise that subtree
        hashes differently depending on source key order, defeating the
        point of the function.
        """
        a = sort_dataset({"x": [[{"z": 1, "a": 2}]]})
        b = sort_dataset({"x": [[{"a": 2, "z": 1}]]})

        assert json.dumps(a) == json.dumps(b)
        assert list(a["x"][0][0].keys()) == ["a", "z"]

    @pytest.mark.parametrize(
        "elements",
        [
            # `isinstance(True, int)` is True in python, so a bool must be
            # ranked before the numeric check -- otherwise True and 1 compare
            # equal, the sort is left to input order, and the same content
            # hashes two different ways.
            [True, 1],
            [False, 0],
            # a list mixing every json type must not attempt an unsupported
            # comparison between types, and must land in one stable order
            [None, 3, "a", True, 1.5, {"x": 1}, [1, 2]],
        ],
    )
    def test_sort_of_mixed_type_list_is_stable_across_input_orders(self, elements):
        outputs = {
            json.dumps(sort_dataset(list(permutation)))
            for permutation in itertools.permutations(elements)
        }

        assert len(outputs) == 1, f"ordering depends on input order: {outputs}"

    def test_canonical_form_is_pinned(self):
        """
        the absolute canonical form, not just its stability. every stored
        source_hash depends on it, so a change to the sort key or type ranks
        silently invalidates every hash in the database -- this pins the
        output so that change has to be deliberate.
        """
        record = {
            "identifier": "golden",
            "keyword": ["b", "a and more", "a"],
            "distribution": [{"title": "two", "x": 1}, {"title": "one"}],
            "spatial": {
                "type": "LineString",
                "coordinates": [[10.0, 1.0], [2.0, 3.0]],
            },
            # one of every json type, so the relative order of the type
            # ranks is pinned too, not just the values within a rank
            "mixed": [None, 3, "a", True, 1.5, {"k": 1}, [1, 2]],
        }

        assert json.dumps(sort_dataset(record)) == (
            '{"distribution": [{"title": "one"}, {"title": "two", "x": 1}], '
            '"identifier": "golden", '
            '"keyword": ["a", "a and more", "b"], '
            '"mixed": [null, true, 1.5, 3, "a", [1, 2], {"k": 1}], '
            '"spatial": {"coordinates": [[10.0, 1.0], [2.0, 3.0]], '
            '"type": "LineString"}}'
        )


class TestDcatus3Catalog:
    def test_strip_dcatus3_catalog_objects_removes_harvested_fields(self):
        catalog = {
            "@type": "Catalog",
            "title": "Test Catalog",
            "dataset": [{"identifier": "ds-1"}],
            "service": [{"identifier": "svc-1"}],
            "record": [{"identifier": "rec-1"}],
            "datasetSeries": [{"identifier": "series-1"}],
        }

        stripped = strip_dcatus3_catalog_objects(catalog)

        assert stripped == {"@type": "Catalog", "title": "Test Catalog"}
        # original is untouched
        assert "dataset" in catalog

    def test_strip_dcatus3_catalog_objects_recurses_into_nested_catalogs(self):
        catalog = {
            "title": "Parent Catalog",
            "dataset": [{"identifier": "parent-ds"}],
            "catalog": [
                {
                    "title": "Child Catalog",
                    "dataset": [{"identifier": "child-ds"}],
                    "catalog": [
                        {
                            "title": "Grandchild Catalog",
                            "dataset": [{"identifier": "grandchild-ds"}],
                        }
                    ],
                }
            ],
        }

        stripped = strip_dcatus3_catalog_objects(catalog)

        assert stripped == {
            "title": "Parent Catalog",
            "catalog": [
                {
                    "title": "Child Catalog",
                    "catalog": [{"title": "Grandchild Catalog"}],
                }
            ],
        }

    def test_extract_dcatus3_catalog_datasets_flat(self):
        catalog = {"dataset": [{"identifier": "ds-1"}, {"identifier": "ds-2"}]}

        assert extract_dcatus3_catalog_datasets(catalog) == [
            {"identifier": "ds-1"},
            {"identifier": "ds-2"},
        ]

    def test_extract_dcatus3_catalog_datasets_recurses_arbitrarily_deep(self):
        catalog = {
            "dataset": [{"identifier": "parent-ds"}],
            "catalog": [
                {
                    "dataset": [{"identifier": "child-ds"}],
                    "catalog": [
                        {"dataset": [{"identifier": "grandchild-ds"}]},
                    ],
                }
            ],
        }

        assert extract_dcatus3_catalog_datasets(catalog) == [
            {"identifier": "parent-ds"},
            {"identifier": "child-ds"},
            {"identifier": "grandchild-ds"},
        ]

    def test_extract_dcatus3_catalog_datasets_missing_fields(self):
        assert extract_dcatus3_catalog_datasets({}) == []
        assert extract_dcatus3_catalog_datasets({"catalog": None}) == []
        assert extract_dcatus3_catalog_datasets({"dataset": None}) == []

    def test_extract_dcatus3_catalog_services_flat(self):
        catalog = {"service": [{"identifier": "svc-1"}, {"identifier": "svc-2"}]}

        assert extract_dcatus3_catalog_services(catalog) == [
            {"identifier": "svc-1"},
            {"identifier": "svc-2"},
        ]

    def test_extract_dcatus3_catalog_services_recurses_arbitrarily_deep(self):
        catalog = {
            "service": [{"identifier": "parent-svc"}],
            "catalog": [
                {
                    "service": [{"identifier": "child-svc"}],
                    "catalog": [
                        {"service": [{"identifier": "grandchild-svc"}]},
                    ],
                }
            ],
        }

        assert extract_dcatus3_catalog_services(catalog) == [
            {"identifier": "parent-svc"},
            {"identifier": "child-svc"},
            {"identifier": "grandchild-svc"},
        ]

    def test_extract_dcatus3_catalog_services_missing_fields(self):
        assert extract_dcatus3_catalog_services({}) == []
        assert extract_dcatus3_catalog_services({"catalog": None}) == []
        assert extract_dcatus3_catalog_services({"service": None}) == []

    def test_extract_dcatus3_catalog_services_independent_of_datasets(self):
        """A catalog with both dataset and service arrays extracts each
        independently of the other."""
        catalog = {
            "dataset": [{"identifier": "ds-1"}],
            "service": [{"identifier": "svc-1"}],
        }

        assert extract_dcatus3_catalog_datasets(catalog) == [{"identifier": "ds-1"}]
        assert extract_dcatus3_catalog_services(catalog) == [{"identifier": "svc-1"}]

    def test_extract_dcatus3_catalog_records_flat(self):
        catalog = {"record": [{"@id": "rec-1"}, {"@id": "rec-2"}]}

        assert extract_dcatus3_catalog_records(catalog) == [
            {"@id": "rec-1"},
            {"@id": "rec-2"},
        ]

    def test_extract_dcatus3_catalog_records_recurses_arbitrarily_deep(self):
        catalog = {
            "record": [{"@id": "parent-rec"}],
            "catalog": [
                {
                    "record": [{"@id": "child-rec"}],
                    "catalog": [
                        {"record": [{"@id": "grandchild-rec"}]},
                    ],
                }
            ],
        }

        assert extract_dcatus3_catalog_records(catalog) == [
            {"@id": "parent-rec"},
            {"@id": "child-rec"},
            {"@id": "grandchild-rec"},
        ]

    def test_extract_dcatus3_catalog_records_missing_fields(self):
        assert extract_dcatus3_catalog_records({}) == []
        assert extract_dcatus3_catalog_records({"catalog": None}) == []
        assert extract_dcatus3_catalog_records({"record": None}) == []

    def test_extract_dcatus3_catalog_records_independent_of_datasets(self):
        """A catalog with both dataset and record arrays extracts each
        independently of the other."""
        catalog = {
            "dataset": [{"identifier": "ds-1"}],
            "record": [{"@id": "rec-1"}],
        }

        assert extract_dcatus3_catalog_datasets(catalog) == [{"identifier": "ds-1"}]
        assert extract_dcatus3_catalog_records(catalog) == [{"@id": "rec-1"}]


class TestBackfillCatalogRecordIdentifiers:
    def test_missing_id_gets_synthesized(self):
        records = [{"primaryTopic": "ds-1", "modified": "2024-06-15"}]

        result = backfill_catalog_record_identifiers(records)

        assert result[0]["@id"].startswith("urn:datagov:catalogrecord:")

    def test_existing_id_left_alone(self):
        records = [{"@id": "https://example.gov/rec-1", "primaryTopic": "ds-1"}]

        result = backfill_catalog_record_identifiers(records)

        assert result[0]["@id"] == "https://example.gov/rec-1"

    def test_synthesized_id_is_stable_for_same_content(self):
        records = [{"primaryTopic": "ds-1", "modified": "2024-06-15"}]

        first = backfill_catalog_record_identifiers(records)[0]["@id"]
        second = backfill_catalog_record_identifiers(records)[0]["@id"]

        assert first == second

    def test_synthesized_id_differs_for_different_content(self):
        a = backfill_catalog_record_identifiers(
            [{"primaryTopic": "ds-1", "modified": "2024-06-15"}]
        )[0]["@id"]
        b = backfill_catalog_record_identifiers(
            [{"primaryTopic": "ds-2", "modified": "2024-06-15"}]
        )[0]["@id"]

        assert a != b

    def test_does_not_mutate_input(self):
        records = [{"primaryTopic": "ds-1", "modified": "2024-06-15"}]

        backfill_catalog_record_identifiers(records)

        assert "@id" not in records[0]


class TestExtractDcatus3NestedDatasets:
    def test_uses_custom_parent_identifier_field(self):
        """DatasetSeries (like CatalogRecord) has no "identifier" field,
        only a top-level "@id"."""
        parents = [
            {
                "@id": "series-1",
                "seriesMember": [{"identifier": "ds-1"}],
            }
        ]

        result = extract_dcatus3_nested_datasets(
            parents, "seriesMember", parent_identifier_field="@id"
        )

        assert result == [{"identifier": "ds-1", "parent_identifier": "series-1"}]

    def test_extracts_single_field(self):
        parents = [
            {
                "identifier": "svc-1",
                "servesDataset": [
                    {"identifier": "ds-1"},
                    {"identifier": "ds-2"},
                ],
            }
        ]

        result = extract_dcatus3_nested_datasets(parents, "servesDataset")

        assert result == [
            {"identifier": "ds-1", "parent_identifier": "svc-1"},
            {"identifier": "ds-2", "parent_identifier": "svc-1"},
        ]

    def test_extracts_multiple_fields_including_singular_ones(self):
        """DatasetSeries has seriesMember (a list) plus first/last (single
        objects, not lists) -- all three should be pulled out."""
        parents = [
            {
                "identifier": "series-1",
                "seriesMember": [{"identifier": "ds-2"}],
                "first": {"identifier": "ds-1"},
                "last": {"identifier": "ds-3"},
            }
        ]

        result = extract_dcatus3_nested_datasets(
            parents, "seriesMember", "first", "last"
        )

        assert {d["identifier"] for d in result} == {"ds-1", "ds-2", "ds-3"}
        assert all(d["parent_identifier"] == "series-1" for d in result)

    def test_missing_fields_produce_nothing(self):
        parents = [{"identifier": "svc-1"}]

        assert extract_dcatus3_nested_datasets(parents, "servesDataset") == []

    def test_multiple_parents_each_tagged_with_their_own_identifier(self):
        parents = [
            {"identifier": "svc-1", "servesDataset": [{"identifier": "ds-1"}]},
            {"identifier": "svc-2", "servesDataset": [{"identifier": "ds-2"}]},
        ]

        result = extract_dcatus3_nested_datasets(parents, "servesDataset")

        assert result == [
            {"identifier": "ds-1", "parent_identifier": "svc-1"},
            {"identifier": "ds-2", "parent_identifier": "svc-2"},
        ]

    def test_does_not_mutate_original_dataset_dicts(self):
        original_dataset = {"identifier": "ds-1"}
        parents = [{"identifier": "svc-1", "servesDataset": [original_dataset]}]

        extract_dcatus3_nested_datasets(parents, "servesDataset")

        assert "parent_identifier" not in original_dataset

    def test_dedupes_same_identifier_across_fields_on_one_parent(self):
        """A DatasetSeries's "first"/"last" are typically also present in
        "seriesMember" -- that's redundant source data, not a
        duplicate-identifier error, so only one copy should survive per
        parent."""
        parents = [
            {
                "identifier": "series-1",
                "seriesMember": [
                    {"identifier": "ds-1", "title": "First title seen"},
                    {"identifier": "ds-2"},
                ],
                "first": {
                    "identifier": "ds-1",
                    "title": "Redundant, should be dropped",
                },
            }
        ]

        result = extract_dcatus3_nested_datasets(
            parents, "seriesMember", "first", "last"
        )

        assert [d["identifier"] for d in result] == ["ds-1", "ds-2"]
        assert result[0]["title"] == "First title seen"

    def test_does_not_dedupe_missing_identifiers_across_datasets(self):
        """Datasets with no usable identifier must each be kept (and later
        flagged individually as missing an identifier), not collapsed into
        one just because they all normalize to None."""
        parents = [
            {
                "identifier": "series-1",
                "seriesMember": [{"title": "No id one"}, {"title": "No id two"}],
            }
        ]

        result = extract_dcatus3_nested_datasets(parents, "seriesMember")

        assert len(result) == 2

    def test_same_identifier_across_different_parents_not_deduped(self):
        """Overlapping identifiers across different parents are a real
        cross-source ambiguity, not the same kind of intra-parent
        redundancy, so both copies should be kept for the normal
        duplicate-identifier filter to catch."""
        parents = [
            {"identifier": "svc-1", "servesDataset": [{"identifier": "ds-1"}]},
            {"identifier": "svc-2", "servesDataset": [{"identifier": "ds-1"}]},
        ]

        result = extract_dcatus3_nested_datasets(parents, "servesDataset")

        assert len(result) == 2
        assert {d["parent_identifier"] for d in result} == {"svc-1", "svc-2"}


class TestMergeDcatus3Datasets:
    def test_top_level_and_nested_overlap_merged_into_one(self):
        """Top-level entry wins, gains the nested entry's parent_identifier."""
        top_level = [{"identifier": "ds-1", "title": "Canonical title"}]
        nested = [
            {
                "identifier": "ds-1",
                "title": "Redundant series-member copy",
                "parent_identifier": "series-1",
            }
        ]

        result = merge_dcatus3_datasets(top_level, nested)

        assert len(result) == 1
        assert result[0]["title"] == "Canonical title"
        assert result[0]["parent_identifier"] == "series-1"

    def test_nested_only_dataset_kept_as_its_own_record(self):
        """A nested dataset with no top-level counterpart is unaffected."""
        nested = [{"identifier": "ds-2", "parent_identifier": "series-1"}]

        result = merge_dcatus3_datasets([], nested)

        assert result == nested

    def test_disjoint_top_level_and_nested_both_kept(self):
        top_level = [{"identifier": "ds-1"}]
        nested = [{"identifier": "ds-2", "parent_identifier": "series-1"}]

        result = merge_dcatus3_datasets(top_level, nested)

        assert {d["identifier"] for d in result} == {"ds-1", "ds-2"}

    def test_nested_vs_nested_overlap_not_merged(self):
        """Nested-vs-nested overlap is left for the duplicate filter to catch."""
        nested_a = [{"identifier": "ds-1", "parent_identifier": "svc-1"}]
        nested_b = [{"identifier": "ds-1", "parent_identifier": "series-1"}]

        result = merge_dcatus3_datasets([], nested_a, nested_b)

        assert len(result) == 2
        assert {d["parent_identifier"] for d in result} == {"svc-1", "series-1"}

    def test_first_nested_list_wins_when_multiple_match_top_level(self):
        """First nested list to claim a top-level dataset wins the tag."""
        top_level = [{"identifier": "ds-1"}]
        nested_a = [{"identifier": "ds-1", "parent_identifier": "svc-1"}]
        nested_b = [{"identifier": "ds-1", "parent_identifier": "series-1"}]

        result = merge_dcatus3_datasets(top_level, nested_a, nested_b)

        assert len(result) == 1
        assert result[0]["parent_identifier"] == "svc-1"

    def test_missing_identifiers_not_treated_as_matching(self):
        """Both normalize to None, so they stay as separate records."""
        top_level = [{"title": "No id top-level"}]
        nested = [{"title": "No id nested", "parent_identifier": "series-1"}]

        result = merge_dcatus3_datasets(top_level, nested)

        assert len(result) == 2

    def test_does_not_mutate_inputs(self):
        top_level = [{"identifier": "ds-1", "title": "Canonical title"}]
        nested = [{"identifier": "ds-1", "parent_identifier": "series-1"}]

        merge_dcatus3_datasets(top_level, nested)

        assert "parent_identifier" not in top_level[0]


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
