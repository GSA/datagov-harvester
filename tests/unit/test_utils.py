import json

import pytest

from harvester.utils.ckan_utils import (
    create_ckan_extras,
    create_ckan_tags,
    munge_spatial,
    munge_tag,
    munge_title_to_name,
    translate_spatial,
)
from harvester.utils.general_utils import (
    dynamic_map_list_items_to_dict,
    parse_args,
    prepare_transform_msg,
    process_job_complete_percentage,
    query_filter_builder,
    validate_geojson,
)


class TestCKANUtils:
    """Some of these tests are copied from
    # https://github.com/ckan/ckan/blob/master/ckan/tests/lib/test_munge.py
    """

    def test_create_ckan_tags(self, dcatus_keywords):
        expected = [
            {"name": "biosphere"},
            {"name": "coastal"},
            {"name": "docnoaanosocm"},
            {"name": "earth-science"},
            {"name": "ecosystems"},
            {"name": "ma"},
            {"name": "marine-ecosystems"},
            {"name": "national-ocean-service"},
            {"name": "nerrs"},
            {"name": "noaa"},
            {"name": "office-of-coastal-management"},
            {"name": "us-department-of-commerce"},
            {"name": "waquoit-bay-nerr"},
        ]

        tags = sorted(create_ckan_tags(dcatus_keywords), key=lambda t: t["name"])
        for i in range(len(tags)):
            assert tags[i] == expected[i]

    @pytest.mark.parametrize(
        "original,expected",
        [
            ("unchanged", "unchanged"),
            ("s", "s_"),  # too short
            ("some spaces  here", "some-spaces--here"),
            ("random:other%characters&_.here", "randomothercharactershere"),
            ("river-water-dashes", "river-water-dashes"),
        ],
    )
    def test_munge_tag_multiple_pass(self, original, expected):
        """Munge a list of tags muliple times gives expected results."""

        first_munge = munge_tag(original)
        assert first_munge == expected
        second_munge = munge_tag(first_munge)
        assert second_munge == expected

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

    def test_create_ckan_extras(self, dol_distribution_json, source_data_dcatus_orm):
        extras = create_ckan_extras(
            dol_distribution_json, source_data_dcatus_orm, "1234"
        )

        assert extras == [
            {"key": "resource-type", "value": "Dataset"},
            {"key": "harvest_object_id", "value": "1234"},
            {"key": "source_datajson_identifier", "value": True},
            {
                "key": "harvest_source_id",
                "value": "2f2652de-91df-4c63-8b53-bfced20b276b",
            },
            {"key": "harvest_source_title", "value": "Test Source"},
            {"key": "accessLevel", "value": "public"},
            {"key": "identifier", "value": "https://data.wa.gov/api/views/f6w7-q2d2"},
            {"key": "modified", "value": "2025-01-16"},
            {"key": "publisher_hierarchy", "value": "data.wa.gov"},
            {"key": "publisher", "value": "data.wa.gov"},
            {"key": "old-spatial", "value": "United States"},
            {
                "key": "spatial",
                "value": '{"type":"MultiPolygon","coordinates":'
                "[[[[-124.733253,24.544245],[-124.733253,49.388611],"
                "[-66.954811,49.388611],[-66.954811,24.544245],[-124.733253,24.544245]]]]}",
            },
            {"key": "identifier", "value": "https://data.wa.gov/api/views/f6w7-q2d2"},
        ]


# Point example
# "{\"type\": \"Point\", \"coordinates\": [-87.08258, 24.9579]}"
class TestGeneralUtils:
    def test_args_parsing(self):
        args = parse_args(["test-id", "test-type"])
        assert args.jobId == "test-id"
        assert args.jobType == "test-type"

    @pytest.mark.parametrize(
        "base,facets,expected",
        [
            ("1", "", "1"),
            ("1", "234", "1 AND 234"),
            ("1", "2,3,4", "1 AND 2 AND 3 AND 4"),
            ("1", "2,3,4,", "1 AND 2 AND 3 AND 4"),
            ("1", "2 != 3,3 <= 4,", "1 AND 2 != 3 AND 3 <= 4"),
            (None, "1,", "1"),
            (None, "1 AND 2", "1 AND 2"),
            (None, "1,2", "1 AND 2"),
            (None, "1 , 2", "1  AND  2"),
            (None, "1 OR 2", "1 OR 2"),
            (None, ", facet_key = 'facet_val'", "facet_key = 'facet_val'"),
        ],
    )
    def test_facet_builder(self, base, facets, expected):
        assert expected == query_filter_builder(base, facets)

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
