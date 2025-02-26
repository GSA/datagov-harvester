import pytest

from harvester.utils.ckan_utils import munge_tag, munge_title_to_name
from harvester.utils.general_utils import (
    dynamic_map_list_items_to_dict,
    parse_args,
    prepare_transform_msg,
    query_filter_builder,
)


class TestCKANUtils:
    """These tests are copied from
    # https://github.com/ckan/ckan/blob/master/ckan/tests/lib/test_munge.py
    """

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
