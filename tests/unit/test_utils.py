import pytest

from harvester.utils.ckan_utils import munge_tag, munge_title_to_name
from harvester.utils.general_utils import parse_args

# these tests are copied from
# https://github.com/ckan/ckan/blob/master/ckan/tests/lib/test_munge.py


class TestCKANUtils:
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
        args = parse_args(["test-id"])
        assert args.jobId == "test-id"
