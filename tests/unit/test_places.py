import time

import pytest

from dcatus_validation import places
from scripts.refresh_location_names import write_location_names


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    """Point the lookup at a small SQLite file built from these locations rows,
    one entry per row. Names in no_geom are rows without a geometry."""

    def use(names, no_geom=()):
        path = tmp_path / "location_names.sqlite"
        write_location_names([(name, name not in no_geom) for name in names], path)
        monkeypatch.setattr(places, "LOCATION_NAMES_PATH", path)
        places._cached_lookup.cache_clear()

    yield use
    places._cached_lookup.cache_clear()


class TestSnapshotPlaceLookup:
    def test_single_row_match_resolves_case_insensitively(self, snapshot):
        snapshot(["Fairfax County, Virginia", "Reston, VA (20190)"])

        assert places.snapshot_place_lookup("fairfax county") == (
            "Fairfax County, Virginia"
        )

    def test_non_ascii_case_folds(self, snapshot):
        snapshot(["Côte d'Ivoire"])

        assert places.snapshot_place_lookup("CÔTE") == "Côte d'Ivoire"

    def test_name_shared_by_two_rows_does_not_resolve(self, snapshot):
        # the harvester's .scalar() raises on more than one row
        snapshot(["Stoneham, MA (02180)", "Stoneham, MA (02180)"])

        assert places.snapshot_place_lookup("Stoneham") is None

    def test_substring_of_two_names_does_not_resolve(self, snapshot):
        snapshot(["Virginia", "West Virginia"])

        assert places.snapshot_place_lookup("Virginia") is None

    def test_city_inside_a_postal_code_name_does_not_resolve(self, snapshot):
        snapshot(
            [
                "Abbeville County, South Carolina",
                "Abbeville, AL (36310)",
                "Abbeville, AL (36310)",
            ]
        )

        assert places.snapshot_place_lookup("Abbeville") is None
        assert places.snapshot_place_lookup("Abbeville County") == (
            "Abbeville County, South Carolina"
        )

    def test_row_without_geometry_does_not_resolve(self, snapshot):
        snapshot(["Ohio"], no_geom={"Ohio"})

        assert places.snapshot_place_lookup("Ohio") is None
        assert places.snapshot_place_lookup("oh") is None

    def test_no_match(self, snapshot):
        snapshot(["Ohio"])

        assert places.snapshot_place_lookup("Nowhere") is None

    def test_like_wildcards_are_plain_characters(self, snapshot):
        # unlike the harvester's ILIKE; see the places.py docstring
        snapshot(["Ohio", "Utah"])

        assert places.snapshot_place_lookup("O_io") is None
        assert places.snapshot_place_lookup("U%h") is None
        assert places.snapshot_place_lookup("%") is None

    def test_percent_matches_only_a_literal_percent(self, snapshot):
        snapshot(["100% County", "1000 County"])

        assert places.snapshot_place_lookup("100%") == "100% County"
        assert places.snapshot_place_lookup("100\\%") is None

    def test_backslash_and_quote_are_plain_characters(self, snapshot):
        snapshot(["Ohio", "x Ohio\\", 'Say "Ohio"'])

        assert places.snapshot_place_lookup("Ohio\\") == "x Ohio\\"
        assert places.snapshot_place_lookup('"Ohio"') == 'Say "Ohio"'

    def test_short_names_use_the_same_rule(self, snapshot):
        snapshot(["Ohio", "Utah", "Côte"])

        assert places.snapshot_place_lookup("ô") == "Côte"
        assert places.snapshot_place_lookup("OH") == "Ohio"
        assert places.snapshot_place_lookup("h") is None
        assert places.snapshot_place_lookup("") is None
        assert places.snapshot_place_lookup("zz") is None


def test_empty_table_is_refused(tmp_path):
    with pytest.raises(SystemExit):
        write_location_names([], tmp_path / "location_names.sqlite")


def test_bundled_snapshot_loads_and_resolves_a_unique_name():
    places._cached_lookup.cache_clear()
    places.load_location_names()

    assert places.snapshot_place_lookup("United States") == "United States"


@pytest.mark.parametrize(
    "label",
    ["%%%%%%%%\x01", "%_" * 50 + "\x01", "%" * 1_000_000, "a%" * 1_000_000],
)
def test_wildcard_and_long_labels_return_quickly(snapshot, label):
    snapshot(["Ohio", "Utah", "Abbeville County, South Carolina"])

    start = time.monotonic()
    places.snapshot_place_lookup(label)

    assert time.monotonic() - start < 1


def test_names_longer_than_any_location_are_not_cached(snapshot):
    snapshot(["Ohio"])

    assert places.snapshot_place_lookup("Ohio" + "x") is None
    assert places._cached_lookup.cache_info().currsize == 0
