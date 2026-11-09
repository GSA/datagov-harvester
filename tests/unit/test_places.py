import csv
import time
from collections import Counter

import pytest

from dcatus_validation import places


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    """Point the lookup at a small CSV built from these locations rows (one
    entry per row, so a repeated name has a row count above 1). Names in
    in_postal_code are flagged as appearing inside a postal-code name."""

    def use(names, in_postal_code=()):
        path = tmp_path / "location_names.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["display_name", "row_count", "in_postal_code_name"])
            for name, count in Counter(names).items():
                writer.writerow([name, count, int(name in in_postal_code)])
        monkeypatch.setattr(places, "LOCATION_NAMES_PATH", path)
        places._snapshot.cache_clear()
        places._cached_lookup.cache_clear()

    yield use
    places._snapshot.cache_clear()
    places._cached_lookup.cache_clear()


class TestSnapshotPlaceLookup:
    def test_single_row_match_resolves_case_insensitively(self, snapshot):
        snapshot(["Fairfax County, Virginia", "Reston, VA (20190)"])

        assert places.snapshot_place_lookup("fairfax county") == (
            "Fairfax County, Virginia"
        )

    def test_name_shared_by_two_rows_does_not_resolve(self, snapshot):
        # the harvester's .scalar() raises on more than one row
        snapshot(["Stoneham, MA (02180)", "Stoneham, MA (02180)"])

        assert places.snapshot_place_lookup("Stoneham") is None

    def test_substring_of_two_names_does_not_resolve(self, snapshot):
        snapshot(["Virginia", "West Virginia"])

        assert places.snapshot_place_lookup("Virginia") is None

    def test_two_matches_inside_one_name_still_resolve(self, snapshot):
        snapshot(["Ia Ia Town", "Ohio"])

        assert places.snapshot_place_lookup("ia") == "Ia Ia Town"

    def test_no_match(self, snapshot):
        snapshot(["Ohio"])

        assert places.snapshot_place_lookup("Nowhere") is None

    def test_like_wildcards_are_plain_characters(self, snapshot):
        # unlike the harvester's ILIKE; see the places.py docstring
        snapshot(["Ohio", "Utah"])

        assert places.snapshot_place_lookup("O_io") is None
        assert places.snapshot_place_lookup("U%h") is None

    def test_percent_matches_only_a_literal_percent(self, snapshot):
        snapshot(["100% County", "1000 County"])

        assert places.snapshot_place_lookup("100%") == "100% County"
        assert places.snapshot_place_lookup("100\\%") is None

    def test_backslash_is_a_plain_character(self, snapshot):
        snapshot(["Ohio", "x Ohio\\", "x Ohio%"])

        assert places.snapshot_place_lookup("Ohio\\") == "x Ohio\\"

    def test_name_inside_a_postal_code_name_does_not_resolve(self, snapshot):
        # "Angola" is also inside "Angola, IN (46703)", whose rows were left out
        snapshot(["Angola"], in_postal_code={"Angola"})

        assert places.snapshot_place_lookup("Angola") is None

    def test_newline_never_matches_across_names(self, snapshot):
        snapshot(["Ohio", "Utah"])

        assert places.snapshot_place_lookup("ohio\nutah") is None


def test_bundled_snapshot_loads_and_resolves_a_unique_name():
    places._snapshot.cache_clear()
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


def test_long_names_are_not_cached(snapshot):
    snapshot(["Ohio"])

    places.snapshot_place_lookup("x" * (places._MAX_CACHED_NAME_CHARS + 1))

    assert places._cached_lookup.cache_info().currsize == 0
