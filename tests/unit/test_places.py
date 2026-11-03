import json

import pytest

from dcatus_validation import places


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    """Point the lookup at a small snapshot, the way the locations table
    would hold these rows."""

    def use(names):
        path = tmp_path / "location_names.json"
        path.write_text(json.dumps({"values": names}))
        monkeypatch.setattr(places, "LOCATION_NAMES_PATH", path)
        places._snapshot.cache_clear()
        places.snapshot_place_lookup.cache_clear()

    yield use
    places._snapshot.cache_clear()
    places.snapshot_place_lookup.cache_clear()


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

    def test_like_wildcards_match_as_in_postgres(self, snapshot):
        snapshot(["Ohio", "Utah"])

        assert places.snapshot_place_lookup("O_io") == "Ohio"
        assert places.snapshot_place_lookup("U%h") == "Utah"
        assert places.snapshot_place_lookup("%") is None

    def test_escaped_wildcard_is_literal(self, snapshot):
        snapshot(["100% County", "1000 County"])

        assert places.snapshot_place_lookup("100\\%") == "100% County"

    def test_trailing_escape_escapes_the_closing_wildcard(self, snapshot):
        # the harvester builds f"%{name}%", so "Ohio\\" means "ends in Ohio%"
        snapshot(["Ohio", "x Ohio%", "Ohio% y"])

        assert places.snapshot_place_lookup("Ohio\\") == "x Ohio%"

    def test_newline_never_matches_across_names(self, snapshot):
        snapshot(["Ohio", "Utah"])

        assert places.snapshot_place_lookup("ohio\nutah") is None


def test_bundled_snapshot_loads_and_resolves_a_unique_name():
    places._snapshot.cache_clear()
    places.snapshot_place_lookup.cache_clear()
    places.load_location_names()

    assert places.snapshot_place_lookup("United States") == "United States"
