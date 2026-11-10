"""The validator's place-name lookup (dcatus_validation/places.py, reading a
SQLite file built by scripts/refresh_location_names.py) reimplements the rule
in HarvesterDBInterface.get_geo_from_string. These tests build the file from
the same rows the database holds and require both to give the same answer, so
a change to either one fails here until the other matches (GSA/data.gov#6380)."""

import pytest

from database.models import Locations
from dcatus_validation import places
from scripts.refresh_location_names import fetch_rows, write_location_names

# Any valid geometry; only whether a row has one matters to either lookup.
GEOM = (
    "0103000020E61000000100000005000000BA6B09F9A0C751C046B6F3FDD4384540BA6B09F9"
    "A0C751C08E06F016484045401B9E5E29CBC451C08E06F016484045401B9E5E29CBC451C046"
    "B6F3FDD4384540BA6B09F9A0C751C046B6F3FDD4384540"
)

# (type, display_name), one entry per locations row. The test database also
# starts with a "United States" country row (tests/conftest.py).
ROWS = [
    ("country", "Angola"),
    ("us_postalcode", "Angola, IN (46703)"),
    ("us_postalcode", "Angola, IN (46703)"),
    ("us_state", "Virginia"),
    ("us_state", "West Virginia"),
    ("us_county", "Fairfax County, Virginia"),
    ("us_county", "Abbeville County, South Carolina"),
    ("us_postalcode", "Abbeville, AL (36310)"),
    ("us_postalcode", "Abbeville, AL (36310)"),
    ("us_postalcode", "Stoneham, MA (02180)"),
    ("us_postalcode", "Stoneham, MA (02180)"),
    ("country", "Georgia"),
    ("us_state", "Georgia"),
    ("country", "100% Island"),
    ("country", "1000 Islands"),
    ("country", "Côte d'Ivoire"),
    ("country", "Fiji"),
    ("country", "Nogeomland"),
]
NO_GEOM = {"Nogeomland"}

QUERIES = [
    "United States",
    "united states",
    "Angola",
    "Virginia",
    "West Virginia",
    "Fairfax County",
    "fairfax county, virginia",
    "Abbeville County",
    "Stoneham",
    "Stoneham, MA (02180)",
    "Georgia",
    "Nowhere",
    "",
    "Islands",
    "Abbeville",
    "CÔTE",
    "fj",
    "ji",
    "Nogeomland",
]


@pytest.fixture
def db_lookup(interface, tmp_path, monkeypatch):
    """Load ROWS into the database, then build the validator's file from every
    row the database holds, the way the refresh script does for real data."""
    for index, (row_type, display_name) in enumerate(ROWS):
        interface.db.add(
            Locations(
                id=str(900000 + index),
                type=row_type,
                name=display_name,
                display_name=display_name,
                the_geom=None if display_name in NO_GEOM else GEOM,
                type_order="1",
            )
        )
    interface.db.commit()

    location_names = tmp_path / "location_names.sqlite"
    write_location_names(fetch_rows(interface.db.connection()), location_names)
    monkeypatch.setattr(places, "LOCATION_NAMES_PATH", location_names)
    places._cached_lookup.cache_clear()
    yield places.snapshot_place_lookup
    places._cached_lookup.cache_clear()


@pytest.mark.parametrize("query", QUERIES)
def test_validator_lookup_matches_database_lookup(interface, db_lookup, query):
    database_resolves = interface.get_geo_from_string(query) is not None
    validator_resolves = db_lookup(query) is not None

    assert validator_resolves == database_resolves


def test_known_gap_like_wildcards_are_literal(interface, db_lookup):
    # Documented in dcatus_validation/places.py: the database treats %, _ and \
    # as ILIKE wildcards/escape, the validator matches them as plain characters.
    assert interface.get_geo_from_string("Fair_ax") is not None
    assert db_lookup("Fair_ax") is None

    assert interface.get_geo_from_string("Fair%Virginia") is not None
    assert db_lookup("Fair%Virginia") is None

    assert interface.get_geo_from_string("100\\%") is not None
    assert db_lookup("100\\%") is None

    # "%100%%" also matches "1000 Islands"; the literal "100%" does not
    assert interface.get_geo_from_string("100%") is None
    assert db_lookup("100%") == "100% Island"
