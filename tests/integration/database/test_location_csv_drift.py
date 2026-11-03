"""The validator's place-name lookup (dcatus_validation/places.py, reading a CSV
built by scripts/refresh_location_names.py) reimplements the rule in
HarvesterDBInterface.get_geo_from_string. These tests build the CSV from the
same rows the database holds and require both to give the same answer, so a
change to either one fails here until the other matches (GSA/data.gov#6380)."""

import csv

import pytest

from database.models import Locations
from dcatus_validation import places
from scripts.refresh_location_names import build_rows, write_location_names

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
]

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
    "Fair_ax",
    "Fair%Virginia",
    "100\\%",
    "100%",
    "Islands",
]


@pytest.fixture
def csv_lookup(interface, tmp_path, monkeypatch):
    """Load ROWS into the database, then build the validator's CSV from every
    row the database holds, the way the refresh script does for real data."""
    for index, (row_type, display_name) in enumerate(ROWS):
        interface.db.add(
            Locations(
                id=str(900000 + index),
                type=row_type,
                name=display_name,
                display_name=display_name,
                the_geom=GEOM,
                type_order="1",
            )
        )
    interface.db.commit()

    export = tmp_path / "locations.csv"
    with open(export, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["type", "display_name", "the_geom"])
        for row_type, display_name, has_geom in interface.db.query(
            Locations.type, Locations.display_name, Locations.the_geom.isnot(None)
        ):
            writer.writerow([row_type, display_name, "x" if has_geom else ""])

    location_names = tmp_path / "location_names.csv"
    write_location_names(build_rows(export), location_names)
    monkeypatch.setattr(places, "LOCATION_NAMES_PATH", location_names)
    places._snapshot.cache_clear()
    places.snapshot_place_lookup.cache_clear()
    yield places.snapshot_place_lookup
    places._snapshot.cache_clear()
    places.snapshot_place_lookup.cache_clear()


@pytest.mark.parametrize("query", QUERIES)
def test_csv_lookup_matches_database_lookup(interface, csv_lookup, query):
    database_resolves = interface.get_geo_from_string(query) is not None
    csv_resolves = csv_lookup(query) is not None

    assert csv_resolves == database_resolves


def test_known_gap_city_name_inside_a_postal_code_name(interface, csv_lookup):
    # Documented in dcatus_validation/places.py: "Abbeville" matches one kept
    # county row and two left-out postal-code rows. If this ever agrees, the
    # gap is closed; update the docs that describe it.
    assert interface.get_geo_from_string("Abbeville") is None
    assert csv_lookup("Abbeville") == "Abbeville County, South Carolina"
