from database.models import (
    Locations,
)


def test_get_geo_from_string(interface, named_location_us, named_location_stoneham):
    stoneham = Locations(
        **{
            "id": "34333",
            "type": "us_postalcode",
            "name": "2180",
            "display_name": "Stoneham, MA (02180)",
            "the_geom": "0103000020E61000000100000005000000BA6B09F9A0C751C046B6F3FDD4384540BA6B09F9A0C751C08E06F016484045401B9E5E29CBC451C08E06F016484045401B9E5E29CBC451C046B6F3FDD4384540BA6B09F9A0C751C046B6F3FDD4384540",  # noqa E501
            "type_order": "4",
        }
    )
    interface.db.add(stoneham)
    interface.db.commit()

    # Expect to find th US, loaded on test setup
    geojson_str = interface.get_geo_from_string("United States")
    assert geojson_str == named_location_us

    # Expect to find Stoneham, loaded in this test
    geojson_str = interface.get_geo_from_string("Stoneham")
    assert geojson_str == named_location_stoneham

    # Do not expect the following strings to match
    assert interface.get_geo_from_string("not exists") is None
    assert interface.get_geo_from_string("US, Virginia, Fairfax, Reston") is None
