import pytest

from harvester.opensearch.spatial import calc_geometry_centroid


def test_calc_geometry_centroid_returns_average():
    geometry = {"type": "MultiPoint", "coordinates": [[0, 0], [2, 2]]}

    centroid = calc_geometry_centroid(geometry)

    assert centroid == {"lat": 1.0, "lon": 1.0}


def test_calc_geometry_centroid_accepts_json_string():
    geometry = '{"type": "Point", "coordinates": [10, 20]}'

    centroid = calc_geometry_centroid(geometry)

    assert centroid == {"lat": 20.0, "lon": 10.0}


@pytest.mark.parametrize(
    "geometry",
    [
        {"type": "Point", "coordinates": [185.0, 45.0]},
        {"type": "Point", "coordinates": [-74.0, -91.0]},
    ],
)
def test_calc_geometry_centroid_skips_out_of_range_coordinates(geometry):
    assert calc_geometry_centroid(geometry) is None


def test_calc_geometry_centroid_from_polygon():
    geometry = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
    }
    centroid = calc_geometry_centroid(geometry)
    assert centroid is not None
    assert centroid["lon"] == pytest.approx(0.8)
    assert centroid["lat"] == pytest.approx(0.8)


def test_calc_geometry_centroid_skips_out_of_range_longitude():
    """
    A longitude outside -180-180 (e.g. 185.34) must be excluded from the
    centroid calculation to prevent OpenSearch geo_point parse failures.
    """
    geometry = {
        "type": "Point",
        "coordinates": [185.34570208999997, 45.0],
    }
    centroid = calc_geometry_centroid(geometry)
    # The single point is invalid, so no valid points remain,
    # so it should return none
    assert centroid is None


def test_calc_geometry_centroid_skips_out_of_range_latitude():
    """
    A latitude outside -90-90 (e.g. -90.90) must be excluded from the
    centroid calculation to prevent OpenSearch geo_point parse failures.
    """
    geometry = {
        "type": "Point",
        "coordinates": [-74.0, -90.90776196883162],
    }
    centroid = calc_geometry_centroid(geometry)
    assert centroid is None


def test_calc_geometry_centroid_uses_valid_points_when_some_are_out_of_range():
    """
    When a geometry contains a mix of valid and out-of-range coordinates, the
    centroid is computed from only the valid points rather than discarding
    the whole geometry.
    """
    geometry = {
        "type": "MultiPoint",
        "coordinates": [
            [10.0, 20.0],  # valid
            [185.0, 45.0],  # invalid lon
            [30.0, -91.0],  # invalid lat
            [50.0, 60.0],  # valid
        ],
    }
    centroid = calc_geometry_centroid(geometry)
    assert centroid is not None
    assert centroid["lon"] == pytest.approx(30.0)  # mean of 10.0 and 50.0
    assert centroid["lat"] == pytest.approx(40.0)  # mean of 20.0 and 60.0
