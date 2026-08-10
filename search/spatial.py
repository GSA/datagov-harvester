import json
from typing import Any

from haversine import Unit, haversine


def calc_geometry_centroid(geometry: Any) -> dict | None:
    """
    Return a centroid point (latitude/longitude) for a GeoJSON geometry mapping.

    Parameters
    ----------
    geometry : Any
        A GeoJSON geometry mapping (dict) or a JSON-encoded string containing
        such a mapping. If None is passed, or if the input cannot be parsed /
        does not contain coordinates, the function returns None.

    Returns
    -------
    dict | None
        A dictionary with keys 'lat' and 'lon' containing the arithmetic mean of the
        collected coordinates as floats, e.g. {'lat': 12.34, 'lon': 56.78},
        or None if no valid coordinate pairs were found.

    Examples
    --------
    - Point: {'type': 'Point', 'coordinates': [lon, lat]} yields
        {'lon': lon, 'lat': lat}
    - Polygon: the centroid returned is the mean of all polygon vertex
        coordinates (not the true polygon centroid).
    """
    """Return a centroid point for a GeoJSON geometry mapping."""
    if geometry is None:
        return None

    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except json.JSONDecodeError:
            return None

    if not isinstance(geometry, dict):
        return None

    coords = geometry.get("coordinates")
    if coords is None:
        return None

    points: list[tuple[float, float]] = []

    def _add_point(value: Any) -> bool:
        if not isinstance(value, (list, tuple)):
            return False
        if len(value) < 2:
            return False
        lon, lat = value[0], value[1]
        if not isinstance(lon, (int, float)) or not isinstance(lat, (int, float)):
            return False
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            return False
        points.append((float(lon), float(lat)))
        return True

    def _walk(value: Any) -> None:
        if _add_point(value):
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                _walk(item)

    _walk(coords)

    if not points:
        return None

    lon_total = sum(point[0] for point in points)
    lat_total = sum(point[1] for point in points)
    count = len(points)
    return {"lat": lat_total / count, "lon": lon_total / count}


def calc_distance_km(point_a: dict, point_b: dict) -> float | None:
    """Return the great-circle distance in km between two points.

    Accepts ``{"lat": ..., "lon": ...}`` or ``[lon, lat]`` / ``(lon, lat)``.
    Uses ``haversine.haversine`` with ``Unit.KILOMETERS``.
    Haversine formula: https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.haversine_distances.html
    """
    if not point_a or not point_b:
        return None

    def _extract(point: dict):
        if isinstance(point, dict):
            lat = point.get("lat")
            lon = point.get("lon")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                return float(lat), float(lon)
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            lon, lat = point[0], point[1]
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                return float(lat), float(lon)
        return None

    parsed_a = _extract(point_a)
    parsed_b = _extract(point_b)
    if not parsed_a or not parsed_b:
        return None

    lat1, lon1 = parsed_a
    lat2, lon2 = parsed_b

    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)
