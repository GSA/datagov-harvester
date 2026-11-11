"""Spatial value normalization shared by the harvest runner and the public
validator. Place-name resolution is passed in as `place_lookup` (the harvester
uses the locations table; the validator a bundled CSV), so this module
needs no database."""

import json
import logging
import re
from typing import Callable

import geojson_validator
import shapely
import shapely.wkt
from shapely.geometry import LineString, Point
from shapely.geometry import mapping as shapely_geom_mapping

from dcatus_validation.deadline import check_deadline
from dcatus_validation.limits import SPATIAL_SECONDS_PER_MB

logger = logging.getLogger(__name__)

# Resolves a place name to a GeoJSON string, or None. A caller that only needs
# to know whether a value resolves may return any truthy value instead.
PlaceLookup = Callable[[str], str | None]


# geojson validator criteria
INVALID_CRITERIA = [
    "unclosed",
    "less_three_unique_nodes",
    "exterior_not_ccw",
    "interior_not_cw",
]


PROBLEMATIC_CRITERIA = [
    "holes",
    "inner_and_exterior_ring_intersect",
    "self_intersection",
    "duplicate_nodes",
    # "excessive_coordinate_precision",  # ignore
    "excessive_vertices",
    "3d_coordinates",
    "outside_lat_lon_boundaries",
    "crosses_antimeridian",
]


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Find if a line between 2 x coordinates would cross the meridian
def crosses_meridian(val1, val2):
    """
    checks if the 2 values cross the anti-meridian

    this function is only called under the condition that geojson-validator
    identified the geometry as "crosses_antimeridian" so something like
    abs(-160-40) > 180 shouldn't happen.
    """

    # A jump greater than 180 degrees means the edge crosses
    # the antimeridian while both coordinates are already normalized.
    if abs(val1 - val2) > 180:
        return True, val1 < 0

    longs = [val1, val2]
    longs.sort()

    if longs[1] > 180 and longs[0] <= 180:
        return True, val1 - val2 > 0
    if longs[0] < -180 and longs[1] >= -180:
        return True, val1 - val2 > 0

    return False, None


# Change value to valid x coordinate, no matter how far afield
def fix_longitude(val):
    if val > 180:
        return fix_longitude(val - 360)
    if val < -180:
        return fix_longitude(val + 360)
    return val


def ensure_counter_clockwise(points: list[list]):
    """
    the input points represent the external ring of a polygon.
    geojson requires external rings to be counterclockwise.
    """
    area = sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(points, points[1:]))

    if area < 0:
        points.reverse()

    return points


# https://www.rfc-editor.org/rfc/rfc7946#section-3.1.9
# Changes polygon to multi-polygon around the antimeridian
# We can assume this is a counter-clockwise list creating a polygon
def spatial_wrap_around_meridian(geom):
    # Place our new geometry here
    new_geom = {"type": "MultiPolygon", "coordinates": [[[], []]]}
    # We will switch between the two polygons as we cross the meridian
    polygon_num = 0

    # Find where we cross, and fill the multi-polygon
    point_list = geom.get("coordinates")[0]
    for i, coord in enumerate(point_list):
        # If we are at the last point, then we can just save it and break
        if i == len(point_list) - 1:
            new_geom["coordinates"][0][polygon_num].append(
                [fix_longitude(coord[0]), coord[1]]
            )
            break
        # Am I going to cross the meridian on the next point?
        crossing_meridian, is_positive = crosses_meridian(
            coord[0], point_list[i + 1][0]
        )
        if crossing_meridian:
            new_long = fix_longitude(coord[0])
            new_geom["coordinates"][0][polygon_num].append([new_long, coord[1]])
            # Assume we start on the left side of the meridian
            longs = [180.0, -180.0]
            if is_positive:
                # If not, we started on the right side
                longs = [-180.0, 180.0]
            # Calculate the distance longitude between the 2 points
            x_dist = abs(coord[0] - point_list[i + 1][0])

            if x_dist > 180:
                x_dist = 360 - x_dist

            # Calculate the percentage of the longitude to the meridian
            x_perc = abs(abs(new_long) - 180.0) / x_dist
            # Calculate the height at the meridian
            height_at_meridian = (
                point_list[i + 1][1] - point_list[i][1]
            ) * x_perc + coord[1]
            # Add the point at the meridian
            new_geom["coordinates"][0][polygon_num].append(
                [longs[0], height_at_meridian]
            )
            # Wrap up the "middle" polygon, it ends where it begins.
            if polygon_num == 1:
                new_geom["coordinates"][0][polygon_num].append(
                    new_geom["coordinates"][0][polygon_num][0]
                )
            polygon_num = (polygon_num + 1) % 2
            # Start the next polygon at the same point, just on the other side.
            new_geom["coordinates"][0][polygon_num].append(
                [longs[1], height_at_meridian]
            )
        # If not, continue to add to the current polygon
        else:
            new_geom["coordinates"][0][polygon_num].append(
                [fix_longitude(coord[0]), coord[1]]
            )
    # Unclear why this is needed, but to work with the right hand rule.
    # https://medium.com/@jinagamvasubabu/solution-polygons-and-multipolygons-should-follow-the-right-hand-rule-27b96fa61c6
    new_geom["coordinates"] = [
        [ensure_counter_clockwise(new_geom["coordinates"][0][0])],
        [ensure_counter_clockwise(new_geom["coordinates"][0][1])],
    ]

    return new_geom


def validate_geojson(geojson_str: str) -> bool:
    try:
        res = geojson_validator.validate_geometries(
            json.loads(geojson_str),
            criteria_invalid=INVALID_CRITERIA,
            criteria_problematic=PROBLEMATIC_CRITERIA,
        )
        # If the geometry is valid, return the string
        if res.get("invalid") == {} and res.get("problematic") == {}:
            return geojson_str
        else:
            logger.warning(
                "This spatial value has problems: %s, we will attempt a fix and "
                "revalidate",
                geojson_str,
            )
    # ruff: noqa: E722
    except:
        logger.warning("This spatial value was unable be validated: %s", geojson_str)
        pass

    # Try to fix the geometry
    try:
        geojson = json.loads(geojson_str)
        # Multi-polygon doesn't get cleaned up well; if it's actually a polygon,
        # change it to one.
        if (
            geojson.get("type") == "MultiPolygon"
            and len(geojson.get("coordinates")) == 1
            and len(geojson.get("coordinates")[0]) == 1
        ):
            geojson["type"] = "Polygon"
            geojson["coordinates"] = geojson["coordinates"][0]
        fixed_geom = geojson_validator.fix_geometries(geojson)
        fixed_geom = fixed_geom.get("features")[0].get("geometry")
        res = geojson_validator.validate_geometries(
            fixed_geom,
            criteria_invalid=INVALID_CRITERIA,
            criteria_problematic=PROBLEMATIC_CRITERIA,
        )
        if res.get("invalid") == {} and res.get("problematic") == {}:
            return json.dumps(fixed_geom)
        elif (
            res.get("problematic").get("crosses_antimeridian") is not None
            and fixed_geom.get("type") == "Polygon"
        ):
            fixed_geom = spatial_wrap_around_meridian(fixed_geom)
            return json.dumps(fixed_geom)
        else:
            logger.warning(
                "This spatial value will be ignored: %s, for the following reasons: %s",
                geojson_str,
                res,
            )
    # ruff: noqa: E722
    except:
        logger.warning("This spatial value was unable be fixed: %s", geojson_str)
        pass
    return False


_WKT_GEOMETRY_RE = re.compile(
    r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION)\s*(?:[ZM]{1,2}\s*)?\(",
    re.IGNORECASE,
)


def _reduce_degenerate_polygon(geom):
    """Collapse a zero-area Polygon exterior ring into a Point or LineString.

    Coordinate rounding by some sources (e.g. bboxes rounded to 2 decimal
    places for a small survey area) can collapse a Polygon's corners onto
    each other or onto a line. shapely still parses these, but
    geojson_validator's less_three_unique_nodes/exterior_not_ccw checks
    reject them outright. munge_spatial already reduces this same situation
    for v1.1 comma-separated bboxes, so mirror that here instead of losing
    the geometry.
    """
    if geom.geom_type != "Polygon":
        return geom

    unique_coords = list(dict.fromkeys(geom.exterior.coords))
    if len(unique_coords) == 1:
        return Point(unique_coords[0])
    if len(unique_coords) == 2:
        return LineString(unique_coords)
    return geom


def translate_wkt_to_geojson(spatial_value: str) -> str:
    """Convert a WKT geometry string into a GeoJSON string, if possible."""

    if not _WKT_GEOMETRY_RE.match(spatial_value):
        return ""

    try:
        geom = shapely.wkt.loads(spatial_value.strip())
        # GeoJSON is 2D; drop any Z/M dimension rather than let the
        # 3d_coordinates check in validate_geojson silently discard it.
        geom = shapely.force_2d(geom)
        geom = _reduce_degenerate_polygon(geom)
        return json.dumps(shapely_geom_mapping(geom))
    except:  # noqa: E722
        logger.warning(
            "This spatial value looked like WKT but failed to parse: %s", spatial_value
        )
        return ""


def munge_spatial(spatial_value: str) -> str:
    """Translate loose spatial inputs into GeoJSON strings when possible."""

    geojson_polygon_tpl = (
        '{{"type": "Polygon", "coordinates": [[[{minx}, {miny}], '
        "[{minx}, {maxy}], [{maxx}, {maxy}], [{maxx}, {miny}], [{minx}, {miny}]]]}}"
    )
    geojson_point_tpl = '{{"type": "Point", "coordinates": [{x}, {y}]}}'
    geojson_line_tpl = (
        '{{"type": "LineString", "coordinates": [[{x1}, {y1}], [{x2}, {y2}]]}}'
    )

    #  do some string munging to try to get to something parseable
    #  1. Remove "+" characters.
    #  2. Replace occurrences of "., " and ".]" to fix some malformed separators.
    #  3. Strip a trailing "." if present.
    #  4. Remove unnecessary leading zeros A regex call.
    spatial_value = spatial_value.replace("+", "")
    spatial_value = spatial_value.replace(".,", ",").replace(".]", "]")
    if spatial_value and spatial_value[-1] == ".":
        spatial_value = spatial_value[:-1]
    spatial_value = re.sub(
        r"(^|\s)(-?)0+((0|[1-9][0-9]*)(\.[0-9]*)?)",
        r"\1\2\3",
        spatial_value,
    )

    try:
        numbers_with_spaces = [int(i) for i in spatial_value.split(" ")]
        if all(isinstance(x, int) for x in numbers_with_spaces):
            spatial_value = spatial_value.replace(" ", ",")
    except ValueError:
        pass

    parts = [part.strip() for part in spatial_value.strip().split(",")]

    if len(parts) == 2 and all(is_number(x) for x in parts):
        x, y = parts
        return geojson_point_tpl.format(x=x, y=y)

    if len(parts) == 4 and all(is_number(x) for x in parts):
        coords = list(dict.fromkeys(parts))  # removes duplicates while preserving order
        # duplicate points situation (e.g. "-92.109, 15.132, -92.109, 15.132")
        if len(coords) == 2 and parts[:2] == parts[2:]:
            x, y = coords
            return geojson_point_tpl.format(x=x, y=y)
        if len(coords) == 3:  # only 3 unique lat/lons which means it's a straight line
            return geojson_line_tpl.format(
                x1=parts[0], y1=parts[1], x2=parts[2], y2=parts[3]
            )
        # e.g. '0,0,0,0'
        if len(coords) == 1:
            return geojson_point_tpl.format(x=coords[0], y=coords[0])

        minx, miny, maxx, maxy = parts
        return geojson_polygon_tpl.format(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    try:
        geometry = json.loads(spatial_value)
        if isinstance(geometry, list) and len(geometry) == 2:
            min_point, max_point = geometry
            return geojson_polygon_tpl.format(
                minx=min_point[0],
                miny=min_point[1],
                maxx=max_point[0],
                maxy=max_point[1],
            )
    except Exception:  # noqa: BLE001
        pass

    return ""


def _unwrap_single_location(input_value):
    """Resolve a single (non-array) spatial value to its geometry-bearing
    value, or None if it has nothing usable.

    When a Location defines more than one of these at once, bbox outranks
    centroid outranks geometry (decided in #6297).
    """

    if (
        isinstance(input_value, dict)
        and not {
            "type",
            "coordinates",
        }
        <= input_value.keys()
    ):
        for field in ("bbox", "centroid", "geometry"):
            value = input_value.get(field)
            if value:
                return value
        return None

    return input_value


def _extract_label(input_value, field_name):
    """Return a Location's `field_name` (prefLabel or altLabel) as a plain
    string, or None if absent, blank, or not a dict.

    Consulted by _unwrap_location only when nothing in the whole input has
    usable geometry - real geometry always outranks a named-place fallback.
    (Full hierarchy is geojson > delimited coords > named location; the
    delimited-coords tier isn't implemented yet - translate_spatial's
    existing validate_geojson -> get_geo_from_string -> munge_spatial order
    is unchanged by this.)
    """

    if not isinstance(input_value, dict):
        return None
    label = input_value.get(field_name)
    if isinstance(label, str) and label.strip():
        return label
    return None


def _unwrap_location(input_value):
    """Extract the geometry-bearing value from a DCAT-US 3.0 Location object.

    v3.0 `spatial` is a Location object or a list of them; v1.1 is a plain
    string. A bare {type, coordinates} GeoJSON dict is passed through.

    Priority (decided in #6297) is bbox > centroid > geometry > prefLabel >
    altLabel, first valid value wins:

    Real geometry anywhere in the input always wins over a named-place
    (prefLabel/altLabel) fallback found anywhere else: the first element
    with usable geometry short-circuits the scan immediately. Only when NO
    element has any geometry do we fall back to the first prefLabel seen
    across the whole input, or - only if no element has a prefLabel either -
    the first altLabel seen. The winning label is returned as a plain string
    so it flows into translate_spatial's existing string branch (and from
    there, its existing locations-table lookup) instead of being discarded.
    """

    items = input_value if isinstance(input_value, list) else [input_value]

    for item in items:
        unwrapped = _unwrap_single_location(item)
        if unwrapped:
            return unwrapped

    for field_name in ("prefLabel", "altLabel"):
        for item in items:
            label = _extract_label(item, field_name)
            if label:
                return label

    return None


def translate_spatial(input_value, place_lookup: PlaceLookup | None = None) -> str:
    """Normalize spatial strings/dicts into GeoJSON strings when possible.

    place_lookup resolves a place name (e.g. "Virginia") when the value is
    neither GeoJSON nor WKT. Without one, place names don't resolve.
    """

    input_value = _unwrap_location(input_value)

    if isinstance(input_value, dict):
        spatial_value = json.dumps(input_value)
    elif isinstance(input_value, str):
        spatial_value = input_value
    else:
        return ""

    check_deadline(len(spatial_value) / 1_000_000 * SPATIAL_SECONDS_PER_MB)

    if isinstance(spatial_value, str):
        wkt_geojson = translate_wkt_to_geojson(spatial_value)
        if wkt_geojson:
            spatial_value = wkt_geojson

    validated_geojson = validate_geojson(spatial_value)
    if validated_geojson:
        return validated_geojson

    if place_lookup is not None:
        try:
            resolved_geojson = place_lookup(spatial_value)
            if resolved_geojson:
                return resolved_geojson
        except Exception:  # pragma: no cover - defensive lookup
            pass

    if isinstance(spatial_value, str):
        return munge_spatial(spatial_value)

    return ""


def translate_spatial_to_geojson(
    spatial_input, place_lookup: PlaceLookup | None = None
):
    """Return a GeoJSON dict for the provided spatial input, if possible."""

    if not spatial_input:
        return None

    translated_value = translate_spatial(spatial_input, place_lookup)
    if not translated_value:
        return None

    try:
        return json.loads(translated_value)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        logger.warning("Unable to decode translated spatial value: %s", exc)
        return None
