import json
import logging
import re
import uuid

from harvester.utils.general_utils import is_number, validate_geojson

from .. import db_interface

PACKAGE_NAME_MAX_LENGTH = 90
PACKAGE_NAME_MIN_LENGTH = 2

logger = logging.getLogger("ckan_utils")


def _munge_to_length(string: str, min_length: int, max_length: int) -> str:
    """Pad or truncate a string to fit the configured bounds."""
    if len(string) < min_length:
        string += "_" * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string


def substitute_ascii_equivalents(text_unicode: str) -> str:
    """Replace Latin-1 characters with ASCII equivalents when possible."""
    char_mapping = {
        0xC0: "A",
        0xC1: "A",
        0xC2: "A",
        0xC3: "A",
        0xC4: "A",
        0xC5: "A",
        0xC6: "Ae",
        0xC7: "C",
        0xC8: "E",
        0xC9: "E",
        0xCA: "E",
        0xCB: "E",
        0xCC: "I",
        0xCD: "I",
        0xCE: "I",
        0xCF: "I",
        0xD0: "Th",
        0xD1: "N",
        0xD2: "O",
        0xD3: "O",
        0xD4: "O",
        0xD5: "O",
        0xD6: "O",
        0xD8: "O",
        0xD9: "U",
        0xDA: "U",
        0xDB: "U",
        0xDC: "U",
        0xDD: "Y",
        0xDE: "th",
        0xDF: "ss",
        0xE0: "a",
        0xE1: "a",
        0xE2: "a",
        0xE3: "a",
        0xE4: "a",
        0xE5: "a",
        0xE6: "ae",
        0xE7: "c",
        0xE8: "e",
        0xE9: "e",
        0xEA: "e",
        0xEB: "e",
        0xEC: "i",
        0xED: "i",
        0xEE: "i",
        0xEF: "i",
        0xF0: "th",
        0xF1: "n",
        0xF2: "o",
        0xF3: "o",
        0xF4: "o",
        0xF5: "o",
        0xF6: "o",
        0xF8: "o",
        0xF9: "u",
        0xFA: "u",
        0xFB: "u",
        0xFC: "u",
        0xFD: "y",
        0xFE: "th",
        0xFF: "y",
    }

    result = []
    for char in text_unicode:
        code_point = ord(char)
        if code_point in char_mapping:
            result.append(char_mapping[code_point])
        elif code_point >= 0x80:
            continue
        else:
            result.append(char)
    return "".join(result)


def munge_title_to_name(name: str) -> str:
    """Convert a dataset title into a slug-friendly package name."""
    name = substitute_ascii_equivalents(name)
    name = re.sub("[ .:/]", "-", name)
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    name = re.sub("-+", "-", name).strip("-")

    if len(name) > PACKAGE_NAME_MAX_LENGTH:
        year_match = re.match(r".*?[_-]((?:\d{2,4}[-/])?\d{2,4})$", name)
        if year_match:
            year = year_match.group(1)
            name = f"{name[: (PACKAGE_NAME_MAX_LENGTH - len(year) - 1)]}-{year}"
        else:
            name = name[:PACKAGE_NAME_MAX_LENGTH]

    return _munge_to_length(name, PACKAGE_NAME_MIN_LENGTH, PACKAGE_NAME_MAX_LENGTH)


def add_uuid_to_package_name(name: str) -> str:
    return f"{name}-{str(uuid.uuid4())[:5]}"


def munge_spatial(spatial_value: str) -> str:
    geojson_polygon_tpl = (
        '{{"type": "Polygon", "coordinates": [[[{minx}, {miny}], '
        '[{minx}, {maxy}], [{maxx}, {maxy}], [{maxx}, {miny}], [{minx}, {miny}]]]}}'
    )
    geojson_point_tpl = '{{"type": "Point", "coordinates": [{x}, {y}]}}'

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

    parts = spatial_value.strip().split(",")
    if len(parts) == 4 and all(is_number(x) for x in parts):
        minx, miny, maxx, maxy = parts
        return geojson_polygon_tpl.format(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
    if len(parts) == 2 and all(is_number(x) for x in parts):
        x, y = parts
        return geojson_point_tpl.format(x=x, y=y)

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
    except Exception:  # noqa: BLE001 - defensive
        pass

    return ""


def translate_spatial(input_value) -> str:
    if isinstance(input_value, dict):
        spatial_value = json.dumps(input_value)
    elif isinstance(input_value, str):
        spatial_value = input_value
    else:
        return ""

    validated_geojson = validate_geojson(spatial_value)
    if validated_geojson:
        return validated_geojson

    resolved_geojson = db_interface.get_geo_from_string(spatial_value)
    if resolved_geojson is not None:
        return resolved_geojson

    if isinstance(spatial_value, str):
        return munge_spatial(spatial_value)

    return ""


def translate_spatial_to_geojson(spatial_input):
    if not spatial_input:
        return None

    translated_value = translate_spatial(spatial_input)
    if not translated_value:
        return None

    try:
        return json.loads(translated_value)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Unable to decode translated spatial value %s: %s",
            translated_value,
            exc,
        )
        return None
