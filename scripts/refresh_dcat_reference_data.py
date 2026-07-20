"""Regenerate the bundled reference-data snapshots used by DCAT-US 3 warning
detection (GSA/data.gov#6127).

The warning checkers in ``harvester/utils/dcat_warnings.py`` validate a handful
of fields against controlled vocabularies. Rather than call these registries at
harvest time (per-record network I/O in a failure-sensitive path, for
non-blocking warnings), we bundle static JSON snapshots and refresh them by
re-running this script.

Machine-readable sources fetched here:
  - ISO 639-1 language codes ....... id.loc.gov (JSON)
  - IANA character set names ....... iana.org (CSV)
  - IANA media types ............... iana.org (one CSV per top-level type)

The NARA restriction authority lists are HTML-only (no CSV/JSON/API) and hold
~5 stable terms each, so they are hand-curated in nara_restrictions.json rather
than scraped here. See that file's header for provenance.

Usage:
    poetry run python scripts/refresh_dcat_reference_data.py
"""

import csv
import io
import json
import re
from pathlib import Path

import requests

REFERENCE_DATA_DIR = (
    Path(__file__).resolve().parents[1] / "harvester" / "utils" / "reference_data"
)

ISO_639_1_URL = "https://id.loc.gov/vocabulary/iso639-1.json"
IANA_CHARSETS_URL = (
    "https://www.iana.org/assignments/character-sets/character-sets-1.csv"
)
# The IANA media types registry publishes one CSV per top-level type.
IANA_MEDIA_TYPE_REGISTRIES = (
    "application",
    "audio",
    "font",
    "haptics",
    "image",
    "message",
    "model",
    "multipart",
    "text",
    "video",
)
IANA_MEDIA_TYPE_URL = "https://www.iana.org/assignments/media-types/{registry}.csv"

_TIMEOUT = 30


def _get(url: str) -> requests.Response:
    response = requests.get(url, timeout=_TIMEOUT)
    response.raise_for_status()
    return response


def fetch_iso_639_1_codes() -> list:
    """Extract the two-letter language codes from the loc.gov vocabulary."""
    data = _get(ISO_639_1_URL).json()
    codes = set()
    for entry in data:
        match = re.search(r"/iso639-1/([a-z]{2})$", entry.get("@id", ""))
        if match:
            codes.add(match.group(1))
    return sorted(codes)


def fetch_iana_charset_names() -> list:
    """Collect IANA character set names plus their aliases (lowercased)."""
    text = _get(IANA_CHARSETS_URL).text
    reader = csv.DictReader(io.StringIO(text))
    names = set()
    for row in reader:
        for field in ("Preferred MIME Name", "Name"):
            value = (row.get(field) or "").strip()
            if value:
                names.add(value.lower())
        for alias in (row.get("Aliases") or "").split():
            alias = alias.strip()
            if alias:
                names.add(alias.lower())
    return sorted(names)


def fetch_iana_media_types() -> list:
    """Collect fully-qualified IANA media types across all registries."""
    media_types = set()
    for registry in IANA_MEDIA_TYPE_REGISTRIES:
        text = _get(IANA_MEDIA_TYPE_URL.format(registry=registry)).text
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            template = (row.get("Template") or "").strip()
            # Some rows carry a name but no registered template; skip those.
            if template and "/" in template:
                media_types.add(template.lower())
    return sorted(media_types)


def _write(filename: str, payload: dict) -> None:
    path = REFERENCE_DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {path} ({len(payload.get('values', []))} values)")


def main() -> None:
    REFERENCE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    _write(
        "iso639_1_language_codes.json",
        {"source": ISO_639_1_URL, "values": fetch_iso_639_1_codes()},
    )
    _write(
        "iana_character_sets.json",
        {"source": IANA_CHARSETS_URL, "values": fetch_iana_charset_names()},
    )
    _write(
        "iana_media_types.json",
        {
            "source": IANA_MEDIA_TYPE_URL.format(registry="{registry}"),
            "values": fetch_iana_media_types(),
        },
    )
    print("done; nara_restrictions.json is hand-curated and not refreshed here")


if __name__ == "__main__":
    main()
