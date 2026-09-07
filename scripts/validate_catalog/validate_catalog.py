"""
Validate a DCAT-US catalog URL against a schema, bypassing the 10MB upload
cap on the hosted /validate page (app/constants.py MAX_UPLOAD_MB).

Usage (run inside the app container):
    docker compose exec app python3 scripts/validate_catalog/validate_catalog.py \
        <url> [schema]

schema is one of:
    "dcatus3.0 catalog" (default)
    "dcatus1.1: federal dataset"
    "dcatus1.1: non-federal dataset"
"""

import sys

import requests

from app.util import validate_records

DEFAULT_SCHEMA = "dcatus3.0 catalog"


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <url> [schema]")
        sys.exit(1)

    url = sys.argv[1]
    schema = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SCHEMA

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    print(f"dataset count: {len(data.get('dataset', []))}")

    errors = validate_records(data, schema)
    print(f"total validation errors: {len(errors)}")
    for identifier, err in errors:
        print(identifier, "::", err)

    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
