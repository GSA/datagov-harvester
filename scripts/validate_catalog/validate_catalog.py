"""
Validate a DCAT-US catalog URL against a schema, bypassing the 10MB upload
cap on the hosted /validate page (app/constants.py MAX_UPLOAD_MB).

Usage (run inside the app container):
    docker compose exec app python3 scripts/validate_catalog/validate_catalog.py \
        <url> [--schema SCHEMA] [--output FILE]

schema is one of:
    "dcatus3.0 catalog" (default)
    "dcatus1.1: federal dataset"
    "dcatus1.1: non-federal dataset"
"""

import argparse
import os
import sys

import requests

from app.util import validate_records

DEFAULT_SCHEMA = "dcatus3.0 catalog"
DEFAULT_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output", "errors.txt")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="URL of the DCAT-US catalog to validate")
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"Schema to validate against (default: {DEFAULT_SCHEMA!r})",
    )
    parser.add_argument(
        "-o",
        "--output",
        nargs="?",
        const=DEFAULT_OUTPUT_PATH,
        default=None,
        help=(
            "Write the full error list to this file instead of stdout. "
            f"Defaults to {DEFAULT_OUTPUT_PATH!r} if given with no path. "
            "Overwrites the file if it already exists."
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()

    resp = requests.get(args.url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    print(f"dataset count: {len(data.get('dataset', []))}")

    errors = validate_records(data, args.schema)
    print(f"total validation errors: {len(errors)}")

    lines = [f"{identifier} :: {err}" for identifier, err in errors]
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"wrote {len(errors)} errors to {args.output}")
    else:
        for line in lines:
            print(line)

    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
