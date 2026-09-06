"""Filesystem locations of the JSON Schemas this repo validates against.

DCAT-US 3.0 lives in the GSA/dcat-us git submodule at `_external/dcat-us`, not
in this repo. Defining the paths here keeps that layout in one place, so moving
or re-pinning the submodule touches a single module.

Do not edit anything under `_external/dcat-us` — open a PR against GSA/dcat-us
instead. See "DCAT-US 3.0 schemas" in docs/developer.md.

DCAT-US 1.1 has no GSA/dcat-us equivalent and stays vendored under `schemas/`.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DCATUS1_1_DIR = REPO_ROOT / "schemas" / "dcatus1.1"

DCATUS3_JSONSCHEMA_DIR = REPO_ROOT / "_external" / "dcat-us" / "jsonschema"
DCATUS3_DEFINITIONS_DIR = DCATUS3_JSONSCHEMA_DIR / "definitions"
DCATUS3_DATASET_SCHEMA = DCATUS3_DEFINITIONS_DIR / "Dataset.json"
DCATUS3_COMPLETE_EXAMPLE = (
    DCATUS3_JSONSCHEMA_DIR / "examples" / "Dataset" / "good" / "complete_example.json"
)
