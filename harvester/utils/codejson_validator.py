"""Validator for code.json structure according to schema version 2.0.0.

This module validates the structure of code.json files before transformation.
It checks for required fields and proper data types but does not perform
full JSON Schema validation.
"""

from harvester.exceptions import ValidationException


def validate_codejson_structure(code_catalog: dict) -> bool:
    """Validate code.json catalog-level structure has required fields.

    Validates that a code.json catalog has the minimum required structure:
    - version: must be "2.0.0"
    - agency: agency name
    - measurementType: measurement configuration
    - releases: array of release objects

    Individual release validation happens during transformation/DCAT validation
    to allow harvests to continue even if some releases are invalid.

    Args:
        code_catalog: Parsed code.json catalog as dictionary

    Returns:
        True if validation passes

    Raises:
        ValidationException: If any required catalog-level field is missing or invalid
    """
    # Check required top-level fields
    required_fields = ["version", "agency", "measurementType", "releases"]
    for field in required_fields:
        if field not in code_catalog:
            raise ValidationException(f"Missing required field: {field}")

    # Check version is 2.0.0
    version = code_catalog.get("version")
    if version != "2.0.0":
        raise ValidationException(
            f"Unsupported code.json version: {version}. Only version 2.0.0 is supported."
        )

    # Check releases is an array
    releases = code_catalog.get("releases")
    if not isinstance(releases, list):
        raise ValidationException("releases must be an array")

    return True


def validate_codejson_release(
    release: dict, release_idx: int = None
) -> tuple[bool, str]:
    """Validate a single code.json release has required fields.

    Each release must have:
    - name: project name
    - repositoryURL: repository URL
    - description: project description
    - permissions: permissions object

    Args:
        release: Single release object from code.json
        release_idx: Optional index of release in array for error messages

    Returns:
        Tuple of (is_valid, error_message)
        - (True, None) if valid
        - (False, error_message) if invalid
    """
    # Get release name for better error messages
    release_name = release.get("name", "")
    if release_idx is not None:
        release_label = (
            f"Release {release_idx} ({release_name})"
            if release_name
            else f"Release {release_idx}"
        )
    else:
        release_label = f"Release ({release_name})" if release_name else "Release"

    # Check required fields
    required_release_fields = ["name", "repositoryURL", "description", "permissions"]
    for field in required_release_fields:
        if field not in release:
            return False, f"{release_label} is missing required field: {field}"

    return True, None
