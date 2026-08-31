"""Validator for code.json structure according to schema version 2.0.0.

This module validates the structure of code.json files before transformation.
It checks for required fields and proper data types but does not perform
full JSON Schema validation.
"""

from harvester.exceptions import ValidationException


def validate_codejson_structure(code_catalog: dict) -> bool:
    """Validate code.json structure has required fields.

    Validates that a code.json catalog has the minimum required structure:
    - version: must be "2.0.0"
    - agency: agency name
    - measurementType: measurement configuration
    - releases: array of release objects

    Each release must have:
    - name: project name
    - repositoryURL: repository URL
    - description: project description
    - permissions: permissions object

    Args:
        code_catalog: Parsed code.json catalog as dictionary

    Returns:
        True if validation passes

    Raises:
        ValidationException: If any required field is missing or invalid
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

    # Validate each release
    required_release_fields = ["name", "repositoryURL", "description", "permissions"]
    for idx, release in enumerate(releases):
        # Get release name for better error messages
        release_name = release.get("name", "")
        release_label = (
            f"Release {idx} ({release_name})" if release_name else f"Release {idx}"
        )

        # Check required fields
        for field in required_release_fields:
            if field not in release:
                raise ValidationException(f"{release_label} is missing: {field}")

    return True
