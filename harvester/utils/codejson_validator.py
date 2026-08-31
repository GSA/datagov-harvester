from harvester.exceptions import ValidationException


def validate_codejson_structure(code_catalog: dict) -> bool:
    required_fields = ["version", "agency", "measurementType", "releases"]
    for field in required_fields:
        if field not in code_catalog:
            raise ValidationException(f"Missing required field: {field}")

    version = code_catalog.get("version")
    if version != "2.0.0":
        raise ValidationException(
            f"Unsupported code.json version: {version}. Only version 2.0.0 is supported."
        )

    releases = code_catalog.get("releases")
    if not isinstance(releases, list):
        raise ValidationException("releases must be an array")

    return True


def validate_codejson_release(
    release: dict, release_idx: int = None
) -> tuple[bool, str]:
    release_name = release.get("name", "")
    if release_idx is not None:
        release_label = (
            f"Release {release_idx} ({release_name})"
            if release_name
            else f"Release {release_idx}"
        )
    else:
        release_label = f"Release ({release_name})" if release_name else "Release"

    required_release_fields = ["name", "repositoryURL", "description", "permissions"]
    for field in required_release_fields:
        if field not in release:
            return False, f"{release_label} is missing required field: {field}"

    return True, None
