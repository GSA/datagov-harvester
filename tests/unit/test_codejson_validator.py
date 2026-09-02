import pytest

from harvester.exceptions import ValidationException
from harvester.utils.codejson_validator import (
    validate_codejson_release,
    validate_codejson_structure,
)


class TestValidateCodejsonStructure:
    """Test validation of code.json structure"""

    def test_valid_codejson_structure_passes(self):
        """Test that valid code.json structure passes validation"""
        valid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "test-project",
                    "repositoryURL": "https://github.com/testag/test",
                    "description": "Test project",
                    "permissions": {"usageType": "openSource"},
                }
            ],
        }

        # Should not raise exception
        result = validate_codejson_structure(valid_catalog)
        assert result is True

    def test_missing_version_raises_exception(self):
        """Test that missing version field raises exception"""
        invalid_catalog = {
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [],
        }

        with pytest.raises(
            ValidationException, match="Missing required field: version"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_missing_agency_raises_exception(self):
        """Test that missing agency field raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "measurementType": {"method": "other"},
            "releases": [],
        }

        with pytest.raises(ValidationException, match="Missing required field: agency"):
            validate_codejson_structure(invalid_catalog)

    def test_missing_measurementType_raises_exception(self):
        """Test that missing measurementType field raises exception"""
        invalid_catalog = {"version": "2.0.0", "agency": "TESTAG", "releases": []}

        with pytest.raises(
            ValidationException, match="Missing required field: measurementType"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_missing_releases_raises_exception(self):
        """Test that missing releases field raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
        }

        with pytest.raises(
            ValidationException, match="Missing required field: releases"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_wrong_version_raises_exception(self):
        """Test that unsupported version raises exception"""
        invalid_catalog = {
            "version": "1.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [],
        }

        with pytest.raises(
            ValidationException, match="Unsupported code.json version: 1.0.0"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_releases_not_array_raises_exception(self):
        """Test that releases must be an array"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": "not-an-array",
        }

        with pytest.raises(ValidationException, match="releases must be an array"):
            validate_codejson_structure(invalid_catalog)

    def test_empty_releases_array_is_valid(self):
        """Test that empty releases array is valid (agency has no repos)"""
        valid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [],
        }

        result = validate_codejson_structure(valid_catalog)
        assert result is True


class TestValidateCodejsonRelease:
    """Test validation of individual code.json releases"""

    def test_valid_release_passes(self):
        """Test that valid release passes validation"""
        valid_release = {
            "name": "test-project",
            "repositoryURL": "https://github.com/testag/test",
            "description": "Test project",
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(valid_release)
        assert is_valid is True
        assert error is None

    def test_release_missing_name_returns_error(self):
        """Test that release missing name field returns error"""
        invalid_release = {
            "repositoryURL": "https://github.com/testag/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(invalid_release, release_idx=0)
        assert is_valid is False
        assert "Release 0" in error
        assert "missing required field: name" in error

    def test_release_missing_repositoryURL_returns_error(self):
        """Test that release missing repositoryURL returns error"""
        invalid_release = {
            "name": "test-project",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(invalid_release, release_idx=0)
        assert is_valid is False
        assert "Release 0" in error
        assert "missing required field: repositoryURL" in error

    def test_release_missing_description_returns_error(self):
        """Test that release missing description returns error"""
        invalid_release = {
            "name": "test-project",
            "repositoryURL": "https://github.com/testag/test",
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(invalid_release, release_idx=0)
        assert is_valid is False
        assert "Release 0" in error
        assert "missing required field: description" in error

    def test_release_missing_permissions_returns_error(self):
        """Test that release missing permissions returns error"""
        invalid_release = {
            "name": "test-project",
            "repositoryURL": "https://github.com/testag/test",
            "description": "Test",
        }

        is_valid, error = validate_codejson_release(invalid_release, release_idx=0)
        assert is_valid is False
        assert "Release 0" in error
        assert "missing required field: permissions" in error

    def test_release_with_name_in_error_message(self):
        """Test that error message includes release name when available"""
        invalid_release = {
            "name": "my-project",
            "repositoryURL": "https://github.com/testag/test",
            # missing description
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(invalid_release, release_idx=0)
        assert is_valid is False
        assert "Release 0 (my-project)" in error
        assert "missing required field: description" in error

    def test_release_without_index_in_error_message(self):
        """Test that error message works without release index"""
        invalid_release = {
            "name": "my-project",
            "repositoryURL": "https://github.com/testag/test",
            # missing description
            "permissions": {"usageType": "openSource"},
        }

        is_valid, error = validate_codejson_release(invalid_release)
        assert is_valid is False
        assert "Release (my-project)" in error
        assert "missing required field: description" in error
