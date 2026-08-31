import pytest

from harvester.exceptions import ValidationException
from harvester.utils.codejson_validator import validate_codejson_structure


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

    def test_release_missing_name_raises_exception(self):
        """Test that release missing name field raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "repositoryURL": "https://github.com/testag/test",
                    "description": "Test",
                    "permissions": {"usageType": "openSource"},
                }
            ],
        }

        with pytest.raises(ValidationException, match="Release 0.*missing: name"):
            validate_codejson_structure(invalid_catalog)

    def test_release_missing_repositoryURL_raises_exception(self):
        """Test that release missing repositoryURL raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "test-project",
                    "description": "Test",
                    "permissions": {"usageType": "openSource"},
                }
            ],
        }

        with pytest.raises(
            ValidationException, match="Release 0.*missing: repositoryURL"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_release_missing_description_raises_exception(self):
        """Test that release missing description raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "test-project",
                    "repositoryURL": "https://github.com/testag/test",
                    "permissions": {"usageType": "openSource"},
                }
            ],
        }

        with pytest.raises(
            ValidationException, match="Release 0.*missing: description"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_release_missing_permissions_raises_exception(self):
        """Test that release missing permissions raises exception"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "test-project",
                    "repositoryURL": "https://github.com/testag/test",
                    "description": "Test",
                }
            ],
        }

        with pytest.raises(
            ValidationException, match="Release 0.*missing: permissions"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_multiple_releases_validates_all(self):
        """Test that all releases are validated"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "valid-project",
                    "repositoryURL": "https://github.com/testag/valid",
                    "description": "Valid",
                    "permissions": {"usageType": "openSource"},
                },
                {
                    "name": "invalid-project",
                    "repositoryURL": "https://github.com/testag/invalid",
                    # missing description
                    "permissions": {"usageType": "openSource"},
                },
            ],
        }

        with pytest.raises(
            ValidationException, match="Release 1.*missing: description"
        ):
            validate_codejson_structure(invalid_catalog)

    def test_release_with_name_in_error_message(self):
        """Test that error message includes release name when available"""
        invalid_catalog = {
            "version": "2.0.0",
            "agency": "TESTAG",
            "measurementType": {"method": "other"},
            "releases": [
                {
                    "name": "my-project",
                    "repositoryURL": "https://github.com/testag/test",
                    # missing description
                    "permissions": {"usageType": "openSource"},
                }
            ],
        }

        with pytest.raises(
            ValidationException,
            match="Release 0 \\(my-project\\).*missing: description",
        ):
            validate_codejson_structure(invalid_catalog)
